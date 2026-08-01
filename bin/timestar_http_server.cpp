#include "cluster/integration/cluster_data_plane.hpp"
#include "cluster/integration/group0_identity_bridge.hpp"
#include "cluster/integration/node_identity.hpp"
#include "config/timestar_config.hpp"
#include "core/engine.hpp"
#include "core/placement_table.hpp"
#include "core/vshard.hpp"
#include "http/content_negotiation.hpp"
#include "http/http_auth.hpp"
#include "http/http_delete_handler.hpp"
#include "http/http_derived_query_handler.hpp"
#include "http/http_metadata_handler.hpp"
#include "http/http_query_handler.hpp"
#include "http/http_retention_handler.hpp"
#include "http/http_routes.hpp"
#include "http/http_stream_handler.hpp"
#include "http/http_write_handler.hpp"
#include "http/proto_converters.hpp"
#include "storage/shard_store_startup.hpp"
#include "storage/storage_layout.hpp"
#include "timestar/version.hpp"
#include "utils/data_dir_lock.hpp"
#include "utils/json_escape.hpp"
#include "utils/logger.hpp"
#include "utils/stop_signal.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>
#include <sstream>
#include <sys/resource.h>
#include <vector>

using namespace seastar;
using namespace httpd;

// HttpServer class removed - using direct implementation in main()

// Global sharded engine - declared here so set_routes() can reference it.
// (Defined before set_routes, initialized in main.)
seastar::sharded<Engine> g_engine;

// The M2 VShard-partitioned data plane, live only when [cluster] enabled +
// partitioned. Lives on shard 0 (started in main's shard-0 lambda); the HTTP
// handlers reach it via invoke_on(0). Inert (never started) otherwise, so single-
// node and M1 full-replication clusters are byte-identical.
timestar::cluster::ClusterDataPlane g_clusterDataPlane;
bool g_clusterPartitioned = false;

// Consecutive compaction failures on any one tier before /health reports
// "degraded". One failure can be transient; a run of them means the tier is
// wedged and its file count is growing without bound.
static constexpr uint64_t HEALTH_COMPACTION_FAILURE_THRESHOLD = 5;

// The default journal layout holds one active descriptor per local VShard.
// At RF=N that is all 4,096 groups, in addition to Engine, RPC, HTTP and
// transient compaction descriptors.  Keep a full second VShard-count as
// headroom so the process does not boot at the edge and fail on its first
// rollover or connection burst.
static constexpr rlim_t REPLICATED_CLUSTER_MIN_OPEN_FILES = 2 * timestar::VIRTUAL_SHARD_COUNT;

static bool ensureReplicatedClusterOpenFileLimit() {
    struct rlimit limit {};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        timestar::http_log.error("Cannot inspect RLIMIT_NOFILE for replicated startup: {}", std::strerror(errno));
        return false;
    }
    if (limit.rlim_cur >= REPLICATED_CLUSTER_MIN_OPEN_FILES)
        return true;
    if (limit.rlim_max < REPLICATED_CLUSTER_MIN_OPEN_FILES) {
        timestar::http_log.error(
            "Replicated startup needs at least {} open files (4,096 active VShard journals plus runtime headroom), "
            "but RLIMIT_NOFILE is soft={} hard={}. Raise the service's LimitNOFILE/ulimit before starting TimeStar.",
            REPLICATED_CLUSTER_MIN_OPEN_FILES, limit.rlim_cur, limit.rlim_max);
        return false;
    }

    const rlim_t previous = limit.rlim_cur;
    limit.rlim_cur = REPLICATED_CLUSTER_MIN_OPEN_FILES;
    if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) {
        timestar::http_log.error("Cannot raise RLIMIT_NOFILE from {} to {} for replicated startup: {}", previous,
                                REPLICATED_CLUSTER_MIN_OPEN_FILES, std::strerror(errno));
        return false;
    }
    timestar::http_log.info("Raised RLIMIT_NOFILE soft limit from {} to {} for replicated VShard journals", previous,
                            REPLICATED_CLUSTER_MIN_OPEN_FILES);
    return true;
}

// Per-shard stream handler pointer, used to call stop() during shutdown.
static thread_local timestar::http::HttpStreamHandler* g_streamHandler = nullptr;

// Readiness flag — set true after all engines are initialized.
// Used by /health for Kubernetes readiness probes.
static std::atomic<bool> g_ready{false};

// Auth token — set once before server start, read by all shards via set_routes().
// Empty string means auth is disabled (all requests pass through).
// Atomic pointer publication: the storage string is fully constructed before
// the pointer is stored with release ordering, and authToken() loads with
// acquire ordering — so other shards observing a non-null pointer also
// observe the completed storage initialization.
static std::string g_authTokenStorage;
static std::atomic<const std::string*> g_authToken{nullptr};

static const std::string& authToken() {
    static const std::string empty;
    auto* p = g_authToken.load(std::memory_order_acquire);
    return p ? *p : empty;
}

static std::string readClusterCredential(const std::string& path, const char* label) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
        throw std::runtime_error(std::string("cannot open cluster ") + label + " file: " + path);
    std::ostringstream out;
    out << in.rdbuf();
    if (!in.good() && !in.eof())
        throw std::runtime_error(std::string("cannot read cluster ") + label + " file: " + path);
    if (out.str().empty())
        throw std::runtime_error(std::string("cluster ") + label + " file is empty: " + path);
    return out.str();
}

static std::string staticTopologyDescription(const timestar::ClusterConfig& cfg) {
    std::string value = "rf=" + std::to_string(cfg.replication_factor) + ";peers=";
    for (const auto& peer : cfg.peers)
        value += std::to_string(peer.size()) + ":" + peer + ";";
    return value;
}

void set_routes(routes& r) {
    // Per-shard handler instances. Each handler's registerRoutes() captures
    // `this` in route lambdas, so the handler must outlive set_routes().
    // We store them in a thread_local vector so they are freed on process exit
    // (avoids ASAN leak reports while keeping shard-local memory ownership).
    static thread_local std::vector<std::unique_ptr<void, void (*)(void*)>> handlers;

    auto emplaceHandler = [&]<typename T>(T* ptr) {
        handlers.emplace_back(ptr, [](void* p) { delete static_cast<T*>(p); });
        return ptr;
    };

    // Simple test endpoints — passed directly to r.add() which takes exclusive
    // ownership.  Must NOT also go through emplaceHandler() (double-free).
    r.add(operation_type::GET, url("/test"),
          new function_handler([](const_req /*req*/) { return "Hello from TimeStar HTTP Server!"; }));

    // Operator visibility for a clustered deployment (integration plan M5/M6 operator
    // surface). Reports this node's identity/peers and, when replicated, how many
    // VShards it hosts, leads, and -- critically -- how many have NO elected leader.
    // A non-zero leaderless count is exactly the condition that makes reads and writes
    // fail, and before this it could only be inferred from query errors.
    r.add(operation_type::GET, url("/cluster/status"),
          new function_handler(
              [](std::unique_ptr<seastar::http::request> /*req*/,
                 std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  rep->set_status(seastar::http::reply::status_type::ok);
                  rep->set_content_type("json");
                  if (!g_clusterPartitioned) {
                      rep->_content = R"({"clustered":false})";
                      co_return std::move(rep);
                  }
                  // The data plane lives on shard 0; hop there for its view.
                  auto st = co_await seastar::smp::submit_to(0u, [] { return g_clusterDataPlane.status(); });
                  std::string peers;
                  for (const auto& [id, addr] : st.peers) {
                      if (!peers.empty())
                          peers += ",";
                      peers += "{\"node\":" + std::to_string(id) + ",\"address\":\"" + addr + "\"}";
                  }
                  std::string body = "{\"clustered\":true,\"node_id\":" + std::to_string(st.self) +
                                     ",\"replication_factor\":" + std::to_string(st.replicationFactor) +
                                     ",\"replicated\":" + (st.replicated ? "true" : "false") + ",\"peers\":[" + peers +
                                     "],\"unresolved_peers\":" + std::to_string(st.unresolvedPeerCount);
                  if (st.replicated) {
                      body += ",\"vshards_hosted\":" + std::to_string(st.vshardsHostedHere) +
                              ",\"vshards_led\":" + std::to_string(st.vshardsLedHere) +
                              ",\"vshards_leaderless\":" + std::to_string(st.vshardsLeaderless) +
                              ",\"healthy\":" + (st.readyForTraffic() ? "true" : "false");
                      // Replication progress of each peer for the groups we lead. A peer
                      // far below vshards_led is not acking our appends.
                      std::string caught;
                      for (const auto& [peer, n] : st.peerCaughtUp) {
                          if (!caught.empty())
                              caught += ",";
                          caught += "\"" + std::to_string(peer) + "\":" + std::to_string(n);
                      }
                      body += ",\"peer_caught_up\":{" + caught + "}";
                      // THE ACK-CONTRACT GAP (debt D-36). An acknowledged write is
                      // durable at commit and readable only at apply, so
                      // apply_lag_entries > 0 names promises this node cannot currently
                      // keep -- and distinguishes a restart still replaying from data
                      // that is actually gone, which nothing else here could.
                      body += ",\"apply_lag_entries\":" + std::to_string(st.applyLagEntries) +
                              ",\"apply_groups_behind\":" + std::to_string(st.applyGroupsBehind) +
                              ",\"apply_failures\":" + std::to_string(st.applyFailures) +
                              ",\"tick_errors\":" + std::to_string(st.tickErrors);
                      // Raft-log snapshot/compaction (debt D-5/D-6). `snapshots_taken`
                      // rising is how an operator sees compaction running at all; the chunk
                      // and install counters are the ONLY way to tell a snapshot-based
                      // catch-up from an append-based one.
                      body += ",\"snapshot_trigger\":" + std::string(st.snapshotTriggerEnabled ? "true" : "false") +
                              ",\"snapshots_taken\":" + std::to_string(st.snapshotsTaken) +
                              ",\"snapshots_refused_too_large\":" + std::to_string(st.snapshotsRefusedTooLarge) +
                              ",\"snapshots_skipped_unflushed\":" + std::to_string(st.snapshotsSkippedUnflushed) +
                              ",\"snapshots_skipped_pending_conversion\":" +
                              std::to_string(st.snapshotsSkippedPendingConversion) +
                              ",\"snapshot_sweeps\":" + std::to_string(st.snapshotSweeps) +
                              ",\"snapshot_max_entries_since\":" + std::to_string(st.snapshotMaxEntriesSince) +
                              ",\"snapshot_chunks_sent\":" + std::to_string(st.snapshotChunksSent) +
                              ",\"snapshots_installed\":" + std::to_string(st.snapshotsInstalled) +
                              ",\"snapshots_undeliverable\":" + std::to_string(st.snapshotsUndeliverable) +
                              ",\"snapshot_transfers_restarted\":" + std::to_string(st.snapshotTransfersRestarted) +
                              ",\"snapshot_transfers_abandoned\":" + std::to_string(st.snapshotTransfersAbandoned) +
                              ",\"snapshot_production_limit_per_shard\":" +
                              std::to_string(st.snapshotProductionLimitPerShard);
                      // Raft journal fsyncs (debt D-10). journal_sync_requests /
                      // journal_fsyncs is the coalescing factor: 1.0 per-VShard, > 1
                      // with the shared per-shard journal. The DISK win is invisible on
                      // tmpfs, so this ratio -- not a throughput number -- is the honest
                      // evidence that the coalescer is doing anything.
                      body +=
                          ",\"journal_shared\":" + std::string(st.journalShared ? "true" : "false") +
                          ",\"journal_fsyncs\":" + std::to_string(st.journalFsyncs) +
                          ",\"journal_sync_requests\":" + std::to_string(st.journalSyncRequests) +
                          ",\"journal_gc_passes\":" + std::to_string(st.journalGcPasses) +
                          ",\"journal_segments_deleted\":" + std::to_string(st.journalSegmentsDeleted) +
                          ",\"journal_segments_pinned_last_pass\":" + std::to_string(st.journalSegmentsPinnedLastPass) +
                          ",\"journal_records_copied_forward\":" + std::to_string(st.journalRecordsCopiedForward);
                  }
                  body += "}";
                  rep->_content = std::move(body);
                  co_return std::move(rep);
              },
              "json"));

    // Operator action: hand leadership of VShards this node leads beyond its fair
    // share to lighter peers (M5 leadership balancing == v1 read balancing). Bounded
    // per call via ?max=N (default 256); call repeatedly to converge.
    timestar::http::addJsonRoute(
        r, operation_type::POST, "/cluster/rebalance-leadership", authToken(),
              [](std::unique_ptr<seastar::http::request> req,
                 std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  size_t maxTransfers = 256;
                  if (req->query_parameters.contains("max")) {
                      try {
                          maxTransfers = std::stoul(req->query_parameters.at("max"));
                      } catch (...) {
                          rep->set_status(seastar::http::reply::status_type::bad_request);
                          rep->_content = R"({"status":"error","message":"max must be a number"})";
                          co_return std::move(rep);
                      }
                  }
                  if (!g_clusterPartitioned) {
                      rep->set_status(seastar::http::reply::status_type::bad_request);
                      rep->_content = R"({"status":"error","message":"node is not clustered"})";
                      co_return std::move(rep);
                  }
                  const size_t before =
                      (co_await seastar::smp::submit_to(0u, [] { return g_clusterDataPlane.status(); })).vshardsLedHere;
                  const size_t moved = co_await seastar::smp::submit_to(
                      0u, [maxTransfers] { return g_clusterDataPlane.rebalanceLeadership(maxTransfers); });
                  const size_t after =
                      (co_await seastar::smp::submit_to(0u, [] { return g_clusterDataPlane.status(); })).vshardsLedHere;
                  rep->set_status(seastar::http::reply::status_type::ok);
                  // Raft leadership transfer is a REQUEST (TimeoutNow): the target only
                  // becomes leader if it can campaign successfully. Report the actual
                  // before/after so an operator can see when transfers are initiated but
                  // not taking effect (e.g. a target too loaded to run an election)
                  // instead of reading "success" and assuming the cluster rebalanced.
                  rep->_content = "{\"status\":\"success\",\"transfers_initiated\":" + std::to_string(moved) +
                                  ",\"vshards_led_before\":" + std::to_string(before) +
                                  ",\"vshards_led_after\":" + std::to_string(after) + "}";
                  co_return std::move(rep);
              });

    r.add(operation_type::GET, url("/health"),
          new function_handler(
              [](std::unique_ptr<seastar::http::request> req,
                 std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  auto resFmt = timestar::http::responseFormat(*req);
                  if (g_ready.load(std::memory_order_acquire)) {
                      bool clusterReady = true;
                      std::string clusterReason;
                      if (g_clusterPartitioned) {
                          auto st = co_await seastar::smp::submit_to(0u, [] { return g_clusterDataPlane.status(); });
                          if (!st.readyForTraffic()) {
                              clusterReady = false;
                              clusterReason = st.readinessReason();
                          }
                      }

                      // A tier that cannot merge is invisible until the server starts
                      // refusing writes: the production incident this guards against ran
                      // 15 minutes with /health saying "healthy" while compaction failed
                      // ~6x/second and the tier grew without bound. Report "degraded"
                      // once a tier has failed repeatedly, while still serving traffic
                      // (200) -- the data is readable, the store just isn't compacting.
                      const uint64_t worstFailures = g_engine.local().getMaxConsecutiveCompactionFailures();
                      const bool compactionStuck = worstFailures >= HEALTH_COMPACTION_FAILURE_THRESHOLD;

                      if (!clusterReady) {
                          rep->set_status(seastar::http::reply::status_type::service_unavailable);
                          if (timestar::http::isProtobuf(resFmt))
                              rep->_content = timestar::proto::formatHealthResponse("not_ready");
                          else
                              rep->_content = "{\"status\":\"not_ready\",\"reason\":\"" +
                                              timestar::jsonEscape(clusterReason) + "\"}";
                      } else if (timestar::http::isProtobuf(resFmt)) {
                          rep->set_status(seastar::http::reply::status_type::ok);
                          rep->_content =
                              timestar::proto::formatHealthResponse(compactionStuck ? "degraded" : "healthy");
                      } else if (compactionStuck) {
                          rep->set_status(seastar::http::reply::status_type::ok);
                          rep->_content =
                              "{\"status\":\"degraded\",\"reason\":\"compaction_failing\",\"consecutive_failures\":" +
                              std::to_string(worstFailures) + "}";
                      } else {
                          rep->set_status(seastar::http::reply::status_type::ok);
                          rep->_content = "{\"status\":\"healthy\"}";
                      }
                  } else {
                      rep->set_status(seastar::http::reply::status_type::service_unavailable);
                      if (timestar::http::isProtobuf(resFmt)) {
                          rep->_content = timestar::proto::formatHealthResponse("starting");
                      } else {
                          rep->_content = "{\"status\":\"starting\"}";
                      }
                  }
                  timestar::http::setContentType(*rep, resFmt);
                  co_return std::move(rep);
              },
              "json"));

    r.add(operation_type::GET, url("/version"),
          new function_handler(
              [](std::unique_ptr<seastar::http::request> req,
                 std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  auto resFmt = timestar::http::responseFormat(*req);
                  rep->set_status(seastar::http::reply::status_type::ok);
                  if (timestar::http::isProtobuf(resFmt)) {
                      auto versionStr = fmt::format("{} ({}) built {} with {}", timestar::VERSION, timestar::GIT_COMMIT,
                                                    timestar::BUILD_TIME, timestar::COMPILER);
                      rep->_content = timestar::proto::formatStatusResponse("ok", versionStr);
                  } else {
                      rep->_content = sstring(fmt::format(
                          R"({{"version":"{}","git_commit":"{}","build_time":"{}","compiler":"{}"}})",
                          timestar::jsonEscape(timestar::VERSION), timestar::jsonEscape(timestar::GIT_COMMIT),
                          timestar::jsonEscape(timestar::BUILD_TIME), timestar::jsonEscape(timestar::COMPILER)));
                  }
                  timestar::http::setContentType(*rep, resFmt);
                  rep->done();
                  return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
              },
              "json"));

    // Protected endpoints — when g_authToken is non-empty, each handler wraps
    // its routes with Bearer token authentication via wrapWithAuth().
    auto* writeHandler = emplaceHandler(new timestar::http::HttpWriteHandler(&g_engine));
    writeHandler->registerRoutes(r, authToken());

    auto* queryHandler = emplaceHandler(new timestar::http::HttpQueryHandler(&g_engine));
    queryHandler->registerRoutes(r, authToken());

    auto* deleteHandler = emplaceHandler(new timestar::http::HttpDeleteHandler(&g_engine, g_clusterPartitioned));
    deleteHandler->registerRoutes(r, authToken());

    auto* metadataHandler = emplaceHandler(new timestar::http::HttpMetadataHandler(&g_engine));
    metadataHandler->registerRoutes(r, authToken());

    auto retentionHandlerPtr =
        std::make_shared<timestar::http::HttpRetentionHandler>(&g_engine, g_clusterPartitioned);
    retentionHandlerPtr->registerRoutes(r, authToken());
    handlers.emplace_back(new std::shared_ptr<timestar::http::HttpRetentionHandler>(retentionHandlerPtr), [](void* p) {
        delete static_cast<std::shared_ptr<timestar::http::HttpRetentionHandler>*>(p);
    });

    auto* streamHandler = emplaceHandler(new timestar::http::HttpStreamHandler(&g_engine, g_clusterPartitioned));
    streamHandler->registerRoutes(r, authToken());
    g_streamHandler = streamHandler;

    auto* derivedQueryHandler = emplaceHandler(new timestar::http::HttpDerivedQueryHandler(&g_engine));
    derivedQueryHandler->registerRoutes(r, authToken());

    r.add(operation_type::GET, url("/"), new function_handler([](const_req /*req*/) {
              return "{\"message\":\"TimeStar HTTP "
                     "Server\",\"endpoints\":[\"/test\",\"/health\",\"/write\",\"/query\",\"/delete\",\"/"
                     "measurements\",\"/"
                     "tags\",\"/fields\",\"/retention\",\"/subscribe\",\"/subscriptions\"]}";
          }));
}

int main(int argc, char** argv) {
    // Pre-scan argv for --version, --dump-config, --config before Seastar touches args.
    // This avoids Seastar complaining about unknown options.
    std::string configPath;
    bool dumpConfig = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--version") == 0) {
            std::cout << "TimeStar " << timestar::VERSION << " (" << timestar::GIT_COMMIT << ")"
                      << "\nBuilt: " << timestar::BUILD_TIME << "\nCompiler: " << timestar::COMPILER << std::endl;
            return 0;
        } else if (std::strcmp(argv[i], "--dump-config") == 0) {
            dumpConfig = true;
        } else if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
            configPath = argv[i + 1];
        }
    }

    if (dumpConfig) {
        std::cout << timestar::dumpDefaultConfig();
        return 0;
    }

    // Support TIMESTAR_CONFIG_FILE env var as alternative to --config
    if (configPath.empty()) {
        if (auto envCfg = std::getenv("TIMESTAR_CONFIG_FILE")) {
            configPath = envCfg;
        }
    }

    // Load config file if specified, otherwise use defaults.
    timestar::TimestarConfig timestarConfig{};
    if (!configPath.empty()) {
        try {
            timestarConfig = timestar::loadConfigFile(configPath);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            return 1;
        }
    }

    // Apply TIMESTAR_* environment variable overrides (env vars > config file > defaults)
    timestar::applyEnvironmentOverrides(timestarConfig);

    // Re-validate after env overrides (env vars may set invalid values)
    auto envErrors = timestarConfig.validate();
    if (!envErrors.empty()) {
        std::cerr << "Config validation errors after environment overrides:" << std::endl;
        for (const auto& e : envErrors) {
            std::cerr << "  - " << e << std::endl;
        }
        return 1;
    }

    timestar::setGlobalConfig(timestarConfig);

    seastar::app_template app;

    namespace bpo = boost::program_options;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(timestarConfig.server.port), "HTTP server port")(
        "log-level", bpo::value<seastar::log_level>()->default_value(seastar::log_level::info),
        "Log level (error, warn, info, debug, trace)")("config", bpo::value<std::string>(), "Path to TOML config file")(
        "dump-config", "Print default config to stdout and exit");

    // Inject Seastar settings from TOML [seastar] section.
    // CLI args are already stored first, so bpo::store won't overwrite them.
    app.set_configuration_reader([&timestarConfig, &app](bpo::variables_map& vm) {
        const auto& ss = timestarConfig.seastar;
        if (ss.settings.empty())
            return;

        // Map TOML underscore keys to Seastar's hyphenated CLI option names.
        static const std::map<std::string, std::string> keyMap = {
            {"smp", "smp"},
            {"memory", "memory"},
            {"reserve_memory", "reserve-memory"},
            {"poll_mode", "poll-mode"},
            {"task_quota_ms", "task-quota-ms"},
            {"overprovisioned", "overprovisioned"},
            {"thread_affinity", "thread-affinity"},
            {"reactor_backend", "reactor-backend"},
            {"blocked_reactor_notify_ms", "blocked-reactor-notify-ms"},
            {"max_networking_io_control_blocks", "max-networking-io-control-blocks"},
            {"unsafe_bypass_fsync", "unsafe-bypass-fsync"},
            {"kernel_page_cache", "kernel-page-cache"},
            {"max_task_backlog", "max-task-backlog"},
            {"io_properties_file", "io-properties-file"},
        };

        std::ostringstream ini;
        for (const auto& [tomlKey, value] : ss.settings) {
            auto it = keyMap.find(tomlKey);
            if (it != keyMap.end()) {
                ini << it->second << "=" << value << "\n";
            }
        }

        std::string iniStr = ini.str();
        if (!iniStr.empty()) {
            std::istringstream iss(iniStr);
            bpo::store(bpo::parse_config_file(iss, app.get_conf_file_options_description()), vm);
        }
    });

    return app.run(argc, argv, [&] {
        return seastar::async([&] {
            auto& config = app.configuration();
            uint16_t port = config["port"].as<uint16_t>();
            auto log_level = config["log-level"].as<seastar::log_level>();

            // Initialize logging
            timestar::init_logging(log_level);
            timestar::http_log.info("Starting TimeStar {} ({}) built {} with {}", timestar::VERSION,
                                    timestar::GIT_COMMIT, timestar::BUILD_TIME, timestar::COMPILER);

            // Resolve the data root from [server] data_dir (default "." = CWD).
            // All shard directories (shard_N), placement.json and
            // shard_count.meta live under this directory.
            const std::string dataRoot = timestar::dataRootPath();
            std::filesystem::create_directories(dataRoot);
            timestar::http_log.info("Data directory: {}", std::filesystem::absolute(dataRoot).string());

            {
                const auto& cc = timestar::config().cluster;
                if (cc.enabled && cc.partitioned)
                    timestar::http_log.info("Cluster mode ENABLED: node {} of {} peers (VShard-partitioned RF={})",
                                            cc.node_id, cc.peers.size(), cc.replication_factor);
                else if (cc.enabled)
                    timestar::http_log.info("Cluster mode ENABLED: node {} of {} peers (full replication)", cc.node_id,
                                            cc.peers.size());
                else
                    timestar::http_log.info("Cluster mode disabled (single node)");

                // Do this before the data directory, Engine or Raft plane is
                // opened.  A low limit previously failed around the thousandth
                // VShard journal, then partial-startup cleanup trapped and made
                // the process look like an illegal-instruction/CPU failure.
                if (cc.enabled && cc.partitioned && cc.replication_factor > 1 &&
                    !ensureReplicatedClusterOpenFileLimit())
                    return 1;
            }

            // Take an exclusive lock on the data directory before ANYTHING
            // touches it, so a second instance cannot race the first one's
            // storage. Held for the lifetime of the process; the kernel
            // releases it on exit by any means, kill -9 included.
            static timestar::DataDirLock dataDirLock;
            try {
                dataDirLock.acquire(dataRoot);
            } catch (const std::exception& e) {
                timestar::http_log.error("{}", e.what());
                return 1;
            }

            // Until group 0 owns cluster bootstrap, bind the static placement inputs
            // to this data directory and stamp the same durable cluster identity into
            // every Raft journal. This turns peer-list edits and cross-wired data dirs
            // into startup failures instead of silent VShard remaps/replay.
            std::optional<timestar::cluster::JournalIdentity> clusterJournalIdentity;
            std::optional<timestar::cluster::DataPlaneTls> clusterTls;
            {
                const auto& cc = timestar::config().cluster;
                if (cc.enabled && cc.partitioned && cc.replication_factor > 1) {
                    auto identity = timestar::cluster::NodeIdentity::loadOrCreate(dataRoot);
                    timestar::cluster::bindClusterUuid(identity, dataRoot, cc.cluster_uuid);
                    timestar::cluster::bindStaticTopology(identity, dataRoot, staticTopologyDescription(cc));
                    clusterJournalIdentity = timestar::cluster::JournalIdentity::fromHex(
                        identity.cluster_uuid, timestar::cluster::NodeIdentity::generateUuid());

                    if (!cc.tls_cert_file.empty()) {
                        clusterTls = timestar::cluster::DataPlaneTls{
                            readClusterCredential(cc.tls_cert_file, "certificate"),
                            readClusterCredential(cc.tls_key_file, "private key"),
                            readClusterCredential(cc.tls_ca_file, "CA"), cc.tls_peer_name};
                    } else {
                        timestar::http_log.warn(
                            "replicated cluster transport is PLAINTEXT because the explicit development-only insecure "
                            "override is enabled");
                    }
                }
            }

            // STEP 0: Fail-closed storage safety gate.
            //
            // Normal startup never runs the legacy core-count rebalancer, which
            // rewrites shard layout and can leave an existing series
            // undiscoverable. Instead it inspects the store read-only and
            // refuses to proceed on an unsafe or ambiguous change (a --smp/core
            // count that does not match the stored shard count, an interrupted
            // rebalance, or unreadable metadata). The inspection creates and
            // moves nothing; it runs before any storage service opens.
            const auto storageLayout = timestar::StorageLayout(dataRoot).anchored();
            timestar::ShardStoreStartupSession shardStoreStartup(storageLayout, seastar::smp::count);
            if (!shardStoreStartup.canStart()) {
                timestar::http_log.error("{}", shardStoreStartup.failureMessage());
                return 1;
            }

            // Initialize virtual shard placement table (Phase 5).
            auto pt = timestar::PlacementTable::buildLocal(seastar::smp::count);
            timestar::setGlobalPlacement(std::move(pt));
            timestar::savePlacement(storageLayout.placementFile().string());

            // STEP 1: Initialize the Engine on all shards
            timestar::http_log.info("Initializing Engine on all shards...");
            g_engine.start(storageLayout).get();

            // Lifecycle guard: Seastar's sharded<> asserts in its destructor if
            // stop() was not called after a successful start(). Keep this armed
            // through data-plane and HTTP startup, the serving loop, and normal
            // shutdown: Engine init was not the last operation that can throw.
            auto engineGuard = seastar::defer([&] {
                if (g_clusterPartitioned) {
                    try {
                        g_clusterDataPlane.stop().get();
                    } catch (const std::exception& e) {
                        timestar::http_log.error("Data-plane cleanup after server failure also failed: {}", e.what());
                    }
                    g_clusterPartitioned = false;
                }
                g_engine.invoke_on_all([](Engine& engine) { return engine.stop(); }).get();
                g_engine.stop().get();
            });

            try {
                g_engine.invoke_on_all([](Engine& engine) { return engine.init(); }).get();

                // Create I/O scheduling groups (global operation, called once from shard 0).
                // Query I/O gets highest priority; compaction gets lowest to protect
                // query tail latency during compaction storms.
                const auto& ioCfg = timestar::config().engine.io_priority;
                auto queryGrp = seastar::create_scheduling_group("ts_query", ioCfg.query_shares).get();
                auto writeGrp = seastar::create_scheduling_group("ts_write", ioCfg.write_shares).get();
                auto compactGrp = seastar::create_scheduling_group("ts_compact", ioCfg.compaction_shares).get();
                auto flushGrp = seastar::create_scheduling_group("ts_flush", ioCfg.flush_shares).get();

                g_engine
                    .invoke_on_all([queryGrp, writeGrp, compactGrp, flushGrp](Engine& engine) {
                        engine.setIOSchedulingGroups(queryGrp, writeGrp, compactGrp, flushGrp);
                        return seastar::make_ready_future<>();
                    })
                    .get();

                // Partitioned clusters must converge shared tier-0 inputs to
                // VShard-pure immutable files. Set the mode before the first
                // background compaction can be scheduled; changing it after
                // the loop starts leaves an order-dependent window where the
                // server can emit another mixed higher-tier generation.
                const bool partitionCompaction = timestar::config().cluster.enabled &&
                                                 timestar::config().cluster.partitioned;
                g_engine
                    .invoke_on_all([partitionCompaction](Engine& engine) {
                        engine.setVShardPartitionedCompaction(partitionCompaction);
                        return seastar::make_ready_future<>();
                    })
                    .get();

                // Compaction placement depends on the groups above, so the loop
                // starts only once they have been distributed to every shard.
                g_engine.invoke_on_all([](Engine& engine) { return engine.startBackgroundCompaction(); }).get();

                // Set back-reference so Engine can do cross-shard operations.
                g_engine
                    .invoke_on_all([](Engine& engine) {
                        engine.setShardedRef(&g_engine);
                        return seastar::make_ready_future<>();
                    })
                    .get();

                // Load retention policies from NativeIndex and broadcast to all shards
                g_engine.invoke_on(0, [](Engine& engine) { return engine.loadAndBroadcastRetentionPolicies(); }).get();

                // Start the retention sweep timer on shard 0 (15-minute interval)
                g_engine
                    .invoke_on(0,
                               [](Engine& engine) {
                                   engine.startRetentionSweepTimer();
                                   return seastar::make_ready_future<>();
                               })
                    .get();

                // Commit the shard count now that Engine initialization has
                // produced the canonical shard directories. Idempotent for a
                // matching store; records the count for a fresh one.
                shardStoreStartup.commitEngineInitialization();
                timestar::http_log.info("Engine init completed on all shards");
            } catch (const std::bad_alloc& e) {
                timestar::http_log.error("bad_alloc during Engine init: {}", e.what());
                // Print backtrace for debugging according to Seastar docs
                timestar::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            } catch (const std::exception& e) {
                timestar::http_log.error("Exception during Engine init: {}", e.what());
                // Print backtrace for debugging
                timestar::http_log.error("Backtrace:\n{}", current_backtrace());
                throw;
            }

            // Start background tasks on all shards for WAL->TSM conversion
            timestar::http_log.info("Starting background tasks on all shards...");
            g_engine.invoke_on_all([](Engine& engine) { return engine.startBackgroundTasks(); }).get();

            // M2: start the VShard-partitioned data plane (RPC listener + router +
            // coordinator) on shard 0. Gated on enabled+partitioned so single-node
            // and M1 clusters never start it. A start failure (e.g. a bad address)
            // fails the boot -- a partitioned node that cannot serve its own VShards
            // must not come up pretending to.
            {
                const auto& cc = timestar::config().cluster;
                if (cc.enabled && cc.partitioned) {
                    if (clusterJournalIdentity)
                        g_clusterDataPlane.setJournalIdentity(*clusterJournalIdentity);
                    if (clusterTls)
                        g_clusterDataPlane.setTlsCredentials(*clusterTls);
                    g_clusterDataPlane.start(cc, g_engine).get();
                    g_clusterPartitioned = true;
                    // Route /query through the data plane (fan out to owners + merge).
                    // The data plane lives on shard 0; hop there from the request shard.
                    timestar::http::HttpQueryHandler::clusterQueryHook = [](timestar::QueryRequest q) {
                        return seastar::smp::submit_to(
                            0u, [q = std::move(q)]() mutable { return g_clusterDataPlane.query(std::move(q)); });
                    };
                    // Route /write through the data plane (per-series owner routing).
                    //
                    // A REPLICATED write never rendezvouses on shard 0: the request
                    // shard splits the batch by owning shard and dispatches straight to
                    // those shards' Raft planes, which forward remote leaders from their
                    // own peer clients. Shipping the whole batch to shard 0 first made
                    // that one core hash, group and re-own every point the node wrote,
                    // and it stayed the profile outlier (persist 132ms vs 3-7ms) after
                    // both transports were sharded. RF=1 still routes through shard 0 --
                    // its router and its peer clients are single instances there.
                    const bool replicatedWrites = cc.replication_factor > 1;
                    timestar::http::HttpWriteHandler::clusterWriteHook =
                        [replicatedWrites](timestar::data::WriteBatch b) {
                            if (replicatedWrites)
                                return g_clusterDataPlane.writeFromShard(std::move(b));
                            return seastar::smp::submit_to(
                                0u, [b = std::move(b)]() mutable { return g_clusterDataPlane.write(std::move(b)); });
                        };
                    if (replicatedWrites) {
                        timestar::http::HttpDeleteHandler::clusterDeleteHook = [](std::string seriesKey,
                                                                                  uint64_t startTime,
                                                                                  uint64_t endTime) {
                            return g_clusterDataPlane.deleteRangeFromShard(std::move(seriesKey), startTime, endTime);
                        };
                    }
                    // Route metadata endpoints through the scatter+merge.
                    timestar::http::HttpMetadataHandler::clusterMetadataHook = [](timestar::data::MetadataRequest r) {
                        return seastar::smp::submit_to(
                            0u, [r = std::move(r)]() mutable { return g_clusterDataPlane.metadata(std::move(r)); });
                    };
                    timestar::http_log.info("VShard-partitioned data plane started (node {})", cc.node_id);
                }
            }

            timestar::http_log.info("Engine initialized successfully with background tasks");
            g_ready.store(true, std::memory_order_release);

            // STEP 2: Create stop signal handler
            // Note: HTTP handlers are created per-shard inside set_routes() to avoid
            // cross-shard memory access. Each shard gets its own handler instance
            // allocated on its own heap.
            seastar_apps_lib::stop_signal stop_signal;

            // Initialize auth token if auth is enabled
            if (timestar::config().server.auth_enabled) {
                g_authTokenStorage = timestar::config().server.auth_token;
                if (g_authTokenStorage.empty()) {
                    g_authTokenStorage = timestar::http::generateToken(32);
                    timestar::http_log.debug("Auth enabled — generated token: {}",
                                             timestar::http::maskToken(g_authTokenStorage));
                } else {
                    timestar::http_log.debug("Auth enabled — using configured token: {}",
                                             timestar::http::maskToken(g_authTokenStorage));
                }
                // Release-store pointer after storage is fully constructed; any
                // shard that subsequently acquire-loads this pointer in authToken()
                // will see the completed string.
                g_authToken.store(&g_authTokenStorage, std::memory_order_release);
            } else {
                timestar::http_log.info("Auth disabled — all endpoints are unauthenticated");
            }

            auto server = std::make_unique<http_server_control>();

            // Start the HTTP server
            try {
                server->start().get();

                // Limit request body size to prevent memory exhaustion from oversized payloads.
                // Uses the larger of write/query body size limits from config.
                // Seastar enforces this at the connection layer before buffering the full body.
                auto maxContentLen =
                    std::max(timestar::config().http.max_write_body_size, timestar::config().http.max_query_body_size);
                server->server()
                    .invoke_on_all(
                        [maxContentLen](httpd::http_server& s) { s.set_content_length_limit(maxContentLen); })
                    .get();

                server->set_routes(set_routes).get();

                // Register Prometheus metrics endpoint at /metrics.
                // Exposes all per-shard TimeStar counters/gauges plus Seastar
                // built-in metrics (reactor, I/O, scheduler, memory).
                seastar::prometheus::config promConfig;
                promConfig.metric_help = "TimeStar TSDB metrics";
                promConfig.prefix = "timestar";
                seastar::prometheus::add_prometheus_routes(server->server(), promConfig).get();

                // Least-connections accept balancing, NOT the kernel-hash
                // default. SO_REUSEPORT hashes the peer 4-tuple, and with a
                // handful of client connections (especially over loopback) all
                // of them routinely land on ONE shard. Measured in a 6GB soak:
                // shard 0 took all 8 connections, its reactor saturated on
                // request parsing (main at 1000 shares), its compaction fiber
                // (ts_compact, 10 shares) got ~1% CPU and fell 116 tier-0
                // files behind, and the accumulated sparse indexes exhausted
                // the shard's memory pool into a bad_alloc storm -- while
                // shard 1 sat healthy. connection_distribution sends each new
                // connection to the least-loaded shard instead.
                seastar::listen_options httpLo;
                httpLo.lba = seastar::server_socket::load_balancing_algorithm::connection_distribution;
                httpLo.reuse_address = true;
                server->listen(seastar::socket_address(seastar::net::inet_address("0.0.0.0"), port), httpLo).get();
            } catch (const std::exception& e) {
                timestar::http_log.error("Failed to start HTTP server: {}", e.what());
                // http_server_control also owns a sharded service. Stop any
                // instances that start() created before the later setup step
                // failed; the lifecycle guard above handles data plane + Engine.
                try {
                    server->stop().get();
                } catch (const std::exception& cleanupError) {
                    timestar::http_log.error("HTTP cleanup after failed startup also failed: {}", cleanupError.what());
                }
                throw;
            }

            timestar::http_log.info("TimeStar HTTP Server listening on port {} ...", port);
            timestar::http_log.info("Available endpoints:");
            timestar::http_log.info("  GET  /         - Root endpoint");
            timestar::http_log.info("  GET  /test     - Test message");
            timestar::http_log.info("  GET  /health   - Health check");
            timestar::http_log.info("  POST /write    - Write time series data");
            timestar::http_log.info("  POST /query    - Query time series data");
            timestar::http_log.info("  POST /delete   - Delete time series data");
            timestar::http_log.info("  POST /subscribe - Subscribe to streaming data (SSE)");
            timestar::http_log.info("  GET  /metrics  - Prometheus metrics");
            timestar::http_log.info("  GET  /version  - Build version info");

            // Wait for stop signal
            stop_signal.wait().get();

            // Graceful shutdown with configurable timeout.
            // If any gate drain, WAL flush, or index close hangs beyond the
            // deadline, we log an error and exit — the OS reclaims resources
            // and WAL recovery handles unflushed data on restart.
            const auto timeoutSec = timestar::config().server.shutdown_timeout_seconds;
            timestar::http_log.info("Shutting down (timeout: {}s)...", timeoutSec);

            auto doShutdown = [&]() -> seastar::future<> {
                co_await server->stop();
                co_await seastar::smp::invoke_on_all([] {
                    if (g_streamHandler)
                        return g_streamHandler->stop();
                    return seastar::make_ready_future<>();
                });
                // Stop the data plane (peer clients + RPC server) BEFORE the engine,
                // so no forwarded write/query can arrive after the Engine is gone.
                if (g_clusterPartitioned) {
                    co_await g_clusterDataPlane.stop();
                    g_clusterPartitioned = false;
                }
                co_await g_engine.invoke_on_all([](Engine& engine) { return engine.stop(); });
                co_await g_engine.stop();
            };

            try {
                if (timeoutSec > 0) {
                    auto deadline = seastar::lowres_clock::now() + std::chrono::seconds(timeoutSec);
                    seastar::with_timeout(deadline, doShutdown()).get();
                } else {
                    doShutdown().get();
                }
                timestar::http_log.info("Shutdown complete");
                // Normal shutdown stopped every guarded sharded service. Only
                // now is it safe to disarm failure-path cleanup.
                engineGuard.cancel();
            } catch (const seastar::timed_out_error&) {
                // with_timeout does NOT cancel the doShutdown() coroutine — it
                // continues running in the background holding references to
                // stack locals (server, g_engine). Returning normally would
                // destroy those objects while the coroutine is still live,
                // causing use-after-free. Use _exit() to terminate immediately;
                // the OS reclaims all resources and WAL recovery handles
                // unflushed data on restart.
                timestar::http_log.error("Shutdown timed out after {}s — forcing exit", timeoutSec);
                std::_Exit(1);
            }

            return 0;
        });
    });
}
