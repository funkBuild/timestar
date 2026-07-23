#include "tsm.hpp"

#include "bool_encoder_rle.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "logger.hpp"
#include "slice_buffer.hpp"
#include "string_encoder.hpp"
#include "zigzag.hpp"

#include <algorithm>
#include <filesystem>
#include <numeric>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/later.hh>
#include <string_view>

// Block header: uint8_t type + uint32_t timestampSize + uint32_t timestampBytes
static constexpr size_t BLOCK_HEADER_SIZE = sizeof(uint8_t) + 2 * sizeof(uint32_t);  // 9 bytes

// (tlStringDict removed — dictionary is now passed explicitly through the call chain)

// Phase 0: Check whether a block's time range overlaps any tombstone range.
// tombstoneRanges must be sorted by start time (as returned by getTombstoneRanges).
// Uses binary search: O(log T) per block instead of scanning all tombstones.
static bool blockOverlapsTombstones(uint64_t blockMin, uint64_t blockMax,
                                    const std::vector<std::pair<uint64_t, uint64_t>>& tombstoneRanges) {
    if (tombstoneRanges.empty())
        return false;
    // Find first tombstone range whose start > blockMax — all earlier ranges potentially overlap.
    // Then check backwards: any range whose end >= blockMin overlaps the block.
    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                               std::make_pair(blockMax, std::numeric_limits<uint64_t>::max()));
    // Check all ranges before 'it' — the one just before could overlap
    if (it != tombstoneRanges.begin()) {
        --it;
        // Walk backwards checking for overlap: range overlaps block if range.end >= blockMin && range.start <= blockMax
        // Since ranges are sorted by start, once range.start > blockMax we're past the block.
        // We go backwards until range.end < blockMin.
        for (auto rit = it;; --rit) {
            if (rit->second >= blockMin && rit->first <= blockMax)
                return true;
            // Ranges are sorted by start and non-overlapping (merged).
            // Once a range ends before the block starts, all earlier ranges do too.
            if (rit->second < blockMin)
                break;
            if (rit == tombstoneRanges.begin())
                break;
        }
    }
    return false;
}

// Select the index blocks overlapping [startTime, endTime] (inclusive) as a contiguous
// range.  Index blocks are sorted by minTime with non-decreasing maxTime
// (the writer emits each series' blocks sequentially in time order, and the
// compactor's zero-copy path requires sorted non-overlapping blocks; its
// merge path re-encodes fully sorted data), so the set of blocks matching
// (minTime <= endTime && startTime <= maxTime) is contiguous: two binary
// searches replace the previous O(blocks) copy_if scan.
static std::span<const TSMIndexBlock> overlappingBlockRange(const std::vector<TSMIndexBlock>& blocks,
                                                            uint64_t startTime, uint64_t endTime) {
    auto first = std::partition_point(blocks.begin(), blocks.end(),
                                      [startTime](const TSMIndexBlock& b) { return b.maxTime < startTime; });
    auto last =
        std::partition_point(first, blocks.end(), [endTime](const TSMIndexBlock& b) { return b.minTime <= endTime; });
    return {first, last};
}

// Block-into-aggregator decode helpers. All four (Float, Integer, Boolean,
// COUNT-only) share an identical prologue: parse the 9-byte block header,
// time-filtered decode of timestamps into a thread-local scratch buffer.
// Only the value path differs.
//
// NOTE: thread_local scratch is safe in these functions because none of
// them suspend (no co_await). Adding any co_await would require switching
// to coroutine-local buffers.

struct BlockHeaderInfo {
    size_t valueOffset;    // byte offset within `data` where values start
    size_t valueByteSize;  // value-region length in bytes
    size_t nSkipped;       // points filtered out before startTime
    size_t nTimestamps;    // decoded timestamps now in tsScratch
    size_t totalInBlock;   // total points stored in the block (pre-filter)
};

// Parse the block header and decode timestamps (filtered by [startTime, endTime], inclusive)
// into `tsScratchOut` (cleared and reserved before decode). Returns nullopt on
// malformed header. nTimestamps==0 is a valid result (no timestamps in range).
// `timestampSize` is the decoded VALUE COUNT and is read raw from the block
// header, then used to size allocations. `timestampBytes` (the compressed byte
// length) is bounds-checked against the block, but the count never was -- so a
// corrupt or truncated header turned directly into a reserve() of up to
// UINT32_MAX * 8 bytes (~34 GB) and a std::bad_alloc far from the real fault.
//
// The two are cross-checkable. The densest the integer encoder can possibly be
// is its constant-delta fast path (integer_encoder_ffor.cpp encodeBlock): a
// block of up to kBlockSize=1024 values whose deltas are all equal emits ONLY a
// 2-word header, i.e. 1024 values in 16 bytes = 64 values per byte, with no
// partition-level prefix (encodeInto returns exactly the block bytes).
//
// 64:1 is therefore the true theoretical maximum -- and it is not a corner case,
// it is the COMMON case for constant-interval timestamps. Checking against it
// exactly would leave zero margin, so any future improvement to that encoding
// would start silently rejecting valid blocks. Several callers of this predicate
// return nullptr/nullopt/0 rather than throwing, so a false rejection is silent
// data loss, not a visible error. Hence the deliberate 4x slack.
//
// This is a sanity check on a raw on-disk uint32, not a tight bound: its job is
// to stop a corrupt count from driving a multi-GB reserve() (UINT32_MAX * 8
// bytes is ~34 GB). At a typical 4 KB block even the slackened bound caps the
// count around 1M, far below anything that could exhaust memory.
inline constexpr uint64_t MAX_TIMESTAMP_VALUES_PER_BYTE = 64;  // encoder's theoretical best
inline constexpr uint64_t TIMESTAMP_COUNT_SLACK_FACTOR = 4;    // margin against future encoders

// Enforce the coupling the comment above describes: if kBlockSize grows, the
// encoder gets denser and this bound must grow with it, or valid blocks start
// being rejected. (kBlockSize values per 16-byte header = kBlockSize/16 values
// per byte at the theoretical densest.)
static_assert(IntegerEncoderFFOR::kBlockSize / 16 <= MAX_TIMESTAMP_VALUES_PER_BYTE * TIMESTAMP_COUNT_SLACK_FACTOR,
              "Integer encoder can now emit more values per byte than the reader's plausibility bound allows; "
              "raise MAX_TIMESTAMP_VALUES_PER_BYTE/TIMESTAMP_COUNT_SLACK_FACTOR or valid blocks will be dropped");

[[nodiscard]] static bool timestampCountIsPlausible(uint32_t timestampSize, uint32_t timestampBytes) {
    const uint64_t maxPlausible =
        static_cast<uint64_t>(timestampBytes) * MAX_TIMESTAMP_VALUES_PER_BYTE * TIMESTAMP_COUNT_SLACK_FACTOR;
    return static_cast<uint64_t>(timestampSize) <= maxPlausible;
}

static std::optional<BlockHeaderInfo> parseHeaderAndDecodeTimestamps(const uint8_t* data, uint32_t blockSize,
                                                                     uint64_t startTime, uint64_t endTime,
                                                                     std::vector<uint64_t>& tsScratchOut) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return std::nullopt;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return std::nullopt;
    if (!timestampCountIsPlausible(timestampSize, timestampBytes))
        return std::nullopt;

    tsScratchOut.clear();
    tsScratchOut.reserve(timestampSize);

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, tsScratchOut, startTime, endTime);

    if (timestampBytes + BLOCK_HEADER_SIZE > blockSize)
        return std::nullopt;
    const size_t valueOffset = BLOCK_HEADER_SIZE + timestampBytes;
    const size_t valueByteSize = blockSize - valueOffset;

    return BlockHeaderInfo{valueOffset, valueByteSize, nSkipped, nTimestamps, timestampSize};
}

static size_t decodeBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                                        timestar::BlockAggregator& aggregator) {
    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    auto hdr = parseHeaderAndDecodeTimestamps(data, blockSize, startTime, endTime, tsScratch);
    if (!hdr || hdr->nTimestamps == 0)
        return 0;

    valScratch.clear();
    CompressedSlice valuesSlice(data + hdr->valueOffset, hdr->valueByteSize);
    FloatDecoder::decode(valuesSlice, hdr->nSkipped, hdr->nTimestamps, valScratch);

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

static size_t decodeIntegerBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime,
                                               uint64_t endTime, timestar::BlockAggregator& aggregator) {
    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    auto hdr = parseHeaderAndDecodeTimestamps(data, blockSize, startTime, endTime, tsScratch);
    if (!hdr || hdr->nTimestamps == 0)
        return 0;

    // Decode zigzag-encoded integers into scratch, then convert to double for the aggregator.
    static thread_local std::vector<uint64_t> zigzagScratch;
    zigzagScratch.clear();
    zigzagScratch.reserve(hdr->nSkipped + hdr->nTimestamps);
    Slice valuesSlice(data + hdr->valueOffset, hdr->valueByteSize);
    IntegerEncoder::decode(valuesSlice, hdr->nSkipped + hdr->nTimestamps, zigzagScratch);

    valScratch.clear();
    valScratch.reserve(hdr->nTimestamps);
    // The value decoder decodes whole FFOR groups, so zigzagScratch may hold up
    // to a full group beyond nSkipped+nTimestamps; bound the loop so values stay
    // in sync with the decoded timestamps.
    const size_t valueEnd = std::min(hdr->nSkipped + hdr->nTimestamps, zigzagScratch.size());
    for (size_t i = hdr->nSkipped; i < valueEnd; ++i) {
        valScratch.push_back(static_cast<double>(ZigZag::zigzagDecode(zigzagScratch[i])));
    }

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

static size_t decodeBoolBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime,
                                            uint64_t endTime, timestar::BlockAggregator& aggregator) {
    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    auto hdr = parseHeaderAndDecodeTimestamps(data, blockSize, startTime, endTime, tsScratch);
    if (!hdr || hdr->nTimestamps == 0)
        return 0;

    // Decode RLE runs straight to doubles (bulk fill_n per run) — no
    // vector<bool> round-trip and no per-bit-proxy conversion loop.
    valScratch.clear();
    Slice valuesSlice(data + hdr->valueOffset, hdr->valueByteSize);
    BoolEncoderRLE::decodeToDouble(valuesSlice, hdr->nSkipped, hdr->nTimestamps, valScratch);

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

// COUNT-only block decode: decode timestamps only, skip value decompression.
//
// NaN awareness: Float blocks can carry NaN values verbatim, and COUNT must
// not count them (canonical: NaN = missing data — docs/nan_policy.md). The
// index blockCount is the block's non-NaN count; when it disagrees with the
// header's total point count, NaN is present and the values must be decoded
// so the fold can skip it. Legacy files (blockCount written pre-NaN-fix as
// the raw total) never mismatch and keep the fast timestamp-only path.
static size_t decodeBlockCountOnly(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                                   timestar::BlockAggregator& aggregator, TSMValueType seriesType,
                                   uint32_t statsValidCount) {
    static thread_local std::vector<uint64_t> tsScratch;
    auto hdr = parseHeaderAndDecodeTimestamps(data, blockSize, startTime, endTime, tsScratch);
    if (!hdr || hdr->nTimestamps == 0)
        return 0;

    if (seriesType == TSMValueType::Float && statsValidCount != static_cast<uint32_t>(hdr->totalInBlock)) [[unlikely]] {
        // NaN-carrying (or stats-less) Float block: full decode so the
        // aggregator's NaN-skipping COUNT fold produces the canonical count.
        return decodeBlockIntoAggregator(data, blockSize, startTime, endTime, aggregator);
    }

    aggregator.addTimestampsOnly(tsScratch);
    return tsScratch.size();
}

TSM::TSM(std::string _absoluteFilePath) {
    size_t filenameEndIndex = _absoluteFilePath.find_last_of(".");
    if (filenameEndIndex == std::string::npos)
        throw std::runtime_error("TSM invalid filename (no extension): " + _absoluteFilePath);
    size_t filenameStartIndex = _absoluteFilePath.find_last_of("/") + 1;

    if (filenameStartIndex >= filenameEndIndex)
        throw std::runtime_error("TSM invalid filename (empty basename): " + _absoluteFilePath);

    std::string filename = _absoluteFilePath.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex);

    // Optional data-sequence suffix on compaction outputs: "<tier>_<seq>_d<dataSeq>".
    // Strip it before the tier/seq parse below; legacy files without the
    // suffix use seqNum as their dataSeq.
    std::optional<uint64_t> parsedDataSeq;
    size_t dSuffixIndex = filename.find_last_of("_");
    if (dSuffixIndex != std::string::npos && dSuffixIndex + 1 < filename.size() && filename[dSuffixIndex + 1] == 'd') {
        try {
            size_t consumed = 0;
            const std::string digits = filename.substr(dSuffixIndex + 2);
            uint64_t v = std::stoull(digits, &consumed);
            if (!digits.empty() && consumed == digits.size()) {
                parsedDataSeq = v;
                filename = filename.substr(0, dSuffixIndex);
            }
        } catch (const std::exception&) {
            // Not a data-seq suffix — fall through to the normal parse.
        }
    }

    size_t underscoreIndex = filename.find_last_of("_");
    if (underscoreIndex == std::string::npos)
        throw std::runtime_error("TSM invalid filename:" + filename);

    try {
        tierNum = std::stoull(filename.substr(0, underscoreIndex));
        seqNum = std::stoull(filename.substr(underscoreIndex + 1));
        dataSeq = parsedDataSeq.value_or(seqNum);

        timestar::tsm_log.debug("tierNum={} seqNum={} dataSeq={}", tierNum, seqNum, dataSeq);
    } catch (const std::exception&) {
        throw std::runtime_error("TSM invalid filename:" + filename);
    }

    filePath = _absoluteFilePath;
}

seastar::future<> TSM::open() {
    std::string_view filePathView{filePath};
    tsmFile = co_await seastar::open_file_dma(filePathView, seastar::open_flags::ro);

    if (!tsmFile) {
        timestar::tsm_log.error("TSM unable to open: {}", filePath);
        throw std::runtime_error("TSM unable to open:" + filePath);
    }

    // Clean up file handle on any failure after successful open.
    // GCC 14 does not support co_await in catch blocks, so we capture
    // the exception, close outside the handler, then rethrow.
    std::exception_ptr openError;
    try {
        length = co_await tsmFile.size();

        // Read and validate file header (magic "TASM" + 1-byte version)
        if (length >= 5) {
            auto hdrBuf = co_await tsmFile.dma_read_exactly<uint8_t>(0, 5);
            // Validate magic bytes "TASM"
            if (hdrBuf.get()[0] != 'T' || hdrBuf.get()[1] != 'A' || hdrBuf.get()[2] != 'S' || hdrBuf.get()[3] != 'M') {
                throw std::runtime_error("Not a TSM file (bad magic): " + filePath);
            }
            fileVersion = hdrBuf.get()[4];
            if (fileVersion < TSM_VERSION_MIN || fileVersion > TSM_VERSION) {
                throw std::runtime_error("Unsupported TSM file version " + std::to_string(fileVersion) +
                                         " (supported: " + std::to_string(TSM_VERSION_MIN) + "-" +
                                         std::to_string(TSM_VERSION) + "): " + filePath);
            }
        }

        // Use lazy loading: read sparse index + bloom filter (not full index).
        // Minimum valid file: 5-byte header + 8-byte index offset footer = 13 bytes.
        if (length < 13) {
            throw std::runtime_error("TSM file too small (" + std::to_string(length) +
                                     " bytes, minimum 13): " + filePath);
        }
        co_await readSparseIndex();

        // Load tombstones if they exist
        co_await loadTombstones();
    } catch (...) {
        openError = std::current_exception();
    }

    if (openError) {
        // Close the leaked file handle before rethrowing
        try {
            co_await tsmFile.close();
        } catch (...) {
            // Ignore close errors during cleanup to avoid masking the original exception
        }
        std::rethrow_exception(openError);
    }
};

seastar::future<> TSM::close() {
    if (tsmFile) {
        co_await tsmFile.close();
    }
    co_return;
}

uint64_t TSM::rankAsInteger() {
    if (tierNum >= 16) {
        throw std::overflow_error("TSM::rankAsInteger: tierNum " + std::to_string(tierNum) +
                                  " >= 16 would overflow in (tierNum << 60)");
    }
    constexpr uint64_t maxSeqNum = (uint64_t{1} << 60) - 1;
    if (seqNum > maxSeqNum) [[unlikely]] {
        throw std::overflow_error("TSM::rankAsInteger: seqNum " + std::to_string(seqNum) + " exceeds 60-bit limit");
    }
    return (tierNum << 60) | seqNum;
}

uint64_t TSM::dataRank() {
    if (tierNum >= 16) {
        throw std::overflow_error("TSM::dataRank: tierNum " + std::to_string(tierNum) + " >= 16 exceeds 4-bit limit");
    }
    constexpr uint64_t maxDataSeq = (uint64_t{1} << 60) - 1;
    if (dataSeq > maxDataSeq) [[unlikely]] {
        throw std::overflow_error("TSM::dataRank: dataSeq " + std::to_string(dataSeq) + " exceeds 60-bit limit");
    }
    // dataSeq-dominant: write recency decides duplicate resolution; tier only
    // breaks ties between a compacted file and an input carrying the same
    // newest generation.
    return (dataSeq << 4) | tierNum;
}

// Lazy loading: read sparse index + build bloom filter
seastar::future<> TSM::readSparseIndex() {
    // Read index offset (last 8 bytes of file)
    auto indexOffsetBuf = co_await tsmFile.dma_read_exactly<uint8_t>(length - sizeof(uint64_t), sizeof(uint64_t));
    uint64_t indexOffset;
    std::memcpy(&indexOffset, indexOffsetBuf.get(), sizeof(uint64_t));

    // Validate indexOffset is within file bounds
    if (indexOffset >= length - sizeof(uint64_t)) {
        throw std::runtime_error("Corrupted TSM file: indexOffset " + std::to_string(indexOffset) +
                                 " is out of bounds (file size: " + std::to_string(length) + "): " + filePath);
    }

    // Read entire index section
    auto indexBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexOffset, length - indexOffset - sizeof(uint64_t));
    Slice indexSlice(indexBuf.get(), indexBuf.size());

    // First pass: Parse index to collect series and build sparse index
    // We need the actual count before initializing the bloom filter
    std::vector<SeriesId128> seriesIds;

    // Require a whole fixed header before touching it: SeriesId128::fromBytes
    // takes a raw pointer and does NOT bounds-check (unlike Slice::read), so a
    // few trailing bytes here would read past the buffer before the following
    // block-count read could throw.
    const uint32_t entryHeaderSize = tsmIndexEntryHeaderSize(fileVersion);
    while (indexSlice.bytesLeft() >= entryHeaderSize) {
        uint64_t entryStartOffset = indexOffset + indexSlice.offset;

        // Read series ID (16 bytes) — zero-copy from index buffer
        SeriesId128 seriesId =
            SeriesId128::fromBytes(reinterpret_cast<const char*>(indexSlice.data + indexSlice.offset), 16);
        indexSlice.offset += 16;

        // Read type (1 byte) and block count (u32 in V3, u16 before)
        uint8_t type = indexSlice.read<uint8_t>();
        uint32_t blockCount = (fileVersion >= 3) ? indexSlice.read<uint32_t>() : indexSlice.read<uint16_t>();

        // Block size depends on type and file version
        if (type > static_cast<uint8_t>(TSMValueType::Integer)) {
            throw std::runtime_error("TSM index corrupt: invalid type byte " + std::to_string(type));
        }
        auto seriesType = static_cast<TSMValueType>(type);
        size_t perBlockBytes = indexBlockBytes(seriesType, fileVersion);

        // Validate blockCount against remaining index data to prevent
        // reads past the end of the index on malformed files.
        size_t blockBytes = static_cast<size_t>(blockCount) * perBlockBytes;
        if (blockBytes > indexSlice.bytesLeft()) {
            throw std::runtime_error("TSM index corrupt: blockCount " + std::to_string(blockCount) + " requires " +
                                     std::to_string(blockBytes) + " bytes but only " +
                                     std::to_string(indexSlice.bytesLeft()) + " remain");
        }

        // Peek at first/last block metadata from the index for sparse lookups.
        uint64_t seriesMinTime = 0;
        uint64_t seriesMaxTime = 0;
        double firstValue = 0.0;
        double latestValue = 0.0;
        bool hasExtStats = false;
        bool boolFirst = false;
        bool boolLatest = false;

        if (blockCount > 0) {
            size_t blockStart = indexSlice.offset;
            size_t lastBlockStart = blockStart + (blockCount - 1) * perBlockBytes;

            // First block: minTime at offset 0
            std::memcpy(&seriesMinTime, indexSlice.data + blockStart, sizeof(uint64_t));
            // Last block: maxTime at offset 8
            std::memcpy(&seriesMaxTime, indexSlice.data + lastBlockStart + 8, sizeof(uint64_t));

            // Float: blockFirstValue at offset 64, blockLatestValue at offset 72
            if (seriesType == TSMValueType::Float) {
                std::memcpy(&firstValue, indexSlice.data + blockStart + 64, sizeof(double));
                std::memcpy(&latestValue, indexSlice.data + lastBlockStart + 72, sizeof(double));
                // NaN endpoints mark NaN-carrying blocks (writer sentinel, or
                // legacy blocks whose real endpoints were NaN): first/latest
                // shortcuts must decode instead (see parseIndexBlocksFromSlice).
                hasExtStats = !std::isnan(firstValue) && !std::isnan(latestValue);
            }
            // Integer (V2): first/latest as int64 at offset 56 and 64 within block
            // Layout: minTime(8) maxTime(8) offset(8) size(4) count(4) sum(8) min(8) max(8) first(8) latest(8)
            //         0         8          16        24      28       32      40      48      56       64
            else if (seriesType == TSMValueType::Integer && fileVersion >= 2) {
                int64_t intFirst, intLatest;
                std::memcpy(&intFirst, indexSlice.data + blockStart + 56, sizeof(int64_t));
                std::memcpy(&intLatest, indexSlice.data + lastBlockStart + 64, sizeof(int64_t));
                firstValue = static_cast<double>(intFirst);
                latestValue = static_cast<double>(intLatest);
                hasExtStats = true;
            }
            // Boolean (V2): first/latest as uint8 at offset 36 and 37
            else if (seriesType == TSMValueType::Boolean && fileVersion >= 2) {
                boolFirst = (indexSlice.data[blockStart + 36] != 0);
                boolLatest = (indexSlice.data[lastBlockStart + 37] != 0);
                firstValue = boolFirst ? 1.0 : 0.0;
                latestValue = boolLatest ? 1.0 : 0.0;
                hasExtStats = true;
            }
        }

        // Skip over the blocks (don't parse them yet)
        indexSlice.offset += blockBytes;

        // Phase 3: Skip over string dictionary if present (V2 String series)
        uint32_t dictBytes = 0;
        if (seriesType == TSMValueType::String && fileVersion >= 2) {
            if (indexSlice.offset + 4 > indexSlice.length_) {
                // No room for the dictionary length — the entry is truncated.
                // Must THROW, not tolerate: a partially parsed index registers
                // the file with only a prefix of its series, and a later
                // compaction of that file deletes the source — permanently
                // destroying the unparsed series instead of leaving them
                // loudly unavailable.
                throw std::runtime_error("TSM index corrupt: string entry truncated before dictionary length");
            }
            std::memcpy(&dictBytes, indexSlice.data + indexSlice.offset, 4);
            if (dictBytes > 16 * 1024 * 1024) {
                throw std::runtime_error("Corrupt TSM: dictionary too large");
            }
            if (indexSlice.offset + 4 + dictBytes > indexSlice.length_) {
                // Dictionary extends past index end — same rule as above:
                // reject the whole file rather than silently keeping a prefix.
                throw std::runtime_error("TSM index corrupt: dictionary size " + std::to_string(dictBytes) +
                                         " at offset " + std::to_string(indexSlice.offset) +
                                         " extends past index end (" + std::to_string(indexSlice.length_) + ")");
            }
            indexSlice.offset += 4 + dictBytes;
        }

        // Calculate total entry size (header + blocks + optional dictionary).
        // entrySize is uint64 so it cannot cap the widened uint32 blockCount;
        // blockBytes is already bounded by the bytesLeft() check above, i.e. by
        // the real index size, so no separate overflow guard is needed.
        uint64_t entrySize = entryHeaderSize + static_cast<uint64_t>(blockBytes);
        if (seriesType == TSMValueType::String && fileVersion >= 2) {
            entrySize += 4 + dictBytes;
        }

        // Store sparse entry with time bounds + first/latest values
        SparseIndexEntry sparseEntry{.seriesId = seriesId,
                                     .fileOffset = entryStartOffset,
                                     .entrySize = entrySize,
                                     .seriesType = seriesType,
                                     .minTime = seriesMinTime,
                                     .maxTime = seriesMaxTime,
                                     .firstValue = firstValue,
                                     .latestValue = latestValue,
                                     .hasExtendedStats = hasExtStats,
                                     .boolFirstValue = boolFirst,
                                     .boolLatestValue = boolLatest};
        sparseIndex.insert({seriesId, sparseEntry});

        // Collect series ID for bloom filter
        seriesIds.push_back(seriesId);
    }

    // The loop exits cleanly only at EXACTLY zero bytes left. A short tail
    // (0 < bytesLeft < header size) means the index is truncated: accepting
    // the parsed prefix would register the file with only some of its series,
    // and a later compaction of that file DELETES the source — permanently
    // destroying the unparsed series' data. Reject the whole file instead
    // (openTsmFile() logs it loudly and leaves the file on disk).
    if (indexSlice.bytesLeft() != 0) {
        throw std::runtime_error("TSM index corrupt: " + std::to_string(indexSlice.bytesLeft()) +
                                 " trailing bytes after " + std::to_string(seriesIds.size()) +
                                 " index entries — index is truncated: " + filePath);
    }

    // Now initialize bloom filter with the ACTUAL series count
    // This is critical for higher-tier TSM files that may have 100K+ series
    size_t actualSeriesCount = seriesIds.size();

    bloom_parameters params;
    params.projected_element_count = std::max(actualSeriesCount, size_t{1});
    params.false_positive_probability = bloomFpr();
    params.compute_optimal_parameters();
    seriesBloomFilter = bloom_filter(params);

    // Add all series to the properly-sized bloom filter
    for (const auto& seriesId : seriesIds) {
        seriesBloomFilter.insert(seriesId.getRawData());
    }

    timestar::tsm_log.info("Loaded sparse index for {} (tier {}): {} series, bloom filter: {} bytes", filePath, tierNum,
                           sparseIndex.size(), seriesBloomFilter.size());
}

// Shared helper: parse index blocks (and optional string dictionary) from a Slice.
// The caller must have already read the series ID, type byte, and block count from
// the Slice before calling this.  On return the Slice offset is advanced past all
// block data and the optional string dictionary.
void TSM::parseIndexBlocksFromSlice(Slice& indexSlice, TSMIndexEntry& entry, uint32_t blockCount) const {
    // Bound the reserve against the bytes actually available before trusting the
    // count. Reads are individually checked by Slice::read(), but the reserve
    // happens first: a uint16 count could only ever ask for ~5.7 MB, whereas a
    // corrupt uint32 count can ask for ~378 GB and throw std::bad_alloc before
    // any read is attempted. The sparse-index path pre-validates this, but
    // getFullIndexEntry()/prefetchFullIndexEntries() do not.
    const size_t perBlockBytes = indexBlockBytes(entry.seriesType, fileVersion);
    const size_t maxBlocks = perBlockBytes ? indexSlice.bytesLeft() / perBlockBytes : 0;
    if (blockCount > maxBlocks) {
        throw std::runtime_error("TSM index corrupt: blockCount " + std::to_string(blockCount) + " exceeds " +
                                 std::to_string(maxBlocks) + " blocks available in " +
                                 std::to_string(indexSlice.bytesLeft()) + " remaining bytes");
    }
    entry.indexBlocks.reserve(blockCount);
    for (uint32_t i = 0; i < blockCount; ++i) {
        TSMIndexBlock block;
        block.minTime = indexSlice.read<uint64_t>();
        block.maxTime = indexSlice.read<uint64_t>();
        block.offset = indexSlice.read<uint64_t>();
        block.size = indexSlice.read<uint32_t>();
        if (entry.seriesType == TSMValueType::Float) {
            block.blockSum = indexSlice.read<double>();
            block.blockMin = indexSlice.read<double>();
            block.blockMax = indexSlice.read<double>();
            block.blockCount = indexSlice.read<uint32_t>();
            block.blockM2 = indexSlice.read<double>();
            block.blockFirstValue = indexSlice.read<double>();
            block.blockLatestValue = indexSlice.read<double>();
            // Extended stats are unusable when the endpoint values are NaN:
            // the writer stores NaN first/latest sentinels for NaN-carrying
            // blocks (LATEST/FIRST/STDDEV must decode and skip per value),
            // and legacy files whose block endpoints were genuinely NaN are
            // caught by the same predicate. See docs/nan_policy.md.
            block.hasExtendedStats = !std::isnan(block.blockFirstValue) && !std::isnan(block.blockLatestValue);
        } else if (fileVersion >= 2) {
            // V2: all non-Float types have at least blockCount
            if (entry.seriesType == TSMValueType::Integer) {
                // 72 bytes: 28 base + count(4) + sum(8) + min(8) + max(8) + first(8) + latest(8)
                block.blockCount = indexSlice.read<uint32_t>();
                int64_t intSum = indexSlice.read<int64_t>();
                int64_t intMin = indexSlice.read<int64_t>();
                int64_t intMax = indexSlice.read<int64_t>();
                int64_t intFirst = indexSlice.read<int64_t>();
                int64_t intLatest = indexSlice.read<int64_t>();
                block.blockSum = static_cast<double>(intSum);
                block.blockMin = static_cast<double>(intMin);
                block.blockMax = static_cast<double>(intMax);
                block.blockFirstValue = static_cast<double>(intFirst);
                block.blockLatestValue = static_cast<double>(intLatest);
                block.hasExtendedStats = true;
            } else if (entry.seriesType == TSMValueType::Boolean) {
                // 40 bytes: 28 base + count(4) + trueCount(4) + firstValue(1) + latestValue(1) + pad(2)
                block.blockCount = indexSlice.read<uint32_t>();
                block.boolTrueCount = indexSlice.read<uint32_t>();
                block.boolFirstValue = (indexSlice.read<uint8_t>() != 0);
                block.boolLatestValue = (indexSlice.read<uint8_t>() != 0);
                indexSlice.offset += 2;  // skip padding
                // Convert for aggregator compatibility
                block.blockSum = static_cast<double>(block.boolTrueCount);
                block.blockMin = (block.boolTrueCount < block.blockCount) ? 0.0 : 1.0;
                block.blockMax = (block.boolTrueCount > 0) ? 1.0 : 0.0;
                block.blockFirstValue = block.boolFirstValue ? 1.0 : 0.0;
                block.blockLatestValue = block.boolLatestValue ? 1.0 : 0.0;
                block.hasExtendedStats = true;
            } else if (entry.seriesType == TSMValueType::String) {
                // 32 bytes: 28 base + count(4)
                block.blockCount = indexSlice.read<uint32_t>();
                // No value stats for strings — blockCount enables COUNT pushdown
            }
        }
        entry.indexBlocks.push_back(block);
    }

    // Phase 3: Parse string dictionary if present
    if (entry.seriesType == TSMValueType::String && fileVersion >= 2 && indexSlice.offset + 4 <= indexSlice.length_) {
        uint32_t dictSize = indexSlice.read<uint32_t>();
        if (dictSize > 0 && indexSlice.offset + dictSize <= indexSlice.length_) {
            auto dict = StringEncoder::deserializeDictionary(indexSlice, dictSize);
            if (dict.valid) {
                entry.stringDictionary = std::make_shared<const std::vector<std::string>>(std::move(dict.entries));
            }
        }
    }
}

// Lazy load full index entry for a series (single DMA read)
seastar::future<TSMIndexEntry*> TSM::getFullIndexEntry(const SeriesId128& seriesId) {
    // Step 1: Bloom filter check (fast, in-memory)
    if (!seriesBloomFilter.contains(seriesId.getRawData())) {
        // Definitely not in this file
        co_return nullptr;
    }

    // Step 2: Check cache (promotes to front on hit)
    if (auto* cached = fullIndexCache.get(seriesId)) {
        co_return cached;
    }

    // Step 3: Sparse index lookup
    auto sparseIt = sparseIndex.find(seriesId);
    if (sparseIt == sparseIndex.end()) {
        // False positive from bloom filter (0.1% chance)
        timestar::tsm_log.trace("Bloom filter false positive for series {}", seriesId.toHex());
        co_return nullptr;
    }

    // Step 4: Single DMA read for entire series entry (through the read
    // elevator: concurrent per-series index misses in the same reactor tick
    // coalesce — index entries are adjacent in the index region).
    const auto& sparse = sparseIt->second;
    auto entryBuf = co_await coalescedDmaRead(sparse.fileOffset, sparse.entrySize);

    // Step 5: Parse the full entry
    Slice entrySlice(entryBuf.get(), entryBuf.size());

    TSMIndexEntry fullEntry;

    // Parse series ID (verify) — zero-copy from entry buffer
    fullEntry.seriesId = SeriesId128::fromBytes(reinterpret_cast<const char*>(entrySlice.data + entrySlice.offset), 16);
    entrySlice.offset += 16;

    // Parse type and block count
    // Validate before casting: an out-of-range type byte falls through every
    // branch in parseIndexBlocksFromSlice, which then parses a 28-byte base
    // while indexBlockBytes() computed a different stride -- a silent misparse
    // instead of an error. The sparse-index path already checks this.
    {
        uint8_t typeByte = entrySlice.read<uint8_t>();
        if (typeByte > static_cast<uint8_t>(TSMValueType::Integer)) {
            throw std::runtime_error("TSM index corrupt: invalid type byte " + std::to_string(typeByte));
        }
        fullEntry.seriesType = static_cast<TSMValueType>(typeByte);
    }
    // Block count is u32 in V3, u16 before.
    uint32_t blockCount = (fileVersion >= 3) ? entrySlice.read<uint32_t>() : entrySlice.read<uint16_t>();

    // Parse all blocks and optional string dictionary
    parseIndexBlocksFromSlice(entrySlice, fullEntry, blockCount);

    // Step 6: Cache it with LRU eviction.
    //
    // Concurrency note (Seastar cooperative model):
    // The co_await at Step 4 above is a suspension point.  While this coroutine
    // was suspended waiting for DMA I/O, another coroutine running on the same
    // shard (e.g. a parallel prefetchFullIndexEntries call) could have already
    // populated the cache for this exact seriesId.  Re-checking here prevents
    // a double-insert: two LRU list nodes for the same key with only one map
    // entry, causing the orphaned node to later evict the map entry prematurely.
    //
    // There is NO race between the size() check and the mutation below because
    // there is no co_await between them — the Seastar reactor cannot schedule
    // another task between these two lines, making the check-then-mutate
    // sequence effectively atomic within a shard.
    if (auto* existing = fullIndexCache.get(seriesId)) {
        // Another coroutine beat us to it while we were suspended on DMA I/O.
        co_return existing;
    }

    // Insert (LRUCache evicts to stay under the byte budget) and return the
    // stable pointer into the cache node (put() returns it directly — no
    // second hash lookup).
    auto* inserted = fullIndexCache.put(seriesId, std::move(fullEntry));

    timestar::tsm_log.trace("Loaded full index entry for series {} ({} blocks)", seriesId.toHex(), blockCount);

    co_return inserted;
}

// Bulk prefetch: identify cache misses and issue coalesced DMA reads.
// Sorts entries by file offset and merges adjacent reads into larger chunks
// to reduce syscall count. A 256KB coalesce window typically merges 10-50
// individual reads into 1-2 DMA operations.
seastar::future<> TSM::prefetchFullIndexEntries(const std::vector<SeriesId128>& seriesIds) {
    // Collect entries that need fetching, with their file offsets for sorting
    struct FetchEntry {
        SeriesId128 id;
        uint64_t offset;
        uint64_t size;  // matches SparseIndexEntry::entrySize
    };
    std::vector<FetchEntry> toFetch;
    toFetch.reserve(seriesIds.size());

    for (const auto& seriesId : seriesIds) {
        if (fullIndexCache.contains(seriesId))
            continue;
        if (!seriesBloomFilter.contains(seriesId.getRawData()))
            continue;
        auto sparseIt = sparseIndex.find(seriesId);
        if (sparseIt == sparseIndex.end())
            continue;
        toFetch.push_back({seriesId, sparseIt->second.fileOffset, sparseIt->second.entrySize});
    }

    if (toFetch.empty())
        co_return;

    // Sort by file offset to enable coalescing
    std::sort(toFetch.begin(), toFetch.end(),
              [](const FetchEntry& a, const FetchEntry& b) { return a.offset < b.offset; });

    // Coalesce adjacent entries into read groups.
    // A gap <= COALESCE_GAP between entries gets included in a single read.
    static constexpr uint64_t COALESCE_GAP = 64 * 1024;  // 64KB gap threshold

    struct ReadGroup {
        uint64_t startOffset;
        uint64_t endOffset;  // exclusive
        std::vector<const FetchEntry*> entries;
    };

    std::vector<ReadGroup> groups;
    groups.push_back({toFetch[0].offset, toFetch[0].offset + toFetch[0].size, {&toFetch[0]}});

    for (size_t i = 1; i < toFetch.size(); ++i) {
        auto& cur = groups.back();
        uint64_t entryEnd = toFetch[i].offset + toFetch[i].size;
        if (toFetch[i].offset <= cur.endOffset + COALESCE_GAP) {
            // Coalesce: extend the current group
            cur.endOffset = std::max(cur.endOffset, entryEnd);
            cur.entries.push_back(&toFetch[i]);
        } else {
            // New group
            groups.push_back({toFetch[i].offset, entryEnd, {&toFetch[i]}});
        }
    }

    // Read each group with a single DMA read and parse all entries from the buffer
    for (auto& group : groups) {
        uint64_t readSize = group.endOffset - group.startOffset;
        seastar::temporary_buffer<char> buf;
        bool readFailed = false;
        try {
            buf = co_await tsmFile.dma_read_exactly<char>(group.startOffset, readSize);
        } catch (...) {
            readFailed = true;
        }
        if (readFailed) {
            // Fallback: read entries individually
            for (const auto* entry : group.entries) {
                co_await getFullIndexEntry(entry->id).discard_result();
            }
            continue;
        }

        for (const auto* entry : group.entries) {
            // Skip if another coroutine cached it while we were reading
            if (fullIndexCache.contains(entry->id))
                continue;

            uint64_t localOffset = entry->offset - group.startOffset;
            if (localOffset + entry->size > buf.size())
                continue;

            Slice entrySlice(reinterpret_cast<const uint8_t*>(buf.get() + localOffset), entry->size);

            TSMIndexEntry fullEntry;
            // Zero-copy series ID from entry buffer
            fullEntry.seriesId =
                SeriesId128::fromBytes(reinterpret_cast<const char*>(entrySlice.data + entrySlice.offset), 16);
            entrySlice.offset += 16;
            {
                uint8_t typeByte = entrySlice.read<uint8_t>();
                if (typeByte > static_cast<uint8_t>(TSMValueType::Integer)) {
                    // Prefetch is a best-effort cache warm; skip this entry rather
                    // than aborting the prefetch (and the query) for every other
                    // series in the batch. A real read of this series will fail
                    // loudly via getFullIndexEntry().
                    timestar::tsm_log.warn("Skipping prefetch of series with invalid type byte {}", typeByte);
                    continue;
                }
                fullEntry.seriesType = static_cast<TSMValueType>(typeByte);
            }
            // Block count is u32 in V3, u16 before.
            uint32_t blockCount = (fileVersion >= 3) ? entrySlice.read<uint32_t>() : entrySlice.read<uint16_t>();

            // Parse all blocks and optional string dictionary
            parseIndexBlocksFromSlice(entrySlice, fullEntry, blockCount);

            // Cache the entry (LRUCache evicts to stay under the byte budget)
            fullIndexCache.put(entry->id, std::move(fullEntry));
        }
    }
}

template <class T>
seastar::future<> TSM::readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                  TSMResult<T>& results) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);

    if (!indexEntry) {
        co_return;  // Series not in this file
    }

    // Take a refcount on the shared dictionary so it survives LRU cache
    // evictions across co_await DMA suspensions (use-after-free fix) — no
    // deep copy of the strings.
    [[maybe_unused]] std::shared_ptr<const std::vector<std::string>> localDictRef;
    [[maybe_unused]] const std::vector<std::string>* localStringDict = nullptr;
    if constexpr (std::is_same_v<T, std::string>) {
        if (indexEntry->stringDictionary && !indexEntry->stringDictionary->empty()) {
            localDictRef = indexEntry->stringDictionary;
            localStringDict = localDictRef.get();
        }
    }

    // Filter blocks by time range (contiguous range via binary search), copied
    // into a frame-local vector because the cache entry can be evicted across
    // the co_await suspensions below.
    auto overlapRange = overlappingBlockRange(indexEntry->indexBlocks, startTime, endTime);
    std::vector<TSMIndexBlock> blocksToScan(overlapRange.begin(), overlapRange.end());

    // Pre-allocate a slot per block so each coroutine writes to its own index,
    // avoiding the data race where multiple coroutines push_back concurrently
    // on the shared results.blocks vector after co_await suspension points.
    std::vector<std::unique_ptr<TSMBlock<T>>> blockSlots(blocksToScan.size());

    // Build index range for parallel_for_each to avoid relying on cooperative
    // scheduling for sequential counter increments.
    std::vector<size_t> slotIndices(blocksToScan.size());
    std::iota(slotIndices.begin(), slotIndices.end(), 0);

    co_await seastar::parallel_for_each(slotIndices, [&](size_t mySlot) -> seastar::future<> {
        auto block = co_await readSingleBlock<T>(blocksToScan[mySlot], startTime, endTime, localStringDict);
        if (block && !block->timestamps.empty()) {
            blockSlots[mySlot] = std::move(block);
        }
    });

    // Collect non-null results into the output TSMResult (single-threaded, safe)
    for (auto& block : blockSlots) {
        if (block) {
            results.appendBlock(std::move(block));
        }
    }

    // parallel_for_each does not guarantee ordering, so sort blocks by start time
    results.sort();
}

std::optional<TSMValueType> TSM::getSeriesType(const SeriesId128& seriesId) {
    // Check bloom filter first
    if (!seriesBloomFilter.contains(seriesId.getRawData())) {
        return {};
    }

    // Check full index cache (populated after getFullIndexEntry is called)
    if (auto* cached = fullIndexCache.get(seriesId)) {
        return cached->seriesType;
    }

    // Fall back to sparse index which stores the type from the initial index parse
    auto sparseIt = sparseIndex.find(seriesId);
    if (sparseIt != sparseIndex.end()) {
        return sparseIt->second.seriesType;
    }

    return {};
}

// Phase 1.1: Get index blocks without reading data
// Note: This returns blocks only if already loaded in cache
// For guaranteed results, caller should await getFullIndexEntry() first
const std::vector<TSMIndexBlock>& TSM::getSeriesBlocks(const SeriesId128& seriesId) const {
    static const std::vector<TSMIndexBlock> empty;
    // Check cache (promotes to front on hit)
    if (auto* cached = fullIndexCache.get(seriesId)) {
        return cached->indexBlocks;
    }

    // Not in cache - return empty
    return empty;
}

// --- Read elevator (see the header for the full contract) ---

seastar::future<seastar::temporary_buffer<uint8_t>> TSM::coalescedDmaRead(uint64_t offset, uint64_t size) {
    auto pending = std::make_unique<PendingDmaRead>();
    pending->offset = offset;
    pending->size = size;
    auto fut = pending->done.get_future();
    _pendingDmaReads.push_back(std::move(pending));
    if (!_dmaDispatchScheduled) {
        _dmaDispatchScheduled = true;
        // Defer to the back of the current task-queue run so every request
        // from concurrently-runnable series lands in this dispatch round.
        (void)seastar::yield().then([this] { return dispatchPendingDmaReads(); });
    }
    return fut;
}

seastar::future<> TSM::dispatchPendingDmaReads() {
    _dmaDispatchScheduled = false;
    auto pending = std::exchange(_pendingDmaReads, {});
    if (pending.empty()) {
        co_return;
    }

    std::sort(pending.begin(), pending.end(),
              [](const std::unique_ptr<PendingDmaRead>& a, const std::unique_ptr<PendingDmaRead>& b) {
                  return a->offset < b->offset;
              });

    // Merge requests whose gaps are cheaper to read through than to seek
    // past.  Gap bytes cost sequential bandwidth; a separate request costs an
    // IOP — on IOPS-billed volumes the 128KB trade is heavily in favour of
    // reading through.  The cap bounds the transient buffer a merged read
    // pins (every share() view holds the whole buffer alive until dropped).
    static constexpr uint64_t kMergeGapBytes = 128 * 1024;
    static constexpr uint64_t kMaxMergedRead = 8 * 1024 * 1024;

    struct MergedRange {
        uint64_t start = 0;
        uint64_t end = 0;  // exclusive
        size_t firstReq = 0;
        size_t lastReq = 0;  // inclusive
    };
    std::vector<MergedRange> ranges;
    ranges.push_back({pending[0]->offset, pending[0]->offset + pending[0]->size, 0, 0});
    for (size_t i = 1; i < pending.size(); ++i) {
        auto& cur = ranges.back();
        const uint64_t reqStart = pending[i]->offset;
        const uint64_t reqEnd = reqStart + pending[i]->size;
        const uint64_t mergedEnd = std::max(cur.end, reqEnd);
        if (reqStart <= cur.end + kMergeGapBytes && mergedEnd - cur.start <= kMaxMergedRead) {
            cur.end = mergedEnd;
            cur.lastReq = i;
        } else {
            ranges.push_back({reqStart, reqEnd, i, i});
        }
    }

    co_await seastar::parallel_for_each(ranges, [this, &pending](const MergedRange& range) -> seastar::future<> {
        std::exception_ptr ep;
        seastar::temporary_buffer<uint8_t> buf;
        try {
            buf = co_await tsmFile.dma_read_exactly<uint8_t>(range.start, range.end - range.start);
        } catch (...) {
            ep = std::current_exception();
        }
        for (size_t i = range.firstReq; i <= range.lastReq; ++i) {
            auto& req = *pending[i];
            if (ep) {
                req.done.set_exception(ep);
            } else if (req.offset - range.start + req.size > buf.size()) {
                req.done.set_exception(std::make_exception_ptr(
                    std::runtime_error("coalesced DMA read came back short: file truncated under a live query")));
            } else {
                req.done.set_value(buf.share(req.offset - range.start, req.size));
            }
        }
    });
}

// Rethrow the in-flight exception with this file's path appended (see the
// declaration for the full contract: bad_alloc passes through, annotation is
// idempotent, and the BlockDecodeError type is preserved so callers that
// classify on it keep working).
[[noreturn]] void TSM::rethrowWithFilePath() const {
    constexpr std::string_view marker = " [tsm ";
    try {
        throw;
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const timestar::BlockDecodeError& e) {
        if (std::string_view(e.what()).find(marker) != std::string_view::npos) {
            throw;
        }
        throw timestar::BlockDecodeError(std::string(e.what()) + " [tsm " + filePath + "]");
    } catch (const std::exception& e) {
        if (std::string_view(e.what()).find(marker) != std::string_view::npos) {
            throw;
        }
        throw std::runtime_error(std::string(e.what()) + " [tsm " + filePath + "]");
    }
}

// Phase 1.1: Read a single block and return it (not appending to results)
template <class T>
seastar::future<std::unique_ptr<TSMBlock<T>>> TSM::readSingleBlock(const TSMIndexBlock& indexBlock, uint64_t startTime,
                                                                   uint64_t endTime,
                                                                   const std::vector<std::string>* stringDict) {
    try {
        co_return co_await readSingleBlockImpl<T>(indexBlock, startTime, endTime, stringDict);
    } catch (...) {
        rethrowWithFilePath();
    }
}

template <class T>
seastar::future<std::unique_ptr<TSMBlock<T>>> TSM::readSingleBlockImpl(const TSMIndexBlock& indexBlock,
                                                                       uint64_t startTime, uint64_t endTime,
                                                                       const std::vector<std::string>* stringDict) {
    // Capture the dictionary pointer before co_await.  All callers pass
    // coroutine-frame-local copies that survive DMA suspensions, so a shallow
    // pointer save is sufficient here.
    const std::vector<std::string>* localDict = nullptr;
    if constexpr (std::is_same_v<T, std::string>) {
        localDict = stringDict;
    }

    // Fully-contained block: every point passes the time filter, so decode with
    // sentinel bounds to hit the branch-free timestamp fast path (same results).
    if (indexBlock.minTime >= startTime && indexBlock.maxTime <= endTime) {
        startTime = 0;
        endTime = UINT64_MAX;
    }

    auto blockBuf = co_await coalescedDmaRead(indexBlock.offset, indexBlock.size);
    Slice blockSlice(blockBuf.get(), blockBuf.size());

    if (indexBlock.size < BLOCK_HEADER_SIZE) {
        throw std::runtime_error("TSM block too small: " + std::to_string(indexBlock.size));
    }

    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > indexBlock.size - BLOCK_HEADER_SIZE) {
        throw std::runtime_error("TSM block timestampBytes exceeds block size");
    }
    if (!timestampCountIsPlausible(timestampSize, timestampBytes)) {
        throw std::runtime_error("TSM block timestampSize " + std::to_string(timestampSize) + " is impossible for " +
                                 std::to_string(timestampBytes) + " compressed bytes");
    }

    // Validate that the block's stored type matches the template parameter
    TSMValueType expectedType = getValueType<T>();
    if (static_cast<TSMValueType>(blockType) != expectedType) {
        throw std::runtime_error("TSM block type mismatch: block contains type " + std::to_string(blockType) +
                                 " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
    }

    auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime);
    size_t valueByteSize = indexBlock.size - timestampBytes - BLOCK_HEADER_SIZE;

    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Phase 3: Check if dictionary-encoded (STR2 magic)
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && localDict && !localDict->empty()) {
            StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, *localDict,
                                            blockResults->values);
        } else {
            StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Coroutine-local buffer — thread_local is unsafe in coroutines because
        // another coroutine on the same thread could clear it during a co_await.
        std::vector<uint64_t> rawUintScratch;
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        blockResults->values.reserve(nTimestamps);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            blockResults->values.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
    }

    // Same count contract as decodeBlockFlat(): these per-block decoders build a
    // TSMBlock whose consumers index values by a TIMESTAMP index
    // (TSMBlock::valueAt), so a desynced pair is an out-of-bounds read.
    //
    // Excess values are truncated (benign); a shortfall is raised, because the
    // only alternatives are to mispair real data or to drop the block and report
    // success -- a silent partial answer.
    if (blockResults->values.size() > blockResults->timestamps.size()) {
        blockResults->values.resize(blockResults->timestamps.size());
    } else if (blockResults->values.size() < blockResults->timestamps.size()) {
        throw timestar::BlockDecodeError("TSM block decode short: " + std::to_string(blockResults->values.size()) +
                                         " values for " + std::to_string(blockResults->timestamps.size()) +
                                         " timestamps");
    }

    co_return blockResults;
}

template seastar::future<> TSM::readSeries<double>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                   TSMResult<double>& results);
template seastar::future<> TSM::readSeries<bool>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                 TSMResult<bool>& results);
template seastar::future<> TSM::readSeries<std::string>(const SeriesId128& seriesId, uint64_t startTime,
                                                        uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readSeries<int64_t>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                    TSMResult<int64_t>& results);

// Phase 1.1: Template instantiations for readSingleBlock
template seastar::future<std::unique_ptr<TSMBlock<double>>> TSM::readSingleBlock<double>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime, const std::vector<std::string>* stringDict);
template seastar::future<std::unique_ptr<TSMBlock<bool>>> TSM::readSingleBlock<bool>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime, const std::vector<std::string>* stringDict);
template seastar::future<std::unique_ptr<TSMBlock<std::string>>> TSM::readSingleBlock<std::string>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime, const std::vector<std::string>* stringDict);
template seastar::future<std::unique_ptr<TSMBlock<int64_t>>> TSM::readSingleBlock<int64_t>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime, const std::vector<std::string>* stringDict);

// Phase 2: Read compressed block bytes directly without decompression
seastar::future<seastar::temporary_buffer<uint8_t>> TSM::readCompressedBlock(const TSMIndexBlock& indexBlock) {
    // Read the entire block as-is (already compressed)
    // No decompression - just read the raw bytes for zero-copy transfer
    auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
    co_return blockBuf;
}

seastar::future<> TSM::scheduleDelete() {
    // Delete associated tombstone file first (no-op if none exists)
    co_await deleteTombstoneFile();

    // Delete the physical file using async Seastar I/O.
    // Unix unlink is safe while the fd is open: in-flight DMA reads
    // continue via the open fd, inode freed when the last fd closes.
    try {
        co_await seastar::remove_file(filePath);
        timestar::tsm_log.info("TSM file deleted: {}", filePath);
    } catch (const std::exception& e) {
        timestar::tsm_log.error("Failed to delete TSM file {}: {}", filePath, e.what());
    }

    // Do NOT close the fd here.  Queries snapshot the TSM file list (as
    // shared_ptr<TSM>) before issuing DMA reads; a query that snapshotted
    // this file before compaction removed it can still read from it AFTER
    // this point.  Closing the fd here made those reads fail — the query
    // handler swallows the exception and returns an EMPTY result for the
    // series (transient invisibility during compaction/rollover).  Instead,
    // defer the close to the destructor: it runs when the last snapshot
    // reference drops, i.e. when no reader can touch the fd anymore.
    deferCloseOnDestroy_ = true;
    co_return;
}

TSM::~TSM() {
    if (deferCloseOnDestroy_ && tsmFile) {
        // Fire-and-forget close of the (already unlinked) file.  No reader
        // can exist anymore — this object is being destroyed because the
        // last shared_ptr reference dropped.  The lambda keeps the moved
        // file handle alive until close() resolves.
        (void)seastar::futurize_invoke([f = std::move(tsmFile)]() mutable {
            auto fut = f.close();
            return fut.finally([f = std::move(f)]() mutable {});
        }).handle_exception([path = filePath](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                timestar::tsm_log.warn("Deferred close of deleted TSM file {} failed: {}", path, e.what());
            }
        });
    }
}

// Group blocks into contiguous batches for optimized I/O.
// Batches are consecutive runs of the input, so each batch holds a zero-copy
// span into `blocks` instead of copying the 88-byte index structs.
std::vector<BlockBatch> TSM::groupContiguousBlocks(std::span<const TSMIndexBlock> blocks) const {
    std::vector<BlockBatch> batches;
    if (blocks.empty()) {
        return batches;
    }

    // Maximum batch size to avoid excessive memory usage (16MB)
    constexpr uint64_t MAX_BATCH_SIZE = 16 * 1024 * 1024;

    size_t batchStart = 0;
    uint64_t startOffset = blocks[0].offset;
    uint64_t totalSize = blocks[0].size;

    for (size_t i = 1; i < blocks.size(); ++i) {
        uint64_t expectedOffset = startOffset + totalSize;
        uint64_t newBatchSize = totalSize + blocks[i].size;

        // Check if contiguous and within size limit
        if (blocks[i].offset == expectedOffset && newBatchSize <= MAX_BATCH_SIZE) {
            // Contiguous and fits - extend the current batch
            totalSize = newBatchSize;
        } else {
            // Gap detected or size limit - finalize current batch and start new one
            batches.push_back({startOffset, totalSize, blocks.subspan(batchStart, i - batchStart)});
            batchStart = i;
            startOffset = blocks[i].offset;
            totalSize = blocks[i].size;
        }
    }

    // Don't forget the last batch
    batches.push_back({startOffset, totalSize, blocks.subspan(batchStart)});

    timestar::tsm_log.debug("Grouped {} blocks into {} batches", blocks.size(), batches.size());

    return batches;
}

// Extract block decoding logic for reuse
template <class T>
std::unique_ptr<TSMBlock<T>> TSM::decodeBlock(Slice& blockSlice, uint32_t blockSize, uint64_t startTime,
                                              uint64_t endTime, const std::vector<std::string>* stringDict) {
    // Defensive check: ensure we have at least header size
    if (blockSize < BLOCK_HEADER_SIZE) {
        timestar::tsm_log.error("Block size {} is too small for header", blockSize);
        return nullptr;
    }

    // Parse block header (type + timestampSize + timestampBytes)
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    // Validate that the block's stored type matches the template parameter
    TSMValueType expectedType = getValueType<T>();
    if (static_cast<TSMValueType>(blockType) != expectedType) {
        throw std::runtime_error("TSM block type mismatch: block contains type " + std::to_string(blockType) +
                                 " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
    }

    // Defensive check: ensure timestampBytes is reasonable
    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE) {
        timestar::tsm_log.error("Timestamp bytes {} exceeds block size {} - {}", timestampBytes, blockSize,
                                BLOCK_HEADER_SIZE);
        return nullptr;
    }
    // ...and that the decoded COUNT is achievable from that many bytes, before
    // it is used to size the block's two vectors.
    if (!timestampCountIsPlausible(timestampSize, timestampBytes)) {
        timestar::tsm_log.error("Timestamp count {} is impossible for {} compressed bytes", timestampSize,
                                timestampBytes);
        return nullptr;
    }

    // Decode timestamps with time-range filtering
    auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime);

    // Decode values based on type — guard against unsigned underflow before subtraction
    if (timestampBytes + BLOCK_HEADER_SIZE > blockSize) {
        timestar::tsm_log.error("Timestamp bytes {} + header {} exceeds block size {}", timestampBytes,
                                BLOCK_HEADER_SIZE, blockSize);
        return nullptr;
    }
    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;

    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && stringDict && !stringDict->empty()) {
            StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, *stringDict,
                                            blockResults->values);
        } else {
            StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Coroutine-local buffer — thread_local is unsafe in coroutines because
        // another coroutine on the same thread could clear it during a co_await.
        std::vector<uint64_t> rawUintScratch;
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        blockResults->values.reserve(nTimestamps);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            blockResults->values.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
    }

    // Same count contract as decodeBlockFlat(): these per-block decoders build a
    // TSMBlock whose consumers index values by a TIMESTAMP index
    // (TSMBlock::valueAt), so a desynced pair is an out-of-bounds read.
    //
    // Excess values are truncated (benign); a shortfall is raised, because the
    // only alternatives are to mispair real data or to drop the block and report
    // success -- a silent partial answer.
    if (blockResults->values.size() > blockResults->timestamps.size()) {
        blockResults->values.resize(blockResults->timestamps.size());
    } else if (blockResults->values.size() < blockResults->timestamps.size()) {
        throw timestar::BlockDecodeError("TSM block decode short: " + std::to_string(blockResults->values.size()) +
                                         " values for " + std::to_string(blockResults->timestamps.size()) +
                                         " timestamps");
    }

    return blockResults;
}

// Decode a block directly into flat output vectors (no TSMBlock heap allocation).
// Returns the number of points decoded.
template <class T>
static size_t decodeBlockFlat(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                              std::vector<uint64_t>& outTimestamps, std::vector<T>& outValues,
                              const std::vector<std::string>* stringDict = nullptr) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;
    if (!timestampCountIsPlausible(timestampSize, timestampBytes))
        return 0;

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    const size_t timestampsBefore = outTimestamps.size();
    const size_t valuesBefore = outValues.size();
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, outTimestamps, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    if (timestampBytes + BLOCK_HEADER_SIZE > blockSize)
        return 0;
    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;

    size_t produced = 0;
    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        produced = FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, outValues);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        produced = BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, outValues);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && stringDict && !stringDict->empty()) {
            produced = StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, *stringDict,
                                                       outValues);
        } else {
            produced = StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, outValues);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Coroutine-local buffer — thread_local is unsafe in coroutines because
        // another coroutine on the same thread could clear it during a co_await.
        std::vector<uint64_t> rawUintScratch;
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            outValues.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
        produced = (end > nSkipped) ? (end - nSkipped) : 0;
    }

    // ---- THE COUNT CONTRACT, enforced once for every value type ----
    //
    // `expected` is what the block says it holds for this read: the timestamps
    // that survived the time filter. Each decoder reports what it really
    // produced (phase 1), so the three previously-divergent behaviours -- ALP
    // trimmed, bool threw, string/int silently under-produced -- now converge
    // here.
    //
    //   produced > expected : benign. A decoder working in fixed-size groups can
    //                         overshoot the tail; truncate and carry on.
    //   produced < expected : the block is corrupt or a decoder regressed. There
    //                         is no safe repair -- pairing values[i] with
    //                         timestamps[i] past the shortfall MISPAIRS real
    //                         data, presenting a wrong point as a valid one.
    const size_t expected = outTimestamps.size() - timestampsBefore;

    // FIRST: the decoder must have APPENDED. Both vectors accumulate across every
    // block of a series, so a decoder that clears or otherwise shrinks the output
    // destroys earlier blocks' values while their timestamps remain.
    //
    // This check is what actually catches that, and trusting the decoder's own
    // produced-count does NOT: the clobbering block reports produced == expected
    // for ITS OWN points (1 value for 1 timestamp) and looks perfectly healthy,
    // while 3000 previously-decoded values have silently vanished. Measuring the
    // real growth of the buffer is the only view that sees it.
    if (outValues.size() < valuesBefore) {
        const size_t lost = valuesBefore - outValues.size();
        outTimestamps.resize(timestampsBefore);
        throw timestar::BlockDecodeError("value decoder shrank the output by " + std::to_string(lost) +
                                         " values -- decoders must APPEND, never clear (block declares " +
                                         std::to_string(timestampSize) + " points)");
    }
    const size_t appended = outValues.size() - valuesBefore;

    if (appended > expected) {
        outValues.resize(valuesBefore + expected);
    } else if (appended < expected) {
        // Roll the timestamps back so no caller can observe a desynced pair even
        // if this exception is caught and the buffers reused.
        outTimestamps.resize(timestampsBefore);
        outValues.resize(valuesBefore);
        throw timestar::BlockDecodeError("TSM block decode short: appended " + std::to_string(appended) +
                                         " values (decoder reported " + std::to_string(produced) + ") for " +
                                         std::to_string(expected) + " timestamps (block declares " +
                                         std::to_string(timestampSize) + " points)");
    }

    // Defence for the other direction: the timestamp decoder must never emit
    // more than the block declares. Both FFOR paths clamp to `timestampSize`,
    // but that is enforced inside the decoder -- this is the check at the
    // consumer, and it is the ONLY thing standing between a future timestamp
    // over-read and silently fabricated points.
    if (expected > timestampSize) {
        outTimestamps.resize(timestampsBefore);
        outValues.resize(valuesBefore);
        throw timestar::BlockDecodeError("TSM block decoded " + std::to_string(expected) +
                                         " timestamps but declares only " + std::to_string(timestampSize));
    }

    return nTimestamps;
}

// Read multiple contiguous blocks with a single I/O operation.
// Decodes all blocks in the batch directly into flat timestamp/value vectors
// stored in a single TSMBlock, eliminating per-block heap allocations.
template <class T>
seastar::future<> TSM::readBlockBatch(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                      TSMResult<T>& results, const std::vector<std::string>* stringDict) {
    // Single large read for the entire batch, via the read elevator so that
    // batches from OTHER series running in the same reactor tick merge into
    // shared large reads (series regions sit back to back in the file).
    auto batchBuf = co_await coalescedDmaRead(batch.startOffset, batch.totalSize);

    // Decode all blocks in this batch into a single flat TSMBlock.
    // Blocks within a batch are contiguous and sorted by offset (= time order),
    // so appending sequentially produces sorted output.
    auto flatBlock = std::make_unique<TSMBlock<T>>(0);

    uint32_t bufferOffset = 0;
    for (const auto& block : batch.blocks) {
        // Fully-contained blocks (index bounds prove every point passes the time
        // filter) decode with sentinel bounds — hits the branch-free timestamp
        // decode fast path with identical results.
        const bool fullyContained = block.minTime >= startTime && block.maxTime < endTime;
        decodeBlockFlat<T>(batchBuf.get() + bufferOffset, block.size, fullyContained ? 0 : startTime,
                           fullyContained ? UINT64_MAX : endTime, flatBlock->timestamps, flatBlock->values, stringDict);
        bufferOffset += block.size;
    }

    if (!flatBlock->timestamps.empty()) {
        results.appendBlock(std::move(flatBlock));
    }

    co_return;
}

// Optimized series read using block batching
template <class T>
seastar::future<> TSM::readSeriesBatched(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                         TSMResult<T>& results) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return;  // Series not in this file
    }

    // Take a refcount on the shared dictionary so it survives LRU cache
    // evictions across co_await DMA suspensions (use-after-free fix) — no
    // deep copy of the strings.
    [[maybe_unused]] std::shared_ptr<const std::vector<std::string>> localDictRef;
    [[maybe_unused]] const std::vector<std::string>* localStringDict = nullptr;
    if constexpr (std::is_same_v<T, std::string>) {
        if (indexEntry->stringDictionary && !indexEntry->stringDictionary->empty()) {
            localDictRef = indexEntry->stringDictionary;
            localStringDict = localDictRef.get();
        }
    }

    // Step 1: Filter blocks by time range (contiguous range via binary search),
    // copied into a frame-local vector because the cache entry can be evicted
    // across the co_await suspensions below.
    auto overlapRange = overlappingBlockRange(indexEntry->indexBlocks, startTime, endTime);
    std::vector<TSMIndexBlock> blocksToScan(overlapRange.begin(), overlapRange.end());

    if (blocksToScan.empty()) {
        co_return;
    }

    // Step 2: Blocks should already be sorted by offset from index, but verify
    // (TSM writer ensures blocks are written in order)

    // Step 3: Group into contiguous batches
    auto batches = groupContiguousBlocks(blocksToScan);

    // Log batching efficiency for performance monitoring
    size_t totalBlocks = blocksToScan.size();
    size_t totalBatches = batches.size();
    if (totalBlocks > 1) {
        double batchEfficiency = (double)totalBlocks / totalBatches;
        timestar::tsm_log.debug("Batch read efficiency: {:.1f} blocks/batch ({} blocks → {} batches)", batchEfficiency,
                                totalBlocks, totalBatches);
    }

    // Step 4: Single-batch fast path (the common case — blocks are contiguous).
    // Avoids parallel_for_each overhead and per-batch TSMResult intermediaries.
    if (batches.size() == 1) {
        co_await readBlockBatch(batches[0], startTime, endTime, results, localStringDict);
    } else {
        // Multiple batches: read in parallel with per-batch slot isolation.
        std::vector<TSMResult<T>> batchResults;
        batchResults.reserve(batches.size());
        for (size_t i = 0; i < batches.size(); ++i) {
            batchResults.emplace_back(0);
        }
        size_t batchSlotIdx = 0;

        co_await seastar::parallel_for_each(batches, [&](const BlockBatch& batch) -> seastar::future<> {
            size_t mySlot = batchSlotIdx++;
            co_await readBlockBatch(batch, startTime, endTime, batchResults[mySlot], localStringDict);
        });

        for (auto& batchResult : batchResults) {
            for (auto& block : batchResult.blocks) {
                results.appendBlock(std::move(block));
            }
        }
    }

    // parallel_for_each does not guarantee ordering, so sort blocks by start time
    results.sort();

    co_return;
}

// Pushdown aggregation: decode float blocks and fold directly into a
// BlockAggregator, bypassing TSMResult/QueryResult materialisation.
// Handles tombstone filtering inline.  Returns the number of points folded.
// Shared value-type dispatch for the aggregation paths (see tsm.hpp).
// Synchronous flavour: decode from an in-memory slice, no suspension.
template <typename Fold>
void TSM::decodeBlockAndFold(Slice& blockSlice, TSMValueType seriesType, uint32_t blockSize, uint64_t startTime,
                             uint64_t endTime, Fold fold) {
    if (seriesType == TSMValueType::Float) {
        auto blockResult = decodeBlock<double>(blockSlice, blockSize, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return vals[j]; });
        }
    } else if (seriesType == TSMValueType::Integer) {
        auto blockResult = decodeBlock<int64_t>(blockSlice, blockSize, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return static_cast<double>(vals[j]); });
        }
    } else {
        // Boolean
        auto blockResult = decodeBlock<bool>(blockSlice, blockSize, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return vals[j] ? 1.0 : 0.0; });
        }
    }
}

// Async flavour: per-block DMA read + decode via readSingleBlock.
template <typename Fold>
seastar::future<> TSM::readBlockAndFold(const TSMIndexBlock& block, TSMValueType seriesType, uint64_t startTime,
                                        uint64_t endTime, Fold fold) {
    if (seriesType == TSMValueType::Float) {
        auto blockResult = co_await readSingleBlock<double>(block, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return vals[j]; });
        }
    } else if (seriesType == TSMValueType::Integer) {
        auto blockResult = co_await readSingleBlock<int64_t>(block, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return static_cast<double>(vals[j]); });
        }
    } else {
        // Boolean
        auto blockResult = co_await readSingleBlock<bool>(block, startTime, endTime);
        if (blockResult && !blockResult->timestamps.empty()) {
            const auto& vals = blockResult->values;
            fold(blockResult->timestamps, [&vals](size_t j) { return vals[j] ? 1.0 : 0.0; });
        }
    }
}

seastar::future<size_t> TSM::aggregateSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                             timestar::BlockAggregator& aggregator, seastar::semaphore* ioSem) {
    try {
        co_return co_await aggregateSeriesImpl(seriesId, startTime, endTime, aggregator, ioSem);
    } catch (...) {
        rethrowWithFilePath();
    }
}

seastar::future<size_t> TSM::aggregateSeriesImpl(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                 timestar::BlockAggregator& aggregator, seastar::semaphore* ioSem) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return 0;
    }

    // Float, Integer, and Boolean support pushdown aggregation; String does not
    if (indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range (contiguous range via binary search).
    // The span into the cache entry is only read synchronously below (stats
    // pre-scan runs before any co_await); blocks that still need decoding are
    // copied into the frame-local decodeBlocks vector before suspension.
    auto blocksToScan = overlappingBlockRange(indexEntry->indexBlocks, startTime, endTime);
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Pre-fetch tombstone ranges once (empty vector if no tombstones)
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }

    size_t totalPoints = 0;

    // Stats pre-scan BEFORE batching: blocks answered by block stats never
    // reach the batch builder, so their bytes are never DMA-read. (Batching
    // first and skipping at decode time would still read the whole contiguous
    // batch — with O_DIRECT files that is real disk I/O, up to ~1000x
    // amplification when only boundary blocks of a range need decoding.)
    // This pre-scan is synchronous, so the aggregator/totalPoints mutations
    // stay outside the parallel_for_each lambdas below.
    const bool countOnly = aggregator.isCountOnly();

    std::vector<TSMIndexBlock> decodeBlocks;
    decodeBlocks.reserve(blocksToScan.size());
    for (const auto& block : blocksToScan) {
        bool blockFullyContained = (block.minTime >= startTime && block.maxTime <= endTime);
        bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
        bool canSkip = !blockHasTombstones && blockFullyContained && block.blockCount > 0 &&
                       aggregator.canUseBlockStats(block.minTime, block.maxTime, block.hasExtendedStats);
        if (canSkip) {
            aggregator.addBlockStats(block.blockSum, block.blockMin, block.blockMax, block.blockCount, block.minTime,
                                     block.maxTime, block.blockM2, block.blockFirstValue, block.blockLatestValue);
            totalPoints += block.blockCount;
        } else {
            decodeBlocks.push_back(block);
        }
    }

    if (decodeBlocks.empty()) {
        co_return totalPoints;
    }

    // Group the blocks that still need decoding into contiguous batches for
    // efficient I/O. Stats-skipped interior blocks become gaps, so boundary
    // blocks get their own small reads instead of dragging the whole range in.
    auto batches = groupContiguousBlocks(decodeBlocks);

    std::vector<size_t> batchIndices;
    batchIndices.reserve(batches.size());
    for (size_t i = 0; i < batches.size(); ++i) {
        batchIndices.push_back(i);
    }

    // ──────────────────────────────────────────────────────────────────────
    // IMPORTANT — SHARED MUTABLE STATE CONTRACT
    //
    // The lambdas below share mutable references to `aggregator` and
    // `totalPoints` WITHOUT synchronisation.  This is safe ONLY because
    // Seastar's cooperative scheduler guarantees that after the last
    // co_await (the DMA read), all subsequent code in each lambda runs to
    // completion without yielding.
    //
    // DO NOT add any co_await, seastar::maybe_yield(), or other
    // suspension points after the "Release I/O semaphore" line inside
    // the lambda.  Doing so would allow another lambda to run
    // concurrently and corrupt the shared state.
    //
    // If you need async work during the decode/fold section, you MUST
    // first collect results into per-batch local variables and merge them
    // into the shared state after the parallel_for_each completes.
    // ──────────────────────────────────────────────────────────────────────
    co_await seastar::parallel_for_each(batchIndices, [&](size_t batchIdx) -> seastar::future<> {
        const auto& batch = batches[batchIdx];

        // Acquire I/O semaphore before DMA read to bound concurrent disk I/O.
        // Released after the read completes and decode/fold finishes (no DMA
        // during decode, so the unit is held slightly longer than strictly
        // necessary, but this keeps the code simple and prevents back-to-back
        // DMA bursts).
        std::optional<seastar::semaphore_units<>> ioUnits;
        if (ioSem) {
            ioUnits = co_await seastar::get_units(*ioSem, 1);
        }

        // Single read for the entire batch (elevator: merges with concurrent
        // series' batch reads in the same dispatch round)
        auto batchBuf = co_await coalescedDmaRead(batch.startOffset, batch.totalSize);

        // Release I/O semaphore after DMA completes — decode is CPU-only
        ioUnits.reset();

        // WARNING: No co_await/maybe_yield allowed below this point —
        // mutable shared state (aggregator, totalPoints) is accessed.
        // Safe only because decode is synchronous in Seastar's cooperative
        // model.  See the contract comment above parallel_for_each.
        uint32_t bufferOffset = 0;
        for (size_t bi = 0; bi < batch.blocks.size(); ++bi) {
            const auto& block = batch.blocks[bi];
            // Every block in the batch needs decoding (stats-answered blocks
            // were removed before batching and their bytes were never read).

            // Phase 0: per-block tombstone check instead of global gate
            bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
            if (!blockHasTombstones) {
                // Fast path: no tombstones for this block — decode into scratch buffers and fold.
                // Fully-contained blocks (index bounds prove every point passes the time
                // filter) use sentinel bounds to hit the branch-free timestamp decode
                // fast path — result-identical, but skips 2 compares per point.
                const bool fullyContained = block.minTime >= startTime && block.maxTime <= endTime;
                const uint64_t decodeStart = fullyContained ? 0 : startTime;
                const uint64_t decodeEnd = fullyContained ? UINT64_MAX : endTime;
                size_t n;
                if (countOnly) {
                    n = decodeBlockCountOnly(batchBuf.get() + bufferOffset, block.size, decodeStart, decodeEnd,
                                             aggregator, seriesType, block.blockCount);
                } else if (seriesType == TSMValueType::Float) {
                    n = decodeBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, decodeStart, decodeEnd,
                                                  aggregator);
                } else if (seriesType == TSMValueType::Integer) {
                    n = decodeIntegerBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, decodeStart,
                                                         decodeEnd, aggregator);
                } else {
                    // Boolean
                    n = decodeBoolBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, decodeStart, decodeEnd,
                                                      aggregator);
                }
                totalPoints += n;
            } else {
                // Tombstone path: need per-point filtering, use full decode.
                // The fold is synchronous (no co_await) — the shared-state
                // contract above still holds.
                Slice blockSlice(batchBuf.get() + bufferOffset, block.size);
                decodeBlockAndFold(blockSlice, seriesType, block.size, startTime, endTime,
                                   [&](const std::vector<uint64_t>& ts, auto getValue) {
                                       // Decoded timestamps are ascending and tombstoneRanges are
                                       // sorted + non-overlapping, so a monotonic cursor replaces
                                       // the per-point std::upper_bound: O(N + T) instead of
                                       // O(N log T).
                                       size_t ri = 0;
                                       const size_t nr = tombstoneRanges.size();
                                       for (size_t i = 0; i < ts.size(); ++i) {
                                           uint64_t t = ts[i];
                                           while (ri < nr && tombstoneRanges[ri].second < t)
                                               ++ri;
                                           if (ri < nr && t >= tombstoneRanges[ri].first)
                                               continue;  // tombstoned
                                           aggregator.addPoint(t, getValue(i));
                                           totalPoints++;
                                       }
                                   });
            }

            bufferOffset += block.size;
        }
    });

    co_return totalPoints;
}

seastar::future<size_t> TSM::aggregateSeriesSelective(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                      timestar::BlockAggregator& aggregator, bool reverse,
                                                      size_t maxPoints) {
    try {
        co_return co_await aggregateSeriesSelectiveImpl(seriesId, startTime, endTime, aggregator, reverse, maxPoints);
    } catch (...) {
        rethrowWithFilePath();
    }
}

seastar::future<size_t> TSM::aggregateSeriesSelectiveImpl(const SeriesId128& seriesId, uint64_t startTime,
                                                          uint64_t endTime, timestar::BlockAggregator& aggregator,
                                                          bool reverse, size_t maxPoints) {
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry || indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range (contiguous range via binary search), copied
    // into a frame-local vector because the cache entry can be evicted across
    // co_await suspensions below.
    auto overlapRange = overlappingBlockRange(indexEntry->indexBlocks, startTime, endTime);
    std::vector<TSMIndexBlock> blocksToScan(overlapRange.begin(), overlapRange.end());
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Fast path: for LATEST/FIRST with maxPoints=1 and extended stats,
    // use the block-level latestValue/firstValue directly from the index entry.
    // This avoids reading any data blocks from disk — pure metadata lookup.
    // indexBlocks are sorted by minTime ascending in the writer, and blocksToScan
    // preserves that order, so LATEST → back(), FIRST → front().
    if (maxPoints == 1) {
        const TSMIndexBlock& bestBlock = reverse ? blocksToScan.back() : blocksToScan.front();
        const bool boundaryInRange = reverse ? (bestBlock.maxTime <= endTime) : (bestBlock.minTime >= startTime);
        if (bestBlock.hasExtendedStats && boundaryInRange) {
            const uint64_t boundaryTs = reverse ? bestBlock.maxTime : bestBlock.minTime;
            // hasTombstones() is a cheap file-level guard; only consult per-series
            // ranges when the file has any tombstones at all. The boundary value is
            // only invalid if the exact boundary timestamp falls inside a tombstone.
            bool boundaryDeleted = false;
            if (hasTombstones()) {
                auto ranges = tombstones->getTombstoneRanges(seriesId);
                for (const auto& r : ranges) {
                    if (r.first > boundaryTs)
                        break;  // ranges sorted by start
                    if (boundaryTs <= r.second) {
                        boundaryDeleted = true;
                        break;
                    }
                }
            }
            if (!boundaryDeleted) {
                const double boundaryVal = reverse ? bestBlock.blockLatestValue : bestBlock.blockFirstValue;
                aggregator.addPoint(boundaryTs, boundaryVal);
                co_return 1;
            }
        }
    }

    // Pre-fetch tombstone ranges once
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }
    const bool hasTombstoneRanges = !tombstoneRanges.empty();

    size_t totalPoints = 0;
    size_t count = blocksToScan.size();

    // Direction-aware monotonic tombstone cursor.  Points are visited in
    // strictly monotonic time order across blocks (ascending forward,
    // descending in reverse) and tombstoneRanges are sorted by start and
    // non-overlapping, so the cursor advances O(N + T) total instead of a
    // per-point std::upper_bound (O(N log T)).
    size_t riFwd = 0;
    ptrdiff_t riRev = static_cast<ptrdiff_t>(tombstoneRanges.size()) - 1;
    auto isTombstoned = [&](uint64_t t) {
        if (!reverse) {
            const size_t nr = tombstoneRanges.size();
            while (riFwd < nr && tombstoneRanges[riFwd].second < t)
                ++riFwd;
            return riFwd < nr && t >= tombstoneRanges[riFwd].first;
        }
        while (riRev >= 0 && tombstoneRanges[riRev].first > t)
            --riRev;
        return riRev >= 0 && t <= tombstoneRanges[riRev].second;
    };

    // Helper lambda: iterate decoded points, optionally filtering tombstones, in fwd/rev order.
    auto processPoints = [&](const std::vector<uint64_t>& ts, auto getValue) {
        size_t pointCount = ts.size();
        for (size_t pi = 0; pi < pointCount && totalPoints < maxPoints; ++pi) {
            size_t j = reverse ? (pointCount - 1 - pi) : pi;
            uint64_t t = ts[j];
            if (hasTombstoneRanges && isTombstoned(t))
                continue;
            aggregator.addPoint(t, getValue(j));
            totalPoints++;
        }
    };

    for (size_t i = 0; i < count && totalPoints < maxPoints; ++i) {
        size_t idx = reverse ? (count - 1 - i) : i;
        const auto& block = blocksToScan[idx];
        co_await readBlockAndFold(block, seriesType, startTime, endTime, processPoints);
    }

    co_return totalPoints;
}

seastar::future<size_t> TSM::aggregateSeriesBucketed(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                     timestar::BlockAggregator& aggregator, bool reverse,
                                                     uint64_t interval, std::unordered_set<uint64_t>& filledBuckets,
                                                     size_t totalBuckets) {
    try {
        co_return co_await aggregateSeriesBucketedImpl(seriesId, startTime, endTime, aggregator, reverse, interval,
                                                       filledBuckets, totalBuckets);
    } catch (...) {
        rethrowWithFilePath();
    }
}

seastar::future<size_t> TSM::aggregateSeriesBucketedImpl(const SeriesId128& seriesId, uint64_t startTime,
                                                         uint64_t endTime, timestar::BlockAggregator& aggregator,
                                                         bool reverse, uint64_t interval,
                                                         std::unordered_set<uint64_t>& filledBuckets,
                                                         size_t totalBuckets) {
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry || indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range (contiguous range via binary search), copied
    // into a frame-local vector because the cache entry can be evicted across
    // co_await suspensions below.
    auto overlapRange = overlappingBlockRange(indexEntry->indexBlocks, startTime, endTime);
    std::vector<TSMIndexBlock> blocksToScan(overlapRange.begin(), overlapRange.end());
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Pre-fetch tombstone ranges once
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }
    const bool hasTombstoneRanges = !tombstoneRanges.empty();

    size_t totalPoints = 0;
    size_t count = blocksToScan.size();

    for (size_t i = 0; i < count; ++i) {
        // Early termination: all buckets filled
        if (filledBuckets.size() >= totalBuckets) {
            break;
        }

        size_t idx = reverse ? (count - 1 - i) : i;
        const auto& block = blocksToScan[idx];

        // Check if all buckets spanned by this block are already filled.
        // Use the same bucket formula as BlockAggregator: (t / interval) * interval
        uint64_t blockStart = std::max(block.minTime, startTime);
        uint64_t blockEnd = std::min(block.maxTime, endTime);
        uint64_t firstBucket = (blockStart / interval) * interval;
        uint64_t lastBucket = (blockEnd / interval) * interval;

        bool allBlockBucketsFilled = true;
        for (uint64_t b = firstBucket; b <= lastBucket; b += interval) {
            if (filledBuckets.find(b) == filledBuckets.end()) {
                allBlockBucketsFilled = false;
                break;
            }
        }
        if (allBlockBucketsFilled) {
            continue;  // Skip this block entirely
        }

        // Extended-stats shortcut: when block has extended stats and fits in one
        // bucket, extract the first/latest endpoint directly without DMA
        // read + decode.  This preserves 1-point-per-bucket semantics.
        // Phase 0: per-block tombstone check instead of global gate
        bool blockFullyContained = (block.minTime >= startTime && block.maxTime <= endTime);
        bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
        if (!blockHasTombstones && blockFullyContained && block.hasExtendedStats) {
            uint64_t bFirst = (block.minTime / interval) * interval;
            uint64_t bLast = (block.maxTime / interval) * interval;
            if (bFirst == bLast) {
                // Single-bucket block: use the relevant endpoint
                if (filledBuckets.find(bFirst) == filledBuckets.end()) {
                    if (reverse) {
                        aggregator.addPoint(block.maxTime, block.blockLatestValue);
                    } else {
                        aggregator.addPoint(block.minTime, block.blockFirstValue);
                    }
                    filledBuckets.insert(bFirst);
                    totalPoints++;
                }
                continue;
            }
        }

        // Helper lambda for bucketed point processing with tombstone filtering.
        // Timestamps are monotonic within a block, so once a bucket is resolved
        // (filled here, or found already filled), its remaining points are
        // skipped with two comparisons instead of a hash lookup per point.
        // The tombstone binary search runs only for candidate points (first
        // unresolved point of a bucket), not for every decoded point.
        auto processBucketedPoints = [&](const std::vector<uint64_t>& ts, auto getValue) {
            size_t pointCount = ts.size();
            uint64_t skipLo = 1, skipHi = 0;  // empty interval: [skipLo, skipHi)
            for (size_t pi = 0; pi < pointCount; ++pi) {
                size_t j = reverse ? (pointCount - 1 - pi) : pi;
                uint64_t t = ts[j];
                if (t >= skipLo && t < skipHi)
                    continue;  // same bucket as the last resolved one
                uint64_t bucketKey = (t / interval) * interval;
                if (filledBuckets.find(bucketKey) != filledBuckets.end()) {
                    skipLo = bucketKey;
                    skipHi = bucketKey + interval;
                    continue;
                }
                if (hasTombstoneRanges) {
                    auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                    std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                    if (rangeIt != tombstoneRanges.begin()) {
                        --rangeIt;
                        if (t >= rangeIt->first && t <= rangeIt->second)
                            continue;  // tombstoned candidate: try next point in bucket
                    }
                }
                aggregator.addPoint(t, getValue(j));
                filledBuckets.insert(bucketKey);
                skipLo = bucketKey;
                skipHi = bucketKey + interval;
                totalPoints++;
            }
        };

        co_await readBlockAndFold(block, seriesType, startTime, endTime, processBucketedPoints);
    }

    co_return totalPoints;
}

// Template instantiations for batched reads
template seastar::future<> TSM::readSeriesBatched<double>(const SeriesId128& seriesId, uint64_t startTime,
                                                          uint64_t endTime, TSMResult<double>& results);
template seastar::future<> TSM::readSeriesBatched<bool>(const SeriesId128& seriesId, uint64_t startTime,
                                                        uint64_t endTime, TSMResult<bool>& results);
template seastar::future<> TSM::readSeriesBatched<std::string>(const SeriesId128& seriesId, uint64_t startTime,
                                                               uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readSeriesBatched<int64_t>(const SeriesId128& seriesId, uint64_t startTime,
                                                           uint64_t endTime, TSMResult<int64_t>& results);

template seastar::future<> TSM::readBlockBatch<double>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                       TSMResult<double>& results, const std::vector<std::string>*);
template seastar::future<> TSM::readBlockBatch<bool>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                     TSMResult<bool>& results, const std::vector<std::string>*);
template seastar::future<> TSM::readBlockBatch<std::string>(const BlockBatch& batch, uint64_t startTime,
                                                            uint64_t endTime, TSMResult<std::string>& results,
                                                            const std::vector<std::string>*);
template seastar::future<> TSM::readBlockBatch<int64_t>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                        TSMResult<int64_t>& results, const std::vector<std::string>*);

template std::unique_ptr<TSMBlock<double>> TSM::decodeBlock<double>(Slice& blockSlice, uint32_t blockSize,
                                                                    uint64_t startTime, uint64_t endTime,
                                                                    const std::vector<std::string>*);
template std::unique_ptr<TSMBlock<bool>> TSM::decodeBlock<bool>(Slice& blockSlice, uint32_t blockSize,
                                                                uint64_t startTime, uint64_t endTime,
                                                                const std::vector<std::string>*);
template std::unique_ptr<TSMBlock<std::string>> TSM::decodeBlock<std::string>(Slice& blockSlice, uint32_t blockSize,
                                                                              uint64_t startTime, uint64_t endTime,
                                                                              const std::vector<std::string>*);
template std::unique_ptr<TSMBlock<int64_t>> TSM::decodeBlock<int64_t>(Slice& blockSlice, uint32_t blockSize,
                                                                      uint64_t startTime, uint64_t endTime,
                                                                      const std::vector<std::string>*);
