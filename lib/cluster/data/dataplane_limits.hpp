#pragma once

#include <cstddef>

namespace timestar::data {

// THE DATA-PLANE FRAME BOUNDS.
//
// These were file-local constants in dataplane_rpc.cpp until D-31. They are in a header
// now for one reason: `kMaxOutboundFrameBytes` is the largest write slice this cluster
// will put on the wire, and therefore the external justification for the Raft PROPOSAL
// bound (`RaftGroup::kMaxProposalBytes`, raft/raft_types.hpp) -- a relationship no file
// could see while the two numbers lived in a .cpp and a header that do not include each
// other. The assertion tying them together is in integration/cluster_data_plane.hpp,
// which is the one place that includes both (the D-20 pattern).
//
// Inbound frame admission (rpc::resource_limits). Without these, seastar's default is
// rpc_semaphore::max_counter() -- effectively unbounded -- so a peer could hold
// arbitrarily much of this node's memory in flight. That is the only bound available
// against per-SERIES decode amplification, which no decoder change can fix: an empty
// WriteSeries costs 13 bytes on the wire (type + a zero keyLen + a zero count + a zero
// revCount) and 144 resident, so a legal, checksum-valid 16 MiB frame of ~1.29M empty
// series decodes to ~458 MB and is RETAINED (it is handed to applyWrites). 13 bytes per
// series is the format's structural floor; the frame size is the thing to bound.
//
// kInboundBloatFactor is seastar's per-request "resident bytes per wire byte" estimate
// (rpc::estimate_request_size), set to that worst-case ratio (144/13 = 11.1, rounded
// up). It makes the semaphore account for what a frame will really COST rather than
// what it weighs, so:
//   - a frame whose estimate exceeds the budget is refused BEFORE the handler runs,
//     with an exceptional reply on a connection that stays up (rpc_impl.hh) -- for
//     these waited verbs that is a clean, retryable write failure;
//   - everything admitted queues behind the budget, so total resident decode stays
//     within it instead of scaling with the number of peers.
// The budget over the bloat factor is the effective per-frame ceiling: ~10.9 MiB of
// wire. The largest legitimate frame is far smaller -- a whole 10k-point HTTP batch
// encodes ~1-2 MB and what actually crosses the wire is a per-VShard SLICE of one --
// so this is several times the real maximum while still refusing the amplifying shape.
inline constexpr size_t kMaxInboundRpcMemory = size_t{128} << 20;  // 128 MiB of estimated in-flight
inline constexpr unsigned kInboundBloatFactor = 12;

// The largest frame this node will SEND on the data plane: the inbound admission ceiling
// above (max_memory / bloat_factor ~= 10.67 MiB), which every peer in a homogeneous
// cluster shares. Checking it CLIENT-side turns "the peer refused an oversized frame" --
// which arrives as an opaque remote error, gets retried pointlessly against every other
// leader, and is finally reported as an internal 500 -- into a local, terminal failure
// naming the actual size, which the HTTP layer maps to 413 (write-scaleout 3d).
//
// A frame carries a whole VIEW of per-VShard slices; a Raft entry carries ONE of them. So
// this bounds the WIRE and `firstUnproposableSlice` (replicated_command.hpp) bounds the
// ENTRY, and neither implies the other -- see that declaration (debt D-31).
inline constexpr size_t kMaxOutboundFrameBytes = kMaxInboundRpcMemory / kInboundBloatFactor;

}  // namespace timestar::data
