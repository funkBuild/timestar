# Architecture Decision Records

Design decisions for the TimeStar cluster implementation. Each ADR records a
decision the [cluster plan](../clustering.md) mandated or left open, with its
rationale, alternatives, and consequences.

The plan gates **Phase 1 / Task 4** (the VShard storage boundary) on three ADRs
landing first. Those three are:

- [ADR 0001 — Journal segmentation and retention](0001-journal-segmentation-and-retention.md)
  (resolves open decision 2)
- [ADR 0002 — VShard-partitioned physical TSM and NativeIndex layout](0002-vshard-physical-layout.md)
  (resolves open decision 3 and the Task-4-critical part of decision 5)
- [ADR 0003 — Pre-Raft point-revision assignment and Raft-index compatibility](0003-point-revision-assignment.md)
  (resolves the point-revision framing of open decision 1)
