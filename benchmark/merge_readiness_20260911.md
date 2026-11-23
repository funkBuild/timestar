**Optimization handoff — 11 September 2026**

Work stops after the thirteenth round at the user's request. The cumulative insert/query changes remain uncommitted on `release/1.5.0-rc.1`, based on `71b1ae9`. No merge or publication has been performed.

The latest round removes full-block unsigned scratch from integer reads and uses SIMD reductions for skipped prefixes. Measured direct TSM reads improve about 1.5× for full blocks and up to 2.9× for late ranges. Integer HTTP bucketed AVG latency improves 12–17%, and middle-range SUM improves about 9%. See the [thirteenth-pass report](query_pass13_20260911.md) for source changes, benchmark methodology, raw results and limits. Earlier rounds are documented in the other dated pass reports, starting with the [initial optimization report](performance_optimizations_20260911.md).

**Final local validation — passed**

- Release HTTP server and unit-test build passes after formatting cleanup, with GCC 14, native architecture and warnings-as-errors enabled. Compiler diagnostics in Seastar headers remain recorded in the build log.
- The repository-wide `lib`/`bin` formatting gate passes with the CI-pinned clang-format 21.1.6. Six modified files required formatting cleanup.
- The semantic implementation passes 3,947 single-shard release tests with ten existing skips; 109 focused four-shard ASan/UBSan correctness tests pass, and the sanitizer server verifies 3,200 float/integer TSM query responses.
- Thirty wire fixtures roundtrip and produce identical encoded bytes in baseline and candidate.
- The final two-shard `CI=true` suite uses 8 GiB total memory (4 GiB per shard). An initial 4 GiB total run passed 3,955 tests but exhausted memory in the large restore fixture; that fixture passes with the corrected allowance and unchanged code. **Final result: 3,956 passed, one software-CRC skip, zero failures (96.164 seconds).** [Full log](query_pass13_20260911/results/merge-tests-final.log) and [JUnit XML](query_pass13_20260911/results/merge-tests-final.xml) are retained.

[Validation logs](query_pass13_20260911/results/validation.txt), [measured-build hashes](query_pass13_20260911/results/artifacts.json), and [final formatted-build hashes](query_pass13_20260911/results/merge-artifacts.json) distinguish the benchmark artifacts from the rebuilt handoff. The integer decoder disassembly is identical before and after formatting. The local build uses native CPU instructions; a fresh generic-architecture Docker CI build and coverage job were not run here.

**Review boundaries**

The work makes measured progress; it does not establish that every insert/query path is globally compute- and memory-optimal. Reports retain small-workload regressions and allocation tradeoffs. Outstanding investigations include the earlier high-offset statistics accuracy limitation, a previously observed allocator crash that was not reproduced or explained, and sustained-compaction behavior. No new optimization work is started after the stop request.

Benchmark binaries and baseline libraries remain in ignored build directories. The `external/seastar` dirty indicator comes from generated, untracked build files; there are no tracked submodule edits. Benchmark reports and their raw result directories are part of the uncommitted handoff.
