#!/usr/bin/env python3
import json, sys, glob, statistics
tag = sys.argv[1]; S = '/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad/bench/res'
B = [json.load(open(f)) for f in sorted(glob.glob(f'{S}/{tag}_base_*.json'))]; N = [json.load(open(f)) for f in sorted(glob.glob(f'{S}/{tag}_new_*.json'))]
print(f"{'query':46} {'points':>8} | server ms before -> after (x)      | client ms before -> after (x)   same result")
for q in B[0]:
    sb = statistics.median(r[q]['server_ms'] for r in B); sn = statistics.median(r[q]['server_ms'] for r in N)
    cb = statistics.median(r[q]['client_ms'] for r in B); cn = statistics.median(r[q]['client_ms'] for r in N)
    pr = sorted(b[q]['server_ms'] / n[q]['server_ms'] for b, n in zip(B, N))
    same = len({r[q]['digest'] for r in B + N}) == 1
    print(f"{q:46} {B[0][q]['points']:>8} | {sb:8.3f} -> {sn:8.3f} ({sb/sn:5.2f}x, pairs {pr[0]:.2f}-{pr[-1]:.2f}) | {cb:8.3f} -> {cn:8.3f} ({cb/cn:5.2f}x)  {same}")
