#!/usr/bin/env python3
"""memq_report.py <tag> <pairs>: medians of first/steady server ms, base vs new, and whether results agree numerically."""
import json, os, sys, statistics
S=os.environ['SCRATCH']+'/bench/res/'
tag=sys.argv[1]; pairs=int(sys.argv[2])
runs={v:[json.load(open(f'{S}{tag}_{v}_{i}.json')) for i in range(1,pairs+1)] for v in ('base','new')}
def canon(series):
    out={}
    for s in series:
        key=(s['measurement'], tuple(sorted(s.get('tags',{}).items())))
        for f,d in s['fields'].items(): out[key+(f,)]=(d['timestamps'], d['values'])
    return out
def close(a,b):
    if a is None or b is None or isinstance(a,(bool,str)) or isinstance(b,(bool,str)): return a==b
    return a==b or abs(a-b)<=1e-10*max(abs(a),abs(b))
print(f"{'query':36s} {'points':>9s} {'first base':>10s} {'first new':>10s} {'steady base':>11s} {'steady new':>11s}  same")
for q in runs['base'][0]:
    fb=statistics.median(r[q]['first_ms'] for r in runs['base']); fn=statistics.median(r[q]['first_ms'] for r in runs['new'])
    sb=statistics.median(r[q]['server_ms'] for r in runs['base']); sn=statistics.median(r[q]['server_ms'] for r in runs['new'])
    a=canon(runs['base'][0][q]['result']); b=canon(runs['new'][0][q]['result'])
    same = a.keys()==b.keys() and all(a[k][0]==b[k][0] and len(a[k][1])==len(b[k][1]) and all(close(x,y) for x,y in zip(a[k][1],b[k][1])) for k in a)
    exact = a==b
    print(f"{q:36s} {runs['base'][0][q]['points']:9d} {fb:10.2f} {fn:10.2f} {sb:11.2f} {sn:11.2f}  {'bit-identical' if exact else ('numerically' if same else 'DIFFERENT')}")
