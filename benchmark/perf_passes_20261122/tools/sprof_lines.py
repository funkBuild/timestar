#!/usr/bin/env python3
"""Top source lines (with inlining) for the first in-binary frame of each sample; optional --focus on function substring anywhere in stack.
usage: sprof_lines.py samples binary [--focus S] [--top N] [--frame K]"""
import struct, subprocess, sys, collections, os, argparse
ap = argparse.ArgumentParser(); ap.add_argument('samples'); ap.add_argument('binary'); ap.add_argument('--top', type=int, default=25)
ap.add_argument('--focus', default=None); ap.add_argument('--skip-frac', type=float, default=0.0)
a = ap.parse_args()
MAX_DEPTH = 48; REC = MAX_DEPTH + 2
data = open(a.samples, 'rb').read(); count = min(struct.unpack_from('<Q', data, 0)[0], 1 << 19)
bn = os.path.realpath(a.binary); ranges = []; base = None
for line in open(a.samples + '.maps'):
    p = line.split()
    if len(p) >= 6 and os.path.realpath(p[5]) == bn:
        lo, hi = [int(x, 16) for x in p[0].split('-')]; off = int(p[2], 16); ranges.append((lo, hi))
        base = lo - off if base is None else min(base, lo - off)
lo_all = min(r[0] for r in ranges); hi_all = max(r[1] for r in ranges)
firsts = collections.Counter(); total = 0
for i in range(int(count * a.skip_frac), count):
    o = 8 + i * REC * 8; tid, n = struct.unpack_from('<QQ', data, o); fr = struct.unpack_from('<%dQ' % n, data, o + 16)[2:]
    total += 1
    for j, x in enumerate(fr):
        if lo_all <= x < hi_all:
            firsts[(x if j == 0 else x - 1, j > 0)] += 1; break
addrs = [k[0] for k in firsts]
out = subprocess.run(['addr2line', '-f', '-C', '-i', '-a', '-e', a.binary] + [hex(x - base) for x in addrs], capture_output=True, text=True).stdout.split('\n')
info = {}; cur = None
for l in out:
    if l.startswith('0x'): cur = int(l, 16) + base; info[cur] = []
    elif cur is not None and l: info[cur].append(l)
agg = collections.Counter()
for (x, viaLib), c in firsts.items():
    lines = info.get(x, [])
    pairs = [(lines[k][:70], lines[k + 1].split('/')[-1] if k + 1 < len(lines) else '') for k in range(0, len(lines), 2)]
    if a.focus and not any(a.focus in f for f, _ in pairs): continue
    key = ('[lib call] ' if viaLib else '') + ' <= '.join(f"{f.split('(')[0][-45:]}@{loc.split(' ')[0]}" for f, loc in pairs[:4])
    agg[key] += c
print('total samples', total)
for k, c in agg.most_common(a.top): print(f'{100*c/total:6.2f}% {c:6d}  {k}')
