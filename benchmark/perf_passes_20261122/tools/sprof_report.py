#!/usr/bin/env python3
"""Symbolize and summarize an sprof sample file.
usage: sprof_report.py <samples> <binary> [--top N] [--tid-min-samples K] [--focus substr]
"""
import struct, subprocess, sys, collections, re, argparse
ap = argparse.ArgumentParser()
ap.add_argument('samples'); ap.add_argument('binary')
ap.add_argument('--top', type=int, default=45)
ap.add_argument('--focus', default=None, help='only stacks containing this substring')
ap.add_argument('--stacks', type=int, default=0, help='print N most common folded stacks (depth-limited)')
ap.add_argument('--depth', type=int, default=8)
ap.add_argument('--skip-frac', type=float, default=0.0)
ap.add_argument('--take-frac', type=float, default=1.0)
a = ap.parse_args()
MAX_DEPTH = 48; REC = MAX_DEPTH + 2
data = open(a.samples, 'rb').read()
count = struct.unpack_from('<Q', data, 0)[0]
count = min(count, 1 << 19)
# find load base of binary
base = None
import os
bn = os.path.realpath(a.binary)
ranges = []
for line in open(a.samples + '.maps'):
    parts = line.split()
    if len(parts) >= 6 and os.path.realpath(parts[5]) == bn:
        lo, hi = [int(x, 16) for x in parts[0].split('-')]
        off = int(parts[2], 16)
        ranges.append((lo, hi, off))
        if base is None or lo - off < base: base = lo - off
lo_all = min(r[0] for r in ranges); hi_all = max(r[1] for r in ranges)
import bisect
libmaps = []  # (lo, hi, base, path)
_seen = {}
for line in open(a.samples + '.maps'):
    parts = line.split()
    if len(parts) >= 6 and parts[5].startswith('/') and os.path.realpath(parts[5]) != bn:
        lo, hi = [int(x, 16) for x in parts[0].split('-')]
        off = int(parts[2], 16)
        b = _seen.setdefault(parts[5], lo - off)
        libmaps.append((lo, hi, min(b, lo - off), parts[5]))
libsyms = {}
def libname(x):
    for lo, hi, b, path in libmaps:
        if lo <= x < hi:
            if path not in libsyms:
                out = subprocess.run(['nm', '-D', '--defined-only', path], capture_output=True, text=True).stdout
                tab = []
                for l in out.split('\n'):
                    f = l.split()
                    if len(f) >= 3:
                        try: tab.append((int(f[0], 16), f[2].split('@')[0]))
                        except ValueError: pass
                tab.sort()
                libsyms[path] = ([t[0] for t in tab], [t[1] for t in tab])
            addrs_, names_ = libsyms[path]
            i = bisect.bisect_right(addrs_, x - b) - 1
            return '[' + os.path.basename(path).split('.')[0] + ']' + (names_[i] if i >= 0 else '?')
    return '[anon/kernel]'
recs = []
addrs = set()
for i in range(int(count*a.skip_frac), int(count*a.take_frac)):
    o = 8 + i * REC * 8
    tid, n = struct.unpack_from('<QQ', data, o)
    fr = struct.unpack_from('<%dQ' % n, data, o + 16)
    # drop handler frames: first frames are handler + sigreturn trampoline (outside binary)
    fr = list(fr)
    # skip frames until after the signal trampoline: frames[0]=handler, [1]=__restore_rt
    fr = fr[2:]
    recs.append((tid, fr))
    for j, x in enumerate(fr):
        if lo_all <= x < hi_all: addrs.add(x if j == 0 else x - 1)
addrs = sorted(addrs)
sym = {}
if addrs:
    p = subprocess.run(['addr2line', '-f', '-C', '-e', a.binary] + [hex(x - base) for x in addrs],
                       capture_output=True, text=True)
    lines = p.stdout.split('\n')
    for k, x in enumerate(addrs):
        fn = lines[2 * k] if 2 * k < len(lines) else '??'
        sym[x] = fn
def short(fn):
    fn = re.sub(r'\[abi:[^\]]*\]', '', fn)
    # collapse template args
    out = []; d = 0
    for ch in fn:
        if ch == '<': d += 1; continue
        if ch == '>': d -= 1; continue
        if d == 0: out.append(ch)
    fn = ''.join(out)
    fn = re.sub(r'\(.*\)', '()', fn)
    return fn[:110]
def name(x, top):
    if lo_all <= x < hi_all: return short(sym.get(x if top else x - 1, '??'))
    return libname(x)
self_c = collections.Counter(); incl = collections.Counter(); stacks = collections.Counter()
tids = collections.Counter(); total = 0
for tid, fr in recs:
    names = [name(x, j == 0) for j, x in enumerate(fr)]
    if a.focus and not any(a.focus in n for n in names): continue
    total += 1; tids[tid] += 1
    if not names: continue
    self_c[names[0]] += 1
    for n in set(names): incl[n] += 1
    stacks[' <- '.join(names[:a.depth])] += 1
print(f'samples={total} (raw {count}) threads={dict(tids.most_common(8))}')
print('--- SELF ---')
for n, c in self_c.most_common(a.top): print(f'{100*c/total:6.2f}% {c:7d}  {n}')
print('--- INCLUSIVE ---')
for n, c in incl.most_common(a.top): print(f'{100*c/total:6.2f}% {c:7d}  {n}')
if a.stacks:
    print('--- STACKS ---')
    for s, c in stacks.most_common(a.stacks): print(f'{100*c/total:6.2f}% {c:6d}  {s}')
