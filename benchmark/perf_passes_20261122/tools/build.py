#!/usr/bin/env python3
"""build.py <src.cpp> <out-binary> [--lib /path/to/liblibtimestar.a] [--inc /alt/source/root] [extra flags]
Compiles a standalone driver with the same flags as the unit tests and links the timestar static lib."""
from pathlib import Path
import json, shlex, subprocess, sys
repo = Path('/home/matt/Desktop/source/tsdb'); build = repo / 'build'
import os
src, out = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
rest = sys.argv[3:]
lib = None; inc = None; extra = []
i = 0
while i < len(rest):
    if rest[i] == '--lib': lib = rest[i+1]; i += 2
    elif rest[i] == '--inc': inc = rest[i+1]; i += 2
    else: extra.append(rest[i]); i += 1
cc = next(c for c in json.loads((build/'compile_commands.json').read_text())
          if c['file'].endswith('/memory_store_sorted_test.cpp') and 'timestar_unit_test' in c['command'])
cmd = []; skip = False
for w in shlex.split(cc['command']):
    if skip: skip = False; continue
    if w in ('-o', '-c'): skip = True; continue
    if w.startswith('-include') or w.endswith('cmake_pch.hxx'): continue
    if w == '-Winvalid-pch': continue
    if inc and str(repo) + '/lib' in w: w = w.replace(str(repo) + '/lib', inc + '/lib')
    cmd.append(w)
obj = out + '.o'
r = subprocess.run(cmd + extra + ['-c', src, '-o', obj], cwd=cc["directory"], capture_output=True, text=True)
if r.returncode: print(r.stderr[-3000:]); sys.exit(1)
link = shlex.split((build/'test/CMakeFiles/timestar_perf_test.dir/link.txt').read_text())
libs = link[link.index('../lib/liblibtimestar.a'):]
if lib: libs = [lib if w == '../lib/liblibtimestar.a' else w for w in libs]
subprocess.run([cmd[0], '-O3', '-g', '-o', out, obj, *libs], cwd=build/'test', check=True)
