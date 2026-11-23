#!/usr/bin/env python3
"""build_agg.py <compiler> <out-binary> [extra flags]: compile lib/query/aggregator.cpp + merge_bench.cpp with <compiler>
(no PCH) and link them ahead of the already-built static lib, so only aggregator codegen differs between variants."""
import json, shlex, subprocess, sys, os
repo='/home/matt/Desktop/source/tsdb'; build=repo+'/build'
cxx, out = sys.argv[1], os.path.abspath(sys.argv[2]); extra = sys.argv[3:]
src = os.environ.get('AGG_SRC', repo+'/lib/query/aggregator.cpp')
cc = next(c for c in json.load(open(build+'/compile_commands.json')) if c['file'].endswith('/lib/query/aggregator.cpp'))
cmd=[]; skip=False
for w in shlex.split(cc['command']):
    if skip: skip=False; continue
    if w in ('-o','-c'): skip=True; continue
    if w == '-include': skip=True; continue
    if w.endswith('cmake_pch.hxx') or w=='-Winvalid-pch': continue
    cmd.append(w)
cmd[0]=cxx
objs=[]
for s,o in ((src, out+'.agg.o'), (repo+'/benchmark/perf_passes_20261122/tools/merge_bench.cpp', out+'.drv.o')):
    r=subprocess.run(cmd+['-I', repo+'/lib/query']+extra+['-c', s, '-o', o], cwd=cc['directory'], capture_output=True, text=True)
    if r.returncode: print(r.stderr[-4000:]); sys.exit(1)
    objs.append(o)
link=shlex.split(open(build+'/test/CMakeFiles/timestar_perf_test.dir/link.txt').read())
libs=link[link.index('../lib/liblibtimestar.a'):]
subprocess.run(['g++-15','-O3','-g','-o',out,*objs,*libs], cwd=build+'/test', check=True)
