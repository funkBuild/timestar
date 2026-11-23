from pathlib import Path
import json, shlex, subprocess, sys
repo=Path(__file__).resolve().parents[2]; build=repo/'build'
cc=next(c for c in json.loads((build/'compile_commands.json').read_text()) if c['file'].endswith('/memory_store_sorted_test.cpp') and 'timestar_unit_test' in c['command'])
mode=sys.argv[1]
out=build/('perf-before-20260911' if mode=='before' else 'perf-after-20260911')
out.mkdir(exist_ok=True)
flags=shlex.split(cc['command']); result=[]; skip=False
for word in flags:
    if skip: skip=False; continue
    if word in ['-o','-c']: skip=True; continue
    result.append(word)
if mode=='before': result.insert(1,'-I'+str(out))
result += ['-c',str(repo/'benchmark/performance_review_20260911/insert_cpu_bench.cpp'),'-o',str(out/'insert_cpu_bench.o')]
subprocess.run(result,cwd=cc['directory'],check=True)
link=shlex.split((build/'test/CMakeFiles/timestar_perf_test.dir/link.txt').read_text())
libs=link[link.index('../lib/liblibtimestar.a'):]
if mode=='before': libs=[str(out/'liblibtimestar.a') if w=='../lib/liblibtimestar.a' else w for w in libs]
subprocess.run(['/usr/bin/g++-14','-O3','-g','-o',str(out/'insert_cpu_bench'),str(out/'insert_cpu_bench.o'),*libs],cwd=build/'test',check=True)
print(mode,'CPU benchmark compiled.')
