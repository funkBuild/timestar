from pathlib import Path
import json, shlex, subprocess, sys
repo=Path(__file__).resolve().parents[2]; build=repo/'build'
mode=sys.argv[1]; assert mode in ['before','after']
out=build/f'perf-query-pass3-{mode}'; out.mkdir(exist_ok=True)
cc=next(c for c in json.loads((build/'compile_commands.json').read_text()) if c['file'].endswith('/memory_store_sorted_test.cpp') and 'timestar_unit_test' in c['command'])
flags=shlex.split(cc['command']); cmd=[]; skip=False
for word in flags:
 if skip: skip=False; continue
 if word in ['-o','-c']: skip=True; continue
 cmd.append(word)
if mode=='before': cmd.insert(1,'-I'+str(out/'lib/query'))
else: cmd.append('-DBENCH_COMPACT_OUTPUT')
cmd += ['-c',str(repo/'benchmark/query_pass3_20260911/query_cpu_bench.cpp'),'-o',str(out/'query_cpu_bench.o')]
subprocess.run(cmd,cwd=cc['directory'],check=True)
link=shlex.split((build/'test/CMakeFiles/timestar_perf_test.dir/link.txt').read_text()); libs=link[link.index('../lib/liblibtimestar.a'):]
if mode=='before': libs=[str(out/'liblibtimestar.a') if w=='../lib/liblibtimestar.a' else w for w in libs]
subprocess.run([cmd[0],'-O3','-g','-o',str(out/'query_cpu_bench'),str(out/'query_cpu_bench.o'),*libs],cwd=build/'test',check=True)
