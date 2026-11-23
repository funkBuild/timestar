from pathlib import Path
import json, shlex, subprocess, sys
repo=Path(__file__).resolve().parents[2]; build=repo/'build'
mode=sys.argv[1]; assert mode in ['before','after']
name=sys.argv[2] if len(sys.argv)>2 else 'decode_cpu_bench'
assert name in ('decode_cpu_bench','wire_probe','progression_cpu_bench','append_cpu_bench','tsm_decode_cpu_bench')
out=build/f'perf-query-pass12-{mode}'; out.mkdir(exist_ok=True)
cc=next(c for c in json.loads((build/'compile_commands.json').read_text()) if c['file'].endswith('/memory_store_sorted_test.cpp') and 'timestar_unit_test' in c['command'])
cmd=[];skip=False
for word in shlex.split(cc['command']):
 if skip: skip=False; continue
 if word in ['-o','-c']: skip=True; continue
 cmd.append(word)

cmd+=['-c',str(Path(__file__).with_name(name+'.cpp')),'-o',str(out/(name+'.o'))]
subprocess.run(cmd,cwd=cc['directory'],check=True)
link=shlex.split((build/'test/CMakeFiles/timestar_perf_test.dir/link.txt').read_text());libs=link[link.index('../lib/liblibtimestar.a'):]
if mode=='before':libs=[str(out/'liblibtimestar.a') if w=='../lib/liblibtimestar.a' else w for w in libs]
subprocess.run([cmd[0],'-O3','-g','-o',str(out/name),str(out/(name+'.o')),*libs],cwd=build/'test',check=True)
