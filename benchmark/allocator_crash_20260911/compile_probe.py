from pathlib import Path
import json,shlex,subprocess,sys
repo=Path(__file__).resolve().parents[2]
mode=sys.argv[1];assert mode in ['old','current','old-asan','current-asan']
san='asan' in mode;old=mode.startswith('old');build=repo/('build-asan' if san else 'build')
out=repo/'build/allocator-crash-probes'/mode;out.mkdir(parents=True,exist_ok=True)
cc=next(c for c in json.loads((build/'compile_commands.json').read_text()) if c['file'].endswith('/memory_store_sorted_test.cpp') and 'timestar_unit_test' in c['command'])
cmd=[];skip=False
for word in shlex.split(cc['command']):
 if skip:skip=False;continue
 if word in ['-c','-o']:skip=True;continue
 cmd.append(word)
previous=repo/'build/perf-query-pass7-before'
if old:cmd.insert(1,'-I'+str(previous/'lib/index/native'))
if old and san:
 subprocess.run(cmd+['-c',str(previous/'lib/index/native/native_index.cpp'),'-o',str(out/'native_index.cpp.o')],cwd=cc['directory'],check=True)
subprocess.run(cmd+['-c',str(Path(__file__).with_name('preemption_probe.cpp')),'-o',str(out/'probe.o')],cwd=cc['directory'],check=True)
link=shlex.split((build/'test/CMakeFiles/timestar_perf_test.dir/link.txt').read_text());libs=link[link.index('../lib/liblibtimestar.a'):]
if old and not san:libs=[str(previous/'liblibtimestar.a') if x=='../lib/liblibtimestar.a' else x for x in libs]
objects=[str(out/'probe.o')]+([str(out/'native_index.cpp.o')] if old and san else [])
subprocess.run([cmd[0],'-g',*(['-fsanitize=address,undefined'] if san else []),'-o',str(out/'preemption_probe'),*objects,*libs],cwd=build/'test',check=True)
