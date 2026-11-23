#!/usr/bin/env python3
"""werror_ci.py <file-substr>...: fully compile (optimizer on, output discarded) the matching lib/bin TUs with the -Werror set CI uses (cmake/CompilerWarnings.cmake)."""
import json, subprocess, shlex, sys, re, concurrent.futures as cf
cc=json.load(open('compile_commands.json'))  # run from the build directory
jobs=[]
for e in cc:
    f=e['file']
    if not re.search(r'/(lib|bin)/[^ ]+\.cpp$', f) or '/build/' in f or '/external/' in f or '/_deps/' in f: continue
    if sys.argv[1:] and not any(s in f for s in sys.argv[1:]): continue
    args=shlex.split(e['command']); out=[]; skip=False
    for a in args:
        if skip: skip=False; out.append('/dev/null'); continue
        if a=='-o': skip=True
        out.append(a)
    out += ['-Werror', '-Wno-error=null-dereference', '-Wno-error=uninitialized', '-Wno-error=maybe-uninitialized', '-Wno-error=array-bounds', '-Wno-error=stringop-overflow', '-Wno-error=stringop-overread']
    jobs.append((f,out,e['directory']))
def run(j):
    f,a,d=j
    r=subprocess.run(a,cwd=d,capture_output=True,text=True)
    return f,r.returncode,r.stderr
bad=0
with cf.ThreadPoolExecutor(16) as ex:
    for f,rc,err in ex.map(run,jobs):
        if rc!=0:
            bad+=1; print('FAIL',f); print('\n'.join(l for l in err.split('\n') if 'error' in l)[:1200])
print(f'{len(jobs)} translation units, {bad} failed')
