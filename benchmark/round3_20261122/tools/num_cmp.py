#!/usr/bin/env python3
"""num_cmp.py dump <port> <out.json> | cmp <a.json> <b.json> : full bucketed results for every method/layout/interval, compared numerically."""
import http.client, json, sys, math
BASE=1_000_000_000_000_000_000; MIN=60_000_000_000; END=BASE+100*10000*MIN*11
def queries():
    for layout in ('blocky','interleaved','aligned'):
        for method in ('avg','min','max','sum','count','latest','first','median','stddev','stdvar','spread'):
            for iv in ('1m','5m','1h'):
                yield f'{method}:{layout}(v) @{iv}', {'query':f'{method}:{layout}(v)','startTime':BASE,'endTime':END,'aggregationInterval':iv}
        for iv in ('1m','10m'):
            yield f'avg:{layout}(v) by host @{iv}', {'query':f'avg:{layout}(v) by {{host}}','startTime':BASE,'endTime':END,'aggregationInterval':iv}
            yield f'max:{layout}(v) by rack @{iv}', {'query':f'max:{layout}(v) by {{rack}}','startTime':BASE,'endTime':END,'aggregationInterval':iv}
        yield f'avg:{layout}(v) start-aligned @7m', {'query':f'avg:{layout}(v)','startTime':BASE+12345,'endTime':END,'aggregationInterval':'7m','bucketAlignment':'start'}
if sys.argv[1]=='dump':
    c=http.client.HTTPConnection('127.0.0.1',int(sys.argv[2])); out={}
    for name,body in queries():
        c.request('POST','/query',body=json.dumps(body),headers={'Content-Type':'application/json'}); r=c.getresponse(); d=r.read(); assert r.status==200,(name,d[:200])
        j=json.loads(d)
        out[name]=sorted(((sorted(s.get('groupTags',[])), {f:[v['timestamps'],v['values']] for f,v in s['fields'].items()}) for s in j['series']), key=lambda x: json.dumps(x[0]))
    json.dump(out,open(sys.argv[3],'w'))
else:
    a=json.load(open(sys.argv[2])); b=json.load(open(sys.argv[3])); worst=0.0; worstq=None; structural=[]; exact=0
    for q in a:
        sa,sb=a[q],b[q]
        if [x[0] for x in sa]!=[x[0] for x in sb]: structural.append((q,'series')); continue
        same=True
        for (ta,fa),(tb,fb) in zip(sa,sb):
            for f in fa:
                if fa[f][0]!=fb[f][0]: structural.append((q,'timestamps')); same=False; break
                for x,y in zip(fa[f][1],fb[f][1]):
                    if x==y or (x is None and y is None): continue
                    same=False
                    if x is None or y is None: structural.append((q,'null')); break
                    rel=abs(x-y)/max(abs(x),abs(y),1e-300)
                    if rel>worst: worst=rel; worstq=q
        exact+=same
    print('queries',len(a),'bit-identical',exact,'structural differences',structural[:5],'largest relative value difference %.3g (%s)' % (worst,worstq))
