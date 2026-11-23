#!/usr/bin/env python3
"""bucket_ab.py <port> <out.json> [iters]: bucketed queries across methods/intervals/layouts; digest of each result + median server ms."""
import http.client, json, sys, hashlib, statistics
port=int(sys.argv[1]); out=sys.argv[2]; iters=int(sys.argv[3]) if len(sys.argv)>3 else 7
BASE=1_000_000_000_000_000_000; MIN=60_000_000_000; END=BASE+100*10000*MIN*11
c=http.client.HTTPConnection('127.0.0.1',port); res={}
Q=[]
for layout in ('blocky','interleaved','aligned'):
    for method in ('avg','min','max','sum','count','latest','first','median','stddev','stdvar','spread'):
        for iv in ('1m','5m','1h'):
            if method in ('median',) and iv=='1m' and layout=='blocky': pass
            Q.append((f'{method}:{layout}(v) @{iv}', {'query':f'{method}:{layout}(v)','startTime':BASE,'endTime':END,'aggregationInterval':iv}))
    for iv in ('1m','10m'):
        Q.append((f'avg:{layout}(v) by host @{iv}', {'query':f'avg:{layout}(v) by {{host}}','startTime':BASE,'endTime':END,'aggregationInterval':iv}))
        Q.append((f'max:{layout}(v) by rack @{iv}', {'query':f'max:{layout}(v) by {{rack}}','startTime':BASE,'endTime':END,'aggregationInterval':iv}))
    Q.append((f'avg:{layout}(v) start-aligned @7m', {'query':f'avg:{layout}(v)','startTime':BASE+12345,'endTime':END,'aggregationInterval':'7m','bucketAlignment':'start'}))
for name,body in Q:
    payload=json.dumps(body); sv=[]; digest=None; pts=0
    for i in range(iters if '@1m' not in name else max(3,iters//2)):
        c.request('POST','/query',body=payload,headers={'Content-Type':'application/json'}); r=c.getresponse(); d=r.read()
        assert r.status==200,(name,d[:300])
        j=json.loads(d); sv.append(j['statistics']['execution_time_ms'])
        if i==0:
            pts=j['statistics']['point_count']; j['statistics'].pop('execution_time_ms',None)
            for s in j['series']: s['groupTags']=sorted(s.get('groupTags',[]))
            j['series'].sort(key=lambda s: json.dumps(s['groupTags'])+s['measurement'])
            # values to 10 significant digits: float sums fold in shard-arrival order, which varies run to run in the last bits
            for srs in j['series']:
                for f in srs['fields'].values():
                    f['values']=[(float('%.10g' % v) if isinstance(v,float) else v) for v in f['values']]
            digest=hashlib.sha256(json.dumps(j,sort_keys=True).encode()).hexdigest()[:16]
    res[name]={'ms':statistics.median(sv),'points':pts,'digest':digest}
json.dump(res,open(out,'w'),indent=1)
