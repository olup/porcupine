#!/usr/bin/env python3
"""Check bounded SQL concurrency on a synthetic test project; expect HTTP 429."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import json
from pathlib import Path
from credentials import load_credentials
import threading
import time
import urllib.error
import urllib.request

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--url',required=True)
p.add_argument('--credentials',type=Path,required=True)
p.add_argument('--output',type=Path,required=True)
p.add_argument('--concurrency',type=int,default=12)
a=p.parse_args()
assert 3<=a.concurrency<=32
keys=load_credentials(a.credentials)
keys.validate_target(a.url)
barrier=threading.Barrier(a.concurrency)
query="SELECT event, count() AS total FROM events GROUP BY event"

def call(_):
    req=urllib.request.Request(a.url.rstrip('/')+'/api/v1/projects/'+keys['project_id']+'/query',data=json.dumps({'sql':query,'use_cache':False}).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+(keys.get('read_token') or keys['admin_token'])})
    barrier.wait(timeout=30)
    start=time.monotonic()
    try:
        with keys.open_api(req,timeout=20) as r:
            body=json.load(r)
            assert 'data' in body['result']
            status=r.status
    except urllib.error.HTTPError as e:
        status=e.code
        e.close()
    return status,round((time.monotonic()-start)*1000,3)

with ThreadPoolExecutor(max_workers=a.concurrency) as pool:
    results=list(pool.map(call,range(a.concurrency)))
counts=Counter(str(status) for status,_ in results)
report={'passed':counts.get('200',0)>0 and counts.get('429',0)>0 and set(counts)<={'200','429'},'concurrency':a.concurrency,'sql':query,'status_counts':dict(counts),'latencies_ms':[t for _,t in results],'expected_guardrail':'2 concurrent analytical operations; excess returns HTTP 429','production_connected':False}
a.output.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
raise SystemExit(0 if report['passed'] else 1)
