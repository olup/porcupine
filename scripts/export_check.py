#!/usr/bin/env python3
"""Verify public SQL exports and optional concurrent ingestion on a TEST instance.

Uses only synthetic porcupine_load events already in the selected project. Never
prints signed URLs, access keys, event payloads or credentials.
"""
import argparse
from datetime import datetime,timezone
import gzip
import hashlib
import json
import os
import re
from pathlib import Path
from credentials import load_credentials
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--url',required=True)
p.add_argument('--credentials',required=True,type=Path)
p.add_argument('--output',required=True,type=Path)
p.add_argument('--mixed-seconds',type=int,default=0)
p.add_argument('--mixed-rps',type=float,default=5,help='100-event batch requests per second')
p.add_argument('--timeout',type=int,default=300)
a=p.parse_args()
keys=load_credentials(a.credentials)
keys.validate_target(a.url)
base=a.url.rstrip('/')
project='/api/v1/projects/'+keys['project_id']
marker='export-check-'+uuid.uuid4().hex

def request(method,path,body=None,key=None):
 if key is None:key=keys.get('read_token') or keys['admin_token']
 hdr={'Content-Type':'application/json','Authorization':'Bearer '+key}
 req=urllib.request.Request(base+path,method=method,data=json.dumps(body).encode() if body is not None else None,headers=hdr)
 try:
  with keys.open_api(req,timeout=20) as r:return r.status,json.load(r)
 except urllib.error.HTTPError as err:
  return err.code,None

def job_wait(job_id,terminal=True):
 end=time.monotonic()+a.timeout
 while time.monotonic()<end:
  status,job=request('GET',project+'/exports/'+job_id)
  if status!=200:raise RuntimeError('Cannot read export job')
  if job['state'] in ('failed','canceled','expired'):raise RuntimeError('Export state: '+job['state']+' '+job.get('error',''))
  if job['state']=='succeeded' or not terminal and job['state']=='running':return job
  time.sleep(.5)
 raise RuntimeError('Export did not finish within test deadline')

status,listing=request('GET',project+'/exports')
assert status==200 and listing['enabled'] and listing['limits']['max_concurrent_jobs']==1
status,source=request('POST',project+'/query',{'sql':'SELECT max(received_at_ms) AS cutoff,count() AS total FROM events WHERE event={event:String}','parameters':{'event':'porcupine_load'}})
assert status==200
cutoff=source['result']['data'][0]['cutoff'];expected=int(source['result']['data'][0]['total'])
assert expected>1000,'Seed the dedicated synthetic project before this test'
query='SELECT uuid,event,distinct_id,properties,timestamp,received_at FROM events WHERE event={event:String} AND received_at_ms<={cutoff:Int64} ORDER BY uuid'
body={'idempotency_key':marker,'sql':query,'parameters':{'event':'porcupine_load','cutoff':str(cutoff)},'format':'jsonl'}
started=time.monotonic(); started_at=datetime.now(timezone.utc).isoformat()
status,job=request('POST',project+'/exports',body)
assert status==202
job_id=job['id']
status,repeated=request('POST',project+'/exports',body)
assert status==202 and repeated['id']==job_id
assert request('POST',project+'/exports',{**body,'sql':'SELECT * FROM system.users'})[0]==400
job_wait(job_id,False)
# An extra queued task can be canceled without touching the active export.
status,queued=request('POST',project+'/exports',{**body,'idempotency_key':marker+'-cancel'})
assert status==202
assert request('DELETE',project+'/exports/'+queued['id'])[0]==202
assert request('GET',project+'/exports/'+queued['id']+'/files')[0]==409
mixed=None
mixed_file=a.output.with_name(a.output.stem+'-ingestion.json')
if a.mixed_seconds:
 mixed=subprocess.Popen([sys.executable,str(Path(__file__).with_name('load.py')),'--url',a.url,'--credentials',str(a.credentials),'--stage',f'{a.mixed_seconds}:{a.mixed_rps}:100:24','--query-interval','1','--drain-timeout','180','--output',str(mixed_file)],stdout=subprocess.DEVNULL)
try:
 finished=job_wait(job_id)
 assert int(finished['rows'])==expected
 files=[];offset=0
 while True:
  status,page=request('GET',project+f'/exports/{job_id}/files?limit=20&offset={offset}')
  assert status==200
  files.extend(page['files'])
  if page.get('next_offset') is None:break
  offset=page['next_offset']
 seen=set(); previous=None; total=0; artifacts=[]
 for file in files:
  digest=hashlib.sha256()
  class HashedReader:
   def __init__(self,raw):self.raw=raw;self.bytes=0
   def read(self,n=-1):
    data=self.raw.read(n);digest.update(data);self.bytes+=len(data);return data
  count=0
  with urllib.request.urlopen(file['url'],timeout=30) as response:
   assert response.headers.get('Content-Encoding') is None
   raw=HashedReader(response)
   with gzip.GzipFile(fileobj=raw,mode='rb') as decoded:
    for line in decoded:
     event=json.loads(line)
     assert event['event']=='porcupine_load' and event['uuid'] not in seen
     assert previous is None or event['uuid']>=previous
     previous=event['uuid'];seen.add(previous);count+=1
   assert raw.bytes==int(file['bytes'])
  assert digest.hexdigest()==file['sha256'] and count==int(file['rows'])
  total+=count
  artifacts.append({'index':file['index'],'rows':count,'bytes':file['bytes'],'sha256':file['sha256']})
 assert total==expected
 if mixed:
  mixed.wait(timeout=a.mixed_seconds+240)
  ingestion=json.loads(mixed_file.read_text())
  assert mixed.returncode==0 and ingestion['passed']
 else:ingestion=None
 report={'passed':True,'target':base,'started_at':started_at,'finished_at':datetime.now(timezone.utc).isoformat(),'job_id':job_id,'source_event':'porcupine_load','expected_events':expected,'exported_events':total,'files':artifacts,'decoded_bytes':finished['decoded_bytes'],'attempts':finished['attempts'],'elapsed_seconds':round(time.monotonic()-started,3),'limits':listing['limits'],'mixed_ingestion_passed':ingestion['passed'] if ingestion else None,'checks':['same-project SQL','full ordered result and unique UUIDs','gzip and compressed SHA256','exact create retry','unsafe SQL denied','queued cancellation','no files for canceled job','private signed download'],'production_connected':False}
 def epoch(value):
  return datetime.fromisoformat(re.sub(r'(\.\d{6})\d+',r'\1',value).replace('Z','+00:00')).timestamp()
 export_start,export_end=epoch(finished['started_at']),epoch(finished['completed_at'])
 report.update(export_started_at=finished['started_at'],export_completed_at=finished['completed_at'],export_execution_seconds=round(export_end-export_start,3))
 if ingestion:
  load_start=epoch(ingestion['started_at']);load_end=load_start+ingestion['ingest_elapsed_seconds']
  report['mixed_overlap_seconds']=round(max(0,min(export_end,load_end)-max(export_start,load_start)),3)
 a.output.parent.mkdir(parents=True,exist_ok=True)
 fd=os.open(a.output,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(fd,'w') as f:json.dump(report,f,indent=2);f.write('\n')
 print(json.dumps(report,indent=2))
finally:
 if mixed and mixed.poll() is None:
  mixed.terminate()
  mixed.wait(timeout=10)
