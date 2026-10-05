import csv, io, ipaddress, json, math, os, re, shutil, socket, sqlite3, ssl, struct, subprocess, threading, time, urllib.error, urllib.parse, urllib.request, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
 import fcntl
except ImportError:
 fcntl=None
from datetime import datetime, timedelta, timezone
from cryptography.fernet import Fernet, InvalidToken
from flask import Flask, Response, jsonify, render_template, request
from PIL import Image, ImageDraw, ImageFont
from zoneinfo import ZoneInfo

ROOT=os.path.dirname(os.path.abspath(__file__)); DATA=os.environ.get("PINGPILOT_DATA",os.path.join(ROOT,"data")); DB=os.path.join(DATA,"pingpilot.db"); KEY=os.path.join(DATA,".secrets.key"); DISPLAY_TZ=ZoneInfo("Asia/Tehran"); MAX_CHART_POINTS_PER_TARGET=720; os.makedirs(DATA,exist_ok=True)
app=Flask(__name__); app.config["JSON_SORT_KEYS"]=False
LOCK,RUN=threading.RLock(),threading.Lock(); STATE={"running":False,"paused":False,"started_at":None,"cycle_active":False,"elapsed_base":0,"last_resume_at":None,"last_hourly_report_at":None,"last_hourly_attempt_at":None}; STOP=threading.Event()
PROTOCOLS={"PING":None,"TCP":None,"UDP":None,"HTTP":80,"HTTPS":443,"DNS":53,"SSH":22,"TELNET":23,"FTP":21,"SMB":445,"RDP":3389,"SQL SERVER":1433,"TFTP":69,"SNMP":161}; TCP={"TCP","SSH","TELNET","FTP","SMB","RDP","SQL SERVER"}; UDP={"UDP","TFTP","SNMP"}; HOST=re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,252}$")
GLOBAL_RUN=threading.Lock();SCHEDULER_LOCK_HANDLE=None
GLOBAL_DEFAULTS=[
 ("United States","US","Google","https://www.google.com","HTTPS",443),
 ("United Kingdom","GB","BBC","https://www.bbc.co.uk","HTTPS",443),
 ("Germany","DE","Tagesschau","https://www.tagesschau.de","HTTPS",443),
 ("France","FR","France.fr","https://www.france.fr","HTTPS",443),
 ("Netherlands","NL","Government.nl","https://www.government.nl","HTTPS",443),
 ("Japan","JP","Japan Government","https://www.go.jp","HTTPS",443),
 ("Singapore","SG","Singapore Government","https://www.gov.sg","HTTPS",443),
 ("Australia","AU","Australia Government","https://www.australia.gov.au","HTTPS",443),
 ("Brazil","BR","Brazil Government","https://www.gov.br","HTTPS",443),
 ("Canada","CA","Canada Government","https://www.canada.ca","HTTPS",443),
]
def utc(): return datetime.now(timezone.utc)
def stamp(): return utc().isoformat().replace("+00:00","Z")
def connect():
 c=sqlite3.connect(DB,timeout=20);c.row_factory=sqlite3.Row;return c
def crypt():
 if not os.path.exists(KEY):
  open(KEY,"wb").write(Fernet.generate_key());os.chmod(KEY,0o600)
 return Fernet(open(KEY,"rb").read())
def enc(s): return crypt().encrypt((s or "").encode()).decode() if s else ""
def dec(s):
 try:return crypt().decrypt(s.encode()).decode() if s else ""
 except (InvalidToken,ValueError):return ""
def init():
 with LOCK,connect() as db:
  db.executescript("""CREATE TABLE IF NOT EXISTS targets(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL DEFAULT 0,name TEXT NOT NULL,host TEXT NOT NULL,port INTEGER NOT NULL DEFAULT 0,protocol TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,rocket_alert INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'UNKNOWN',detail TEXT NOT NULL DEFAULT '',last_ms REAL,ok_count INTEGER NOT NULL DEFAULT 0,fail_count INTEGER NOT NULL DEFAULT 0,consecutive_failures INTEGER NOT NULL DEFAULT 0,consecutive_successes INTEGER NOT NULL DEFAULT 0,updated_at TEXT);CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY AUTOINCREMENT,target_id INTEGER NOT NULL,created_at TEXT NOT NULL,success INTEGER NOT NULL,latency_ms REAL,detail TEXT NOT NULL);CREATE TABLE IF NOT EXISTS sample_rollups(target_id INTEGER NOT NULL,bucket_start INTEGER NOT NULL,success_count INTEGER NOT NULL DEFAULT 0,fail_count INTEGER NOT NULL DEFAULT 0,latency_sum REAL NOT NULL DEFAULT 0,latency_count INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(target_id,bucket_start));CREATE TABLE IF NOT EXISTS global_targets(id INTEGER PRIMARY KEY AUTOINCREMENT,position INTEGER NOT NULL DEFAULT 0,country TEXT NOT NULL,country_code TEXT NOT NULL DEFAULT '',name TEXT NOT NULL,host TEXT NOT NULL,port INTEGER NOT NULL DEFAULT 0,protocol TEXT NOT NULL DEFAULT 'HTTPS',enabled INTEGER NOT NULL DEFAULT 1,status TEXT NOT NULL DEFAULT 'UNKNOWN',detail TEXT NOT NULL DEFAULT '',last_ms REAL,ok_count INTEGER NOT NULL DEFAULT 0,fail_count INTEGER NOT NULL DEFAULT 0,updated_at TEXT);""")
  if "rocket_alert" not in {r[1] for r in db.execute("PRAGMA table_info(targets)")}:db.execute("ALTER TABLE targets ADD COLUMN rocket_alert INTEGER NOT NULL DEFAULT 0")
  for k,v in {"interval":"5","timeout":"3000","workers":"24","dns_domain":"example.com","failure_threshold":"3","recovery_threshold":"2","rocket_config":""}.items():db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",(k,v))
  db.execute("CREATE INDEX IF NOT EXISTS ix_samples_created_at ON samples(created_at)");db.execute("CREATE INDEX IF NOT EXISTS ix_samples_target_created_at ON samples(target_id,created_at)")
  db.execute("CREATE INDEX IF NOT EXISTS ix_global_targets_position ON global_targets(position)")
  if db.execute("SELECT COUNT(*) FROM global_targets").fetchone()[0]==0:
   for pos,(country,code,name,host,protocol,port) in enumerate(GLOBAL_DEFAULTS):db.execute("INSERT INTO global_targets(position,country,country_code,name,host,port,protocol) VALUES(?,?,?,?,?,?,?)",(pos,country,code,name,host,port,protocol))
  if db.execute("SELECT COUNT(*) FROM sample_rollups").fetchone()[0]==0 and db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]>0:db.execute("INSERT INTO sample_rollups(target_id,bucket_start,success_count,fail_count,latency_sum,latency_count) SELECT target_id,(CAST(strftime('%s',created_at) AS INTEGER)/60)*60,SUM(success),COUNT(*)-SUM(success),SUM(CASE WHEN latency_ms IS NULL THEN 0 ELSE latency_ms END),SUM(CASE WHEN latency_ms IS NULL THEN 0 ELSE 1 END) FROM samples GROUP BY target_id,(CAST(strftime('%s',created_at) AS INTEGER)/60)*60")
  db.commit()
def raw():
 with LOCK,connect() as db:return {x["key"]:x["value"] for x in db.execute("SELECT key,value FROM settings")}
def settings():
 r=raw();return {"interval":max(1,min(3600,int(r.get("interval","5")))),"timeout":max(250,min(60000,int(r.get("timeout","3000")))),"workers":max(1,min(128,int(r.get("workers","24")))),"dns_domain":r.get("dns_domain","example.com"),"failure_threshold":max(1,min(20,int(r.get("failure_threshold","3")))),"recovery_threshold":max(1,min(20,int(r.get("recovery_threshold","2"))))}
def rocket(secrets=False):
 d={"base_url":"","username":"","user_id":"","room_id":"","channel":"","recipient":"","room_ids":"","channels":"","recipients":"","password":"","auth_token":""}
 try:d.update(json.loads(dec(raw().get("rocket_config",""))))
 except Exception:pass
 if not d["room_ids"]:d["room_ids"]=d.get("room_id","")
 if not d["channels"]:d["channels"]=d.get("channel","")
 if not d["recipients"]:d["recipients"]=d.get("recipient","")
 d["configured"]=bool(d["base_url"] and (d["auth_token"] or(d["username"] and d["password"])))
 if not secrets:d["password"]="••••••" if d["password"] else "";d["auth_token"]="••••••" if d["auth_token"] else ""
 return d
def save_rocket(data):
 old=rocket(True);base=str(data.get("base_url",old["base_url"])).strip().rstrip("/");p=urllib.parse.urlparse(base) if base else None
 if base and(p.scheme not in {"http","https"} or not p.netloc):raise ValueError("Rocket.Chat URL must start with http:// or https://")
 d={k:str(data.get(k,old[k])).strip() for k in("base_url","username","user_id","room_ids","channels","recipients")}
 if data.get("clear_secrets"):d.update(password="",auth_token="")
 else:
  for k in("password","auth_token"):
   v=str(data.get(k,""));d[k]=old[k] if v in {"","••••••"} else v
 with LOCK,connect() as db:db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('rocket_config',?)",(enc(json.dumps(d)),));db.commit()
 return rocket()
def req_json(url,payload=None,headers=None):
 data=None if payload is None else json.dumps(payload).encode();r=urllib.request.Request(url,data=data,method="POST" if data else "GET");r.add_header("Accept","application/json")
 if data:r.add_header("Content-Type","application/json")
 for k,v in(headers or {}).items():r.add_header(k,v)
 try:
  with urllib.request.urlopen(r,timeout=15,context=ssl.create_default_context()) as x:return json.loads(x.read().decode() or "{}")
 except urllib.error.HTTPError as e:raise RuntimeError("Rocket.Chat HTTP %s"%e.code)
 except urllib.error.URLError as e:raise RuntimeError("Rocket.Chat connection error: %s"%e.reason)
def rocket_auth(c):
 if c["auth_token"] and c["user_id"]:return {"X-Auth-Token":c["auth_token"],"X-User-Id":c["user_id"]}
 if not(c["base_url"] and c["username"] and c["password"]):raise RuntimeError("Rocket.Chat credentials are incomplete")
 d=req_json(c["base_url"]+"/api/v1/login",{"user":c["username"],"password":c["password"]}).get("data",{})
 if not d.get("authToken") or not d.get("userId"):raise RuntimeError("Rocket.Chat authentication failed")
 return {"X-Auth-Token":d["authToken"],"X-User-Id":d["userId"]}
def destinations(value):return [x.strip() for x in re.split(r"[,;\r\n]+",str(value or "")) if x.strip()]
def channel_room(c,h,name):
 name=name.lstrip("#").strip()
 for endpoint,key in(("channels.info?roomName=","channel"),("groups.info?roomName=","group")):
  try:
   x=req_json(c["base_url"]+"/api/v1/"+endpoint+urllib.parse.quote(name),headers=h).get(key,{})
   if x.get("_id"):return x["_id"]
  except RuntimeError:pass
 raise RuntimeError("Rocket.Chat channel '%s' was not found"%name)
def rooms(c,h):
 ids=[]
 for rid in destinations(c.get("room_ids")):ids.append(rid)
 for user in destinations(c.get("recipients")):
  direct=req_json(c["base_url"]+"/api/v1/im.create",{"username":user},h).get("room",{})
  if not direct.get("_id"):raise RuntimeError("Rocket.Chat recipient '%s' was not found"%user)
  ids.append(direct["_id"])
 for name in destinations(c.get("channels")):ids.append(channel_room(c,h,name))
 ids=list(dict.fromkeys(ids))
 if not ids:raise RuntimeError("Add at least one Room ID, channel, or recipient")
 return ids
def rocket_send(text,html=None,attachment=None,filename=None,content_type=None):
 c=rocket(True);h=rocket_auth(c);ids=rooms(c,h);sent=0;errors=[]
 for rid in ids:
  try:
   if not html and attachment is None:
    if not req_json(c["base_url"]+"/api/v1/chat.postMessage",{"roomId":rid,"text":text},h).get("success"):raise RuntimeError("Rocket.Chat rejected the message")
   else:
    boundary="----PingPilot"+uuid.uuid4().hex;parts=[]
    def part(n,v,fn=None,ct=None):parts.extend([("--"+boundary+"\r\n").encode(),("Content-Disposition: form-data; name=\"%s\"%s\r\n"%(n,"; filename=\"%s\""%fn if fn else "")).encode(),("Content-Type: %s\r\n"%ct).encode() if ct else b"",b"\r\n",v if isinstance(v,bytes) else str(v).encode(),b"\r\n"])
    data=attachment if attachment is not None else html.encode();part("msg",text);part("file",data,filename or ("pingpilot-report.html" if html else "pingpilot-attachment.bin"),content_type or ("text/html" if html else "application/octet-stream"));parts.append(("--"+boundary+"--\r\n").encode());r=urllib.request.Request(c["base_url"]+"/api/v1/rooms.upload/"+urllib.parse.quote(rid),data=b"".join(parts),method="POST");r.add_header("Content-Type","multipart/form-data; boundary="+boundary)
    for k,v in h.items():r.add_header(k,v)
    try:
     with urllib.request.urlopen(r,timeout=25,context=ssl.create_default_context()) as x:json.loads(x.read().decode() or "{}")
    except urllib.error.HTTPError as e:raise RuntimeError("Rocket.Chat upload HTTP %s"%e.code)
   sent+=1
  except Exception as e:errors.append(str(e))
 if not sent:raise RuntimeError("Rocket.Chat delivery failed: "+"; ".join(errors[:3]))
 if errors:app.logger.warning("Rocket.Chat delivery partially failed: %s","; ".join(errors))
 return {"sent":sent,"total":len(ids),"failed":len(errors)}
def clean(d):
 p=str(d.get("protocol","PING")).upper().strip();host=str(d.get("host","")).strip()
 if p not in PROTOCOLS:raise ValueError("Unsupported protocol")
 if p in {"HTTP","HTTPS"}:
  if not host.startswith(("http://","https://")):host=("https://" if p=="HTTPS" else "http://")+host
  if not urllib.parse.urlparse(host).hostname:raise ValueError("Enter a valid HTTP/HTTPS URL")
 elif not HOST.match(host.strip("[]")):raise ValueError("Enter a valid hostname or IPv4/IPv6 address")
 try:port=int(d.get("port",PROTOCOLS[p] or 0) or 0)
 except:raise ValueError("Port must be a number")
 if not 0<=port<=65535:raise ValueError("Port must be between 0 and 65535")
 if not port and PROTOCOLS[p]:port=PROTOCOLS[p]
 if p in TCP|UDP|{"DNS"} and not port:raise ValueError("A port is required")
 return {"name":str(d.get("name","")).strip() or host,"host":host,"port":port,"protocol":p,"enabled":int(bool(d.get("enabled",True))),"rocket_alert":int(bool(d.get("rocket_alert",False)))}
def target_identity(t):
 host=str(t["host"]).strip().rstrip("/").casefold() if t["protocol"] in {"HTTP","HTTPS"} else str(t["host"]).strip("[]").casefold()
 return host,t["protocol"],int(t["port"] or 0)
def duplicate_target(db,t,exclude_id=None):
 for row in db.execute("SELECT id,host,port,protocol FROM targets"):
  if exclude_id is not None and row["id"]==exclude_id:continue
  if target_identity(row)==target_identity(t):return row["id"]
 return None
def performance_key(r):
 total=r["ok_count"]+r["fail_count"]
 if not total:return (1,r["position"],r["id"])
 availability=r["ok_count"]*100.0/total
 status={"ONLINE":3,"DEGRADED":2,"OFFLINE":1,"UNKNOWN":0}.get(r["status"],0)
 latency=r["last_ms"] if r["last_ms"] is not None else 999999.0
 return (0,-availability,-status,latency,r["name"].casefold(),r["id"])
def smart_sort(db):
 rows=list(db.execute("SELECT * FROM targets ORDER BY position,id"));rows.sort(key=performance_key)
 for pos,row in enumerate(rows):db.execute("UPDATE targets SET position=? WHERE id=?",(pos,row["id"]))
 return len(rows)
def weak_targets(rows):
 result=[]
 for r in rows:
  total=r["ok_count"]+r["fail_count"]
  if not total:continue
  av=round(r["ok_count"]*100.0/total,1);reasons=[]
  if r["fail_count"]:reasons.append("%d failed check%s"%(r["fail_count"],"s" if r["fail_count"]!=1 else ""))
  if av<99:reasons.append("%.1f%% availability"%av)
  if r["last_ms"] is not None and r["last_ms"]>500:reasons.append("%d ms last response"%round(r["last_ms"]))
  if r["status"] in {"OFFLINE","DEGRADED"}:reasons.append(r["status"].lower())
  if reasons:result.append({"id":r["id"],"name":r["name"],"host":r["host"],"protocol":r["protocol"],"availability":av,"last_ms":r["last_ms"],"reason":", ".join(dict.fromkeys(reasons))})
 return result
def all_targets():
 with LOCK,connect() as db:return [dict(x) for x in db.execute("SELECT * FROM targets ORDER BY position,id")]
def all_global_targets():
 with LOCK,connect() as db:return [dict(x) for x in db.execute("SELECT * FROM global_targets ORDER BY position,id")]
def global_serial(r):
 r=dict(r);total=r["ok_count"]+r["fail_count"];r["availability"]=round(100*r["ok_count"]/total,1) if total else None;r["enabled"]=bool(r["enabled"]);return r
def clean_global(d):
 t=clean({"name":d.get("name"),"host":d.get("host"),"protocol":d.get("protocol","HTTPS"),"port":d.get("port"),"enabled":d.get("enabled",True),"rocket_alert":False})
 country=str(d.get("country","")).strip()
 if not country or len(country)>80:raise ValueError("Country is required and must be under 80 characters")
 code=re.sub(r"[^A-Za-z]","",str(d.get("country_code","")).upper())[:3]
 return {**t,"country":country,"country_code":code}
def global_record(t,ok,latency,detail):
 with LOCK,connect() as db:
  r=db.execute("SELECT * FROM global_targets WHERE id=?",(t["id"],)).fetchone()
  if not r:return
  status="ONLINE" if ok else "OFFLINE"
  db.execute("UPDATE global_targets SET status=?,detail=?,last_ms=?,ok_count=?,fail_count=?,updated_at=? WHERE id=?",(status,detail,latency,r["ok_count"]+int(bool(ok)),r["fail_count"]+int(not ok),stamp(),t["id"]))
  db.commit()
def global_cycle():
 if not GLOBAL_RUN.acquire(False):return
 try:
  targets=[t for t in all_global_targets() if t["enabled"]]
  if not targets:return
  cfg={"timeout":2500,"dns_domain":"example.com"}
  with ThreadPoolExecutor(max_workers=min(16,max(1,len(targets)))) as pool:
   futures={pool.submit(probe,t,cfg):t for t in targets}
   for future in as_completed(futures):
    try:ok,latency,detail=future.result()
    except Exception as ex:ok,latency,detail=False,None,"Global probe error: %s"%ex
    global_record(futures[future],ok,latency,detail)
 finally:GLOBAL_RUN.release()
def global_scheduler():
 while not STOP.is_set():
  # Keep the scheduler cadence at one second. A slow external endpoint does
  # not block the next tick; overlapping full passes are still prevented.
  if not GLOBAL_RUN.locked():threading.Thread(target=global_cycle,name="pingpilot-global-cycle",daemon=True).start()
  STOP.wait(1)
def start_background_services():
 global SCHEDULER_LOCK_HANDLE
 lock_path=os.path.join(DATA,'.scheduler.lock')
 try:
  handle=open(lock_path,'a+')
  if fcntl:fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  SCHEDULER_LOCK_HANDLE=handle
 except (OSError,BlockingIOError):
  app.logger.warning('Scheduler already owned by another PingPilot process; this worker will serve HTTP only')
  return False
 threading.Thread(target=scheduler,name='pingpilot-scheduler',daemon=True).start()
 threading.Thread(target=global_scheduler,name='pingpilot-global-scheduler',daemon=True).start()
 return True
def dns(srv,port,domain,timeout):
 try:ipaddress.ip_address(srv.strip("[]"))
 except:return False,None,"DNS requires the DNS server IP address"
 labels=domain.strip(".").split(".");tx=int.from_bytes(os.urandom(2),"big");q=b"".join(bytes([len(x)])+x.encode("idna") for x in labels)+b"\0"+struct.pack("!HH",1,1);packet=struct.pack("!HHHHHH",tx,0x0100,1,0,0,0)+q;t=time.perf_counter()
 try:
  with socket.socket(socket.AF_INET6 if ":" in srv else socket.AF_INET,socket.SOCK_DGRAM) as x:x.settimeout(timeout);x.sendto(packet,(srv,port));res,_=x.recvfrom(4096)
  ms=(time.perf_counter()-t)*1000;rid,flags,_,ans,_,_=struct.unpack("!HHHHHH",res[:12]);code=flags&15
  if rid!=tx:return False,ms,"DNS transaction ID mismatch"
  if code==3:return False,ms,"DNS responded: NXDOMAIN"
  if code:return False,ms,"DNS response code %s"%code
  return ans>0,ms,"DNS answered %s (%d answer%s)"%(domain,ans,"s" if ans!=1 else "") if ans else "DNS responded without answers"
 except socket.timeout:return False,None,"DNS timeout"
 except OSError as e:return False,None,"DNS network error: %s"%e
def tcp(host,port,timeout,label):
 t=time.perf_counter()
 try:
  with socket.create_connection((host.strip("[]"),port),timeout=timeout):return True,(time.perf_counter()-t)*1000,"%s port %d is open"%(label,port)
 except socket.timeout:return False,None,"%s timeout"%label
 except ConnectionRefusedError:return False,None,"%s connection refused"%label
 except socket.gaierror as e:return False,None,"DNS resolution failed: %s"%e
 except OSError as e:return False,None,"Network error: %s"%e
def udp(host,port,timeout,label):
 try:
  with socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET,socket.SOCK_DGRAM) as x:x.settimeout(timeout);x.connect((host.strip("[]"),port));x.send(b"PingPilot health probe");x.recv(512);return True,None,"%s replied"%label
 except socket.timeout:return False,None,"%s packet sent; no response (health not confirmed)"%label
 except OSError as e:return False,None,"UDP network error: %s"%e
def icmp(host,timeout):
 target=host.strip('[]');wait=max(1,int(math.ceil(timeout)));ping=shutil.which('ping') or '/usr/bin/ping';started=time.perf_counter()
 try:
  result=subprocess.run([ping,'-6' if ':' in target else '-4','-n','-c','1','-W',str(wait),target],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=wait+2,env={'PATH':os.environ.get('PATH','/usr/sbin:/usr/bin:/sbin:/bin'),'LANG':'C.UTF-8'})
  elapsed=(time.perf_counter()-started)*1000
  if result.returncode==0:return True,elapsed,'ICMP reply'
  lines=(result.stdout or 'ICMP failed').strip().splitlines();return False,elapsed,lines[-1] if lines else 'ICMP failed'
 except FileNotFoundError:return False,None,'ICMP ping utility is not installed'
 except subprocess.TimeoutExpired:return False,None,'ICMP timeout'
 except Exception as e:return False,None,'ICMP error: %s'%e
 t=time.perf_counter()
 try:
  x=subprocess.run(["/bin/ping","-6" if ":" in host else "-4","-n","-c","1","-W",str(max(1,int(timeout))),host.strip("[]")],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=timeout+2)
  return (True,(time.perf_counter()-t)*1000,"ICMP reply") if x.returncode==0 else (False,None,(x.stdout or "ICMP failed").strip().splitlines()[-1])
 except:return False,None,"ICMP timeout or error"
def web(url,timeout):
 t=time.perf_counter()
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"PingPilot-Web/2.0"}),timeout=timeout,context=ssl.create_default_context()) as x:return 200<=x.getcode()<400,(time.perf_counter()-t)*1000,"HTTP %d"%x.getcode()
 except urllib.error.HTTPError as e:return False,(time.perf_counter()-t)*1000,"HTTP status %s"%e.code
 except ssl.SSLError as e:return False,None,"TLS certificate error: %s"%e
 except Exception as e:return False,None,"HTTP network error: %s"%e
def probe(t,c):
 p=t["protocol"];timeout=c["timeout"]/1000
 if p=="PING":return icmp(t["host"],timeout)
 if p=="DNS":return dns(t["host"],t["port"] or 53,c["dns_domain"],timeout)
 if p in {"HTTP","HTTPS"}:return web(t["host"],timeout)
 if p in TCP:return tcp(t["host"],t["port"],timeout,p)
 return udp(t["host"],t["port"],timeout,p)
def alert(t,old,status,detail,latency,failures):
 if not STATE["running"] or not t["rocket_alert"] or status not in {"ONLINE","OFFLINE","DEGRADED"} or status==old:return
 icon,title=("🟢","RECOVERED") if status=="ONLINE" else ("🔴","INCIDENT OPENED")
 text="%s **%s**\n\nTarget: **%s**\nHost: `%s`\nProtocol: **%s**\nStatus: **%s**\nReason: %s\nLatency: %s\nConsecutive failures: %s\nTime: %s"%(icon,title,t["name"],t["host"],t["protocol"],status,detail,"%d ms"%latency if latency is not None else "—",failures,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
 try:rocket_send(text)
 except Exception as e:app.logger.warning("Rocket alert failed: %s",e)
def record(t,ok,latency,detail,c):
 with LOCK,connect() as db:
  # A probe may finish after Stop; its result must never revive cleared counters.
  if not STATE["running"] or STATE["paused"]:return
  r=db.execute("SELECT * FROM targets WHERE id=?",(t["id"],)).fetchone()
  if not r:return
  cf=0 if ok else r["consecutive_failures"]+1;cs=r["consecutive_successes"]+1 if ok else 0;status=("ONLINE" if cs>=c["recovery_threshold"] or r["status"] in {"UNKNOWN","ONLINE"} else "DEGRADED") if ok else ("OFFLINE" if cf>=c["failure_threshold"] else "DEGRADED")
  checked_at=stamp();success=int(bool(ok));latency_value=float(latency or 0);latency_count=1 if latency is not None else 0;bucket_start=int(time.time()//60)*60
  db.execute("UPDATE targets SET status=?,detail=?,last_ms=?,ok_count=?,fail_count=?,consecutive_failures=?,consecutive_successes=?,updated_at=? WHERE id=?",(status,detail,latency,r["ok_count"]+ok,r["fail_count"]+(not ok),cf,cs,checked_at,t["id"]));db.execute("INSERT INTO samples(target_id,created_at,success,latency_ms,detail) VALUES(?,?,?,?,?)",(t["id"],checked_at,success,latency,detail));db.execute("INSERT INTO sample_rollups(target_id,bucket_start,success_count,fail_count,latency_sum,latency_count) VALUES(?,?,?,?,?,?) ON CONFLICT(target_id,bucket_start) DO UPDATE SET success_count=success_count+excluded.success_count,fail_count=fail_count+excluded.fail_count,latency_sum=latency_sum+excluded.latency_sum,latency_count=latency_count+excluded.latency_count",(t["id"],bucket_start,success,1-success,latency_value,latency_count));db.execute("DELETE FROM samples WHERE created_at<datetime('now','-60 days')");db.execute("DELETE FROM sample_rollups WHERE bucket_start<?",(int((utc()-timedelta(days=60)).timestamp()),));db.commit();a=(dict(r),r["status"],status,detail,latency,cf)
 threading.Thread(target=alert,args=a,daemon=True).start()
def cycle():
 if not RUN.acquire(False):return
 try:
  if not STATE["running"] or STATE["paused"]:return
  STATE["cycle_active"]=True;c=settings();ts=[x for x in all_targets() if x["enabled"]]
  with ThreadPoolExecutor(max_workers=c["workers"]) as e:
   fs={e.submit(probe,t,c):t for t in ts}
   for f in as_completed(fs):
    try:ok,lat,detail=f.result()
    except Exception as ex:ok,lat,detail=False,None,"Probe error: %s"%ex
    record(fs[f],ok,lat,detail,c)
  # Reorder only after a complete pass, so the table reflects current performance
  # without jumping while individual probe results are still arriving.
  with LOCK,connect() as db:
   smart_sort(db);db.commit()
  hourly_connectivity_report()
 finally:STATE["cycle_active"]=False;RUN.release()
def scheduler():
 while not STOP.is_set():
  if STATE["running"] and not STATE["paused"]:cycle();STOP.wait(settings()["interval"])
  else:STOP.wait(.5)
def serial(r):
 r=dict(r);total=r["ok_count"]+r["fail_count"];r["availability"]=round(100*r["ok_count"]/total,1) if total else None;r["enabled"]=bool(r["enabled"]);r["rocket_alert"]=bool(r["rocket_alert"]);return r
def samples(seconds,ids):
 q="SELECT target_id,created_at,success,latency_ms,detail FROM samples WHERE created_at>=?";p=[(utc()-timedelta(seconds=seconds)).isoformat().replace("+00:00","Z")]
 if ids:q+=" AND target_id IN (%s)"%",".join("?"*len(ids));p+=ids
 q+=" ORDER BY created_at"
 with LOCK,connect() as db:return [dict(x) for x in db.execute(q,p)]
def chart_samples(seconds,ids):
 bucket=max(1,int(math.ceil(float(seconds)/MAX_CHART_POINTS_PER_TARGET)))
 if seconds>=1800:
  bucket=max(60,bucket);q="SELECT target_id,datetime(MAX(bucket_start),'unixepoch')||'Z' AS created_at,100.0*SUM(success_count)/NULLIF(SUM(success_count+fail_count),0) AS availability,SUM(latency_sum)/NULLIF(SUM(latency_count),0) AS latency_ms,SUM(success_count) AS success_count,SUM(fail_count) AS fail_count,SUM(success_count+fail_count) AS sample_count FROM sample_rollups WHERE bucket_start>=?";p=[int((utc()-timedelta(seconds=seconds)).timestamp())]
  if ids:q+=" AND target_id IN (%s)"%",".join("?"*len(ids));p+=ids
  q+=" GROUP BY target_id,bucket_start/? ORDER BY created_at";p.append(bucket)
 else:
  cutoff=(utc()-timedelta(seconds=seconds)).isoformat().replace("+00:00","Z");q="SELECT target_id,MAX(created_at) AS created_at,100.0*AVG(success) AS availability,AVG(latency_ms) AS latency_ms,SUM(success) AS success_count,COUNT(*)-SUM(success) AS fail_count,COUNT(*) AS sample_count FROM samples WHERE created_at>=?";p=[cutoff]
  if ids:q+=" AND target_id IN (%s)"%",".join("?"*len(ids));p+=ids
  q+=" GROUP BY target_id,CAST(strftime('%s',created_at) AS INTEGER)/? ORDER BY created_at";p.append(bucket)
 with LOCK,connect() as db:
  rows=[]
  for row in db.execute(q,p):
   x=dict(row);x["availability"]=round(float(x["availability"] or 0),2);x["success"]=bool(x["success_count"]);rows.append(x)
  return rows,bucket
def chart_font(size,bold=False):
 try:return ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans%s.ttf"%("-Bold" if bold else ""),size)
 except:return ImageFont.load_default()
def outage_timeline_png(targets,rows,seconds=3600):
 width,left,right=1600,275,55;header,footer,row_h=118,72,34;height=max(430,header+footer+row_h*len(targets))
 image=Image.new("RGB",(width,height),"#111d2d");draw=ImageDraw.Draw(image);title=chart_font(27,True);label=chart_font(16,True);text=chart_font(14);small=chart_font(12)
 draw.rounded_rectangle((0,0,width-1,height-1),radius=18,fill="#172538",outline="#35506b",width=2)
 draw.text((30,25),"PingPilot — Outage timeline by target",font=title,fill="#eef5ff")
 end=utc();start=end-timedelta(seconds=seconds);window=end-start;local_start=start.astimezone(DISPLAY_TZ);local_end=end.astimezone(DISPLAY_TZ)
 draw.text((31,64),"Last 1 hour · Asia/Tehran · Green = response received · Red = outage / failed check",font=label,fill="#a9bacd")
 draw.text((width-325,64),"%s → %s"%(local_start.strftime("%H:%M:%S"),local_end.strftime("%H:%M:%S")),font=label,fill="#b9f866")
 usable=width-left-right;by={}
 for row in rows:by.setdefault(row["target_id"],[]).append(row)
 for i,target in enumerate(targets):
  y=header+i*row_h;shade="#182b40" if i%2==0 else "#152538";draw.rounded_rectangle((18,y,width-18,y+row_h-5),radius=7,fill=shade)
  name=(target["name"]+" · "+target["host"])[:34];draw.text((30,y+9),name,font=label,fill="#dce8f5")
  draw.text((30,y+29),target["protocol"],font=small,fill="#94a9c0")
  line_y=y+20;draw.line((left,line_y,width-right,line_y),fill="#486078",width=1)
  for row in by.get(target["id"],[]):
   try:when=datetime.fromisoformat(row["created_at"].replace("Z","+00:00"))
   except:continue
   ratio=(when-start).total_seconds()/max(1,window.total_seconds())
   if ratio<0 or ratio>1:continue
   x=int(left+ratio*usable);color="#38d4c0" if row["success"] else "#fb7185";radius=3 if row["success"] else 5
   draw.ellipse((x-radius,line_y-radius,x+radius,line_y+radius),fill=color)
 for part in range(5):
  x=left+part*usable/4;draw.line((x,header-10,x,height-footer+6),fill="#2c435a",width=1)
  tm=(start+window*part/4).astimezone(DISPLAY_TZ);draw.text((x-24,height-footer+18),tm.strftime("%H:%M"),font=small,fill="#9db0c4")
 draw.rectangle((left,height-33,left+11,height-22),fill="#38d4c0");draw.text((left+17,height-37),"Response",font=small,fill="#d9e6f2")
 draw.rectangle((left+125,height-33,left+136,height-22),fill="#fb7185");draw.text((left+142,height-37),"Outage",font=small,fill="#d9e6f2")
 out=io.BytesIO();image.save(out,format="PNG",optimize=True);return out.getvalue()
def hourly_connectivity_report():
 if not STATE["running"] or STATE["paused"]:return
 now=utc();last=STATE.get("last_hourly_report_at");attempt=STATE.get("last_hourly_attempt_at")
 try:due=not last or (now-datetime.fromisoformat(last.replace("Z","+00:00"))).total_seconds()>=3600
 except:due=True
 if not due:return
 # A failed delivery is retried after five minutes; only successful delivery closes this hourly window.
 try:retry_due=not attempt or (now-datetime.fromisoformat(attempt.replace("Z","+00:00"))).total_seconds()>=300
 except:retry_due=True
 if not retry_due:return
 STATE["last_hourly_attempt_at"]=stamp();ts=all_targets();ss=samples(3600,[]);by={}
 for s in ss:(by.setdefault(s["target_id"],[])).append(s)
 weak=[]
 for t in ts:
  rows=by.get(t["id"],[]);total=len(rows)
  if not total:continue
  ok=sum(r["success"] for r in rows);fail=total-ok;availability=round(ok*100.0/total,1);lat=[r["latency_ms"] for r in rows if r["latency_ms"] is not None];avg=round(sum(lat)/len(lat)) if lat else None
  if fail or availability<99 or (avg is not None and avg>500) or t["status"] in {"OFFLINE","DEGRADED"}:
   weak.append((t,availability,ok,fail,avg))
 if not weak:
  STATE["last_hourly_report_at"]=stamp();return
 lines=["⚠ **PingPilot hourly connectivity summary**","Window: last 1 hour","Problematic targets: %d"%len(weak),""]
 for t,av,ok,fail,avg in weak[:30]:lines.append("• **%s** (`%s`) · %s · %s%% · OK %d / Fail %d%s"%(t["name"],t["host"],t["protocol"],av,ok,fail,(" · Avg %d ms"%avg) if avg is not None else ""))
 if len(weak)>30:lines.append("… and %d more target(s)"%(len(weak)-30))
 try:
  chart=outage_timeline_png(ts,ss);delivery=rocket_send("\n".join(lines),attachment=chart,filename="pingpilot-outage-timeline-1h.png",content_type="image/png");app.logger.info("Hourly connectivity report and outage timeline delivered to %d of %d Rocket.Chat destinations",delivery["sent"],delivery["total"])
  STATE["last_hourly_report_at"]=stamp()
 except Exception as e:app.logger.warning("Hourly connectivity report failed: %s",e)
def h(v):return str(v or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"','&quot;')
def report_html(seconds,ids):
 ts=[serial(x) for x in all_targets() if not ids or x["id"] in ids];ss=samples(seconds,[x["id"] for x in ts]);ok=sum(x["success"] for x in ss);fail=len(ss)-ok;av=round(100*ok/len(ss),2) if ss else 0
 rows="".join("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s / %s / %s%%</td></tr>"%(h(t["name"]),h(t["host"]),h(t["protocol"]),h(t["status"]),h(t["detail"]),t["ok_count"],t["fail_count"],t["availability"] if t["availability"] is not None else "—") for t in ts)
 return "<!doctype html><meta charset=utf-8><title>PingPilot report</title><style>body{font:14px Arial;background:#0b1220;color:#e7eef8;padding:30px}.c{display:inline-block;background:#17283a;border:1px solid #32607a;border-radius:12px;padding:16px;margin:5px;min-width:130px}.v{font:bold 24px monospace;color:#b9f866}table{width:100%%;border-collapse:collapse;margin-top:20px}th,td{padding:9px;border-bottom:1px solid #2c4158;text-align:left}th{background:#22364d}</style><h1 style='color:#39d5c0'>PingPilot Web — Monitoring Report</h1><p>Generated %s · Last %d minutes · %d target(s)</p><div class=c>Checks<div class=v>%d</div></div><div class=c>Success<div class=v>%d</div></div><div class=c>Fail / outage events<div class=v>%d</div></div><div class=c>Availability<div class=v>%s%%</div></div><table><thead><tr><th>Name</th><th>Host</th><th>Protocol</th><th>Status</th><th>Last detail</th><th>OK / Fail / Availability</th></tr></thead><tbody>%s</tbody></table>"%(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),seconds//60,len(ts),len(ss),ok,fail,av,rows)
def runtime_view():
 r=dict(STATE)
 if r["running"] and not r["paused"] and r["last_resume_at"]:
  try:r["elapsed_seconds"]=int(r["elapsed_base"]+(utc()-datetime.fromisoformat(r["last_resume_at"].replace("Z","+00:00"))).total_seconds())
  except:r["elapsed_seconds"]=int(r["elapsed_base"])
 else:r["elapsed_seconds"]=int(r["elapsed_base"])
 r.pop("elapsed_base",None);r.pop("last_resume_at",None)
 return r
def tool_host(value):
 raw=str(value or "").strip()
 parsed=urllib.parse.urlparse(raw if "://" in raw else "//"+raw)
 host=(parsed.hostname or "").strip("[]")
 if not host or not HOST.match(host):raise ValueError("Enter a valid hostname or IP address")
 return host
def tool_url(value):
 pass
def speed_target(value,scheme):
 raw=str(value or '').strip();scheme=str(scheme or 'http').lower()
 if scheme not in {'http','https'}:raise ValueError('Speed test scheme must be HTTP or HTTPS')
 if '://' not in raw:raw=scheme+'://'+raw
 parsed=urllib.parse.urlparse(raw);host=(parsed.hostname or '').strip('[]')
 if parsed.scheme not in {'http','https'} or not parsed.netloc or parsed.username or parsed.password or not host or not HOST.match(host):raise ValueError('Enter a valid URL, hostname, or IP address')
 return raw
def speed_quality(bytes_per_second,complete):
 if not complete:return 'Not rated - the destination returned less data than requested'
 mbps=bytes_per_second*8/1000000
 if mbps<1:return 'Poor - below 1 Mbps'
 if mbps<5:return 'Limited - 1 to 5 Mbps'
 if mbps<25:return 'Good - 5 to 25 Mbps'
 if mbps<100:return 'Very good - 25 to 100 Mbps'
 return 'Excellent - 100 Mbps or higher'
 raw=str(value or "").strip();parsed=urllib.parse.urlparse(raw)
 if parsed.scheme not in {"http","https"} or not parsed.netloc or parsed.username or parsed.password:raise ValueError("Destination must be a plain http:// or https:// URL")
 host=(parsed.hostname or "").strip("[]")
 if not host or not HOST.match(host):raise ValueError("Enter a valid destination hostname or IP address")
 return raw
def speed_test(url,mode,download_bytes,upload_bytes,timeout):
 pass
def speed_test_v2(url,mode,size_bytes,timeout):
 result={'target':url,'mode':mode,'requested_bytes':size_bytes,'download_bytes':0,'upload_bytes':0,'download_elapsed_ms':None,'upload_elapsed_ms':None,'download_bytes_per_second':0,'upload_bytes_per_second':0,'download_status':None,'upload_status':None,'upload_method':None,'download_error':None,'upload_error':None,'errors':[]}
 context=ssl.create_default_context()
 def rate(amount,elapsed):return amount/max(elapsed,0.000001)
 if mode in {'download','both'}:
  started=time.perf_counter()
  try:
   request_obj=urllib.request.Request(url,headers={'User-Agent':'PingPilot-Web/2.1','Range':'bytes=0-%d'%(size_bytes-1)})
   with urllib.request.urlopen(request_obj,timeout=timeout,context=context) as response:
    result['download_status']=response.getcode();remaining=size_bytes
    while remaining>0:
     chunk=response.read(min(1024*1024,remaining))
     if not chunk:break
     result['download_bytes']+=len(chunk);remaining-=len(chunk)
   elapsed=time.perf_counter()-started;result['download_elapsed_ms']=round(elapsed*1000,2);result['download_bytes_per_second']=round(rate(result['download_bytes'],elapsed),2)
  except urllib.error.HTTPError as error:
   result['download_error']='HTTP %s'%error.code;result['errors'].append('Download failed: '+result['download_error'])
  except Exception as error:
   result['download_error']=str(error);result['errors'].append('Download failed: '+result['download_error'])
 if mode in {'upload','both'}:
  started=time.perf_counter();body=os.urandom(size_bytes);upload_error=None
  for method in ('POST','PUT'):
   try:
    request_obj=urllib.request.Request(url,data=body,method=method,headers={'User-Agent':'PingPilot-Web/2.1','Content-Type':'application/octet-stream','Content-Length':str(size_bytes)})
    with urllib.request.urlopen(request_obj,timeout=timeout,context=context) as response:response.read(4096);result['upload_status']=response.getcode();result['upload_method']=method
    elapsed=time.perf_counter()-started;result['upload_bytes']=size_bytes;result['upload_elapsed_ms']=round(elapsed*1000,2);result['upload_bytes_per_second']=round(rate(size_bytes,elapsed),2);break
   except urllib.error.HTTPError as error:
    upload_error='HTTP %s via %s'%(error.code,method)
    result['upload_error']=upload_error;result['upload_method']=method
    if error.code not in {405,501}:break
   except Exception as error:
    upload_error=str(error);result['upload_error']=upload_error;result['upload_method']=method;break
  if result['upload_status'] is None:result['errors'].append('Upload failed: %s. The destination must accept HTTP POST or PUT with an octet-stream body.'%(upload_error or 'destination rejected the request'))
 def leg(title,amount,elapsed,status,method=None,error=None):
  if elapsed is None:return [title,'  Status: '+(('Rejected - '+error) if error else 'not run')]
  throughput=amount/max(elapsed/1000,0.000001);mbps=throughput*8/1000000;complete=amount>=size_bytes;status_text='HTTP %s'%status if status is not None else 'no HTTP response'
  if method:status_text+=' via '+method
  return [title,'  Status: '+status_text,'  Transferred: %.2f MB / %.2f MB'%(amount/1048576,size_bytes/1048576),'  Throughput: %.2f MB/s (%.2f Mbps)'%(throughput/1048576,mbps),'  Elapsed: %.2f ms'%elapsed,'  Quality: '+speed_quality(throughput,complete)]
 display_mode={'download':'Download','upload':'Upload','both':'Download + Upload'}[mode]
 lines=['PINGPILOT SPEED TEST','========================================','Target: '+url,'Mode: '+display_mode,'Test size: %.2f MB (%d bytes)'%(size_bytes/1048576,size_bytes),'',*leg('DOWNLOAD',result['download_bytes'],result['download_elapsed_ms'],result['download_status'],error=result['download_error']),'',*leg('UPLOAD',result['upload_bytes'],result['upload_elapsed_ms'],result['upload_status'],result['upload_method'],result['upload_error'])]
 if result['errors']:lines+=['','NOTES:']+['  - '+error for error in result['errors']]+['','RESULT: Completed with the notes above.']
 else:lines+=['','RESULT: Completed successfully.']
 lines+=['Quality is an indicative HTTP throughput rating; the destination server can limit the measured speed.']
 result['output']='\n'.join(lines);return result
 result={"target":url,"mode":mode,"download_requested_bytes":download_bytes,"upload_requested_bytes":upload_bytes,"download_bytes":0,"upload_bytes":0,"download_elapsed_ms":None,"upload_elapsed_ms":None,"download_bytes_per_second":0,"upload_bytes_per_second":0,"download_status":None,"upload_status":None,"errors":[]}
 def rate(n,elapsed):return round(n/max(elapsed,0.000001),2)
 if mode in {"download","both"}:
  started=time.perf_counter()
  try:
   req=urllib.request.Request(url,headers={"User-Agent":"PingPilot-Web/2.0","Range":"bytes=0-%d"%(download_bytes-1)})
   with urllib.request.urlopen(req,timeout=timeout,context=ssl.create_default_context()) as response:
    result["download_status"]=response.getcode();remaining=download_bytes
    while remaining>0:
     chunk=response.read(min(1024*1024,remaining))
     if not chunk:break
     result["download_bytes"]+=len(chunk);remaining-=len(chunk)
   elapsed=time.perf_counter()-started;result["download_elapsed_ms"]=round(elapsed*1000,2);result["download_bytes_per_second"]=rate(result["download_bytes"],elapsed)
  except Exception as e:result["errors"].append("Download: %s"%e)
 if mode in {"upload","both"}:
  started=time.perf_counter()
  body=os.urandom(upload_bytes);upload_error=None
  for method in ("POST","PUT"):
   try:
    req=urllib.request.Request(url,data=body,method=method,headers={"User-Agent":"PingPilot-Web/2.0","Content-Type":"application/octet-stream","Content-Length":str(upload_bytes)})
    with urllib.request.urlopen(req,timeout=timeout,context=ssl.create_default_context()) as response:response.read(4096);result["upload_status"]=response.getcode();result["upload_method"]=method
    elapsed=time.perf_counter()-started;result["upload_bytes"]=upload_bytes;result["upload_elapsed_ms"]=round(elapsed*1000,2);result["upload_bytes_per_second"]=rate(upload_bytes,elapsed);break
   except urllib.error.HTTPError as e:
    upload_error="HTTP %s via %s"%(e.code,method)
    if e.code not in {405,501}:break
   except Exception as e:upload_error=str(e);break
  if not result["upload_status"]:result["errors"].append("Upload failed: %s. The destination must accept HTTP POST or PUT with an octet-stream body."%(upload_error or "destination rejected the request"))
 def line_bytes(n):return "%d bytes/s"%round(n)
 lines=["Speed test target: %s"%url,"Mode: %s"%mode,"Download: %d / %d bytes · %s · %s"%(result["download_bytes"],download_bytes,line_bytes(result["download_bytes_per_second"]),("HTTP %s"%result["download_status"] if result["download_status"] else "not run")),"Upload: %d / %d bytes · %s · %s"%(result["upload_bytes"],upload_bytes,line_bytes(result["upload_bytes_per_second"]),("HTTP %s"%result["upload_status"] if result["upload_status"] else "not run"))]
 if result["download_elapsed_ms"] is not None:lines.append("Download elapsed: %.2f ms"%result["download_elapsed_ms"])
 if result["upload_elapsed_ms"] is not None:lines.append("Upload elapsed: %.2f ms"%result["upload_elapsed_ms"])
 if result["errors"]:lines.extend(["","Errors:"]+result["errors"])
 else:lines.append("Completed successfully.")
 result["output"]="\n".join(lines);return result
def tool_run(args,timeout=18):
 try:
  result=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=timeout,env={"PATH":os.environ.get("PATH","/usr/sbin:/usr/bin:/sbin:/bin"),"LANG":"C.UTF-8"})
  output=(result.stdout or "").strip()
  return output[:18000] or "Command completed without output"
 except subprocess.TimeoutExpired:return "Tool timed out before finishing. Try a smaller scope."
 except FileNotFoundError:raise ValueError("Required server tool is not installed")
@app.get("/global")
def global_dashboard():return render_template("global.html")
@app.get("/tools")
def tools_page():return render_template("tools.html")
@app.get("/api/global/state")
def global_state():
 ts=[global_serial(x) for x in all_global_targets()];ok=sum(x["ok_count"] for x in ts);fail=sum(x["fail_count"] for x in ts)
 return jsonify({"targets":ts,"stats":{"total":len(ts),"online":sum(x["status"]=="ONLINE" for x in ts),"offline":sum(x["status"]=="OFFLINE" for x in ts),"checks":ok+fail,"availability":round(100*ok/(ok+fail),1) if ok+fail else None},"interval_seconds":1,"cycle_active":GLOBAL_RUN.locked()})
@app.post("/api/global/targets")
def global_add():
 try:
  t=clean_global(request.get_json(force=True) or {})
  with LOCK,connect() as db:
   existing=db.execute("SELECT id FROM global_targets WHERE lower(host)=lower(?) AND protocol=? AND port=?",(t["host"],t["protocol"],t["port"])).fetchone()
   if existing:raise ValueError("This endpoint already exists in Global Connectivity")
   position=db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM global_targets").fetchone()[0]
   result=db.execute("INSERT INTO global_targets(position,country,country_code,name,host,port,protocol,enabled) VALUES(?,?,?,?,?,?,?,?)",(position,t["country"],t["country_code"],t["name"],t["host"],t["port"],t["protocol"],t["enabled"]));db.commit()
  return jsonify({"id":result.lastrowid}),201
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.put("/api/global/targets/<int:tid>")
def global_edit(tid):
 try:
  t=clean_global(request.get_json(force=True) or {})
  with LOCK,connect() as db:
   if not db.execute("SELECT id FROM global_targets WHERE id=?",(tid,)).fetchone():raise ValueError("Global endpoint not found")
   existing=db.execute("SELECT id FROM global_targets WHERE lower(host)=lower(?) AND protocol=? AND port=? AND id<>?",(t["host"],t["protocol"],t["port"],tid)).fetchone()
   if existing:raise ValueError("This endpoint already exists in Global Connectivity")
   db.execute("UPDATE global_targets SET country=?,country_code=?,name=?,host=?,port=?,protocol=?,enabled=? WHERE id=?",(t["country"],t["country_code"],t["name"],t["host"],t["port"],t["protocol"],t["enabled"],tid));db.commit()
  return jsonify({"ok":True})
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.delete("/api/global/targets/<int:tid>")
def global_delete(tid):
 with LOCK,connect() as db:db.execute("DELETE FROM global_targets WHERE id=?",(tid,));db.commit()
 return jsonify({"ok":True})
@app.post("/api/tools/nslookup")
def tool_nslookup():
 try:return jsonify({"ok":True,"target":tool_host((request.get_json(force=True) or {}).get("host")),"output":tool_run(["/usr/bin/nslookup",tool_host((request.get_json(force=True) or {}).get("host"))],12)})
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.post("/api/tools/nmap")
def tool_nmap():
 try:
  data=request.get_json(force=True) or {};host=tool_host(data.get("host"));ports=str(data.get("ports","")).strip();scope=str(data.get("scope","top1000")).strip().lower()
  if ports and not re.fullmatch(r"\d{1,5}(?:-\d{1,5})?(?:,\d{1,5}(?:-\d{1,5})?){0,19}",ports):raise ValueError("Ports must be a list such as 80,443 or 1-1024")
  if ports:scope="custom"
  if scope not in {"web","top100","top1000","full","custom"}:raise ValueError("Invalid scan scope")
  if scope=="custom" and not ports:raise ValueError("Enter one or more ports for a custom scan")
  resolved=sorted({x[4][0] for x in socket.getaddrinfo(host,None,socket.AF_INET,socket.SOCK_STREAM)})
  scan_target=resolved[0] if resolved else host
  scope_args,scope_name,limit={
   "web":(["-p","80,443,8080,8443"],"web service ports",25),
   "top100":(["--top-ports","100"],"top 100 common TCP ports",35),
   "top1000":([],"Nmap default top 1000 TCP ports",55),
   "full":(["-p-"],"all 65,535 TCP ports",90),
   "custom":(["-p",ports],"custom TCP ports: "+ports,45),
  }[scope]
  args=["/usr/bin/nmap","-Pn","-sT","-T3","--max-retries","1","--host-timeout",str(limit)+"s"]+scope_args
  args.append(scan_target)
  output="Requested target: %s\nResolved IPv4: %s\nScan profile: TCP connect · %s\n\n%s"%(host,scan_target,scope_name,tool_run(args,limit+10))
  response=jsonify({"ok":True,"target":host,"resolved_address":scan_target,"scope":scope,"output":output});response.headers["Cache-Control"]="no-store";return response
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.post("/api/tools/speedtest")
def tool_speedtest():
 try:
  data=request.get_json(force=True) or {};url=speed_target(data.get('target',data.get('url')),data.get('scheme','http'));mode=str(data.get('mode','download')).lower()
  if mode not in {'download','upload','both'}:raise ValueError('Invalid speed test mode')
  size_mb=max(1,min(64,float(data.get('size_mb',4))));size_bytes=min(64*1024*1024,int(size_mb*1024*1024));timeout=max(3,min(30,float(data.get('timeout_seconds',15))))
  result=speed_test_v2(url,mode,size_bytes,timeout);response=jsonify({'ok':not result['errors'],**result});response.headers['Cache-Control']='no-store';return response
 except ValueError as error:return jsonify({'error':str(error)}),400
 try:
  data=request.get_json(force=True) or {};url=tool_url(data.get("url"));mode=str(data.get("mode","download")).lower()
  if mode not in {"download","upload","both"}:raise ValueError("Invalid speed test mode")
  size_mb=max(1,min(64,float(data.get("size_mb",4))))
  download_bytes=min(64*1024*1024,int(size_mb*1024*1024));upload_bytes=min(64*1024*1024,int(size_mb*1024*1024))
  timeout=max(3,min(30,float(data.get("timeout_seconds",15))))
  result=speed_test(url,mode,download_bytes,upload_bytes,timeout);response=jsonify({"ok":not result["errors"],**result});response.headers["Cache-Control"]="no-store";return response, (200 if not result["errors"] else 502)
 except (ValueError,TypeError) as e:return jsonify({"error":str(e)}),400
@app.get("/api/tools/status")
def tool_status():return jsonify({"nslookup":bool(shutil.which("nslookup")),"nmap":bool(shutil.which("nmap")),"speedtest":True,"note":"Commands use fixed safe arguments, byte limits, timeouts, and do not execute a user-provided shell command."})
@app.get("/")
def index():return render_template("index.html",protocols=list(PROTOCOLS))
@app.get("/api/state")
def state():
 ts=[serial(x) for x in all_targets()];ok=sum(x["ok_count"] for x in ts);fail=sum(x["fail_count"] for x in ts);return jsonify({"targets":ts,"settings":settings(),"rocket":rocket(),"runtime":runtime_view(),"stats":{"total":len(ts),"online":sum(x["status"]=="ONLINE" for x in ts),"offline":sum(x["status"]=="OFFLINE" for x in ts),"degraded":sum(x["status"]=="DEGRADED" for x in ts),"checks":ok+fail,"success":ok,"fail":fail,"availability":round(100*ok/(ok+fail),1) if ok+fail else None}})
@app.get("/api/dashboard")
def dashboard():
 try:sec=max(60,min(60*86400,int(request.args.get("range",3600))));ids=[int(x) for x in request.args.get("ids","").split(",") if x];filtered=request.args.get("filtered")=="1"
 except:return jsonify({"error":"Invalid dashboard filter"}),400
 rows,bucket=chart_samples(sec,ids) if (not filtered or ids) else ([],max(1,int(math.ceil(float(sec)/MAX_CHART_POINTS_PER_TARGET))))
 end=utc();return jsonify({"range":sec,"bucket_seconds":bucket,"window_start":(end-timedelta(seconds=sec)).isoformat().replace("+00:00","Z"),"window_end":end.isoformat().replace("+00:00","Z"),"samples":rows,"targets":[serial(x) for x in all_targets() if not filtered or x["id"] in ids]})
@app.post("/api/targets")
def add():
 try:
  t=clean(request.get_json(force=True) or {})
  with LOCK,connect() as db:
   if duplicate_target(db,t):raise ValueError("This Host / URL, protocol and port already exist")
   pos=db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM targets").fetchone()[0];x=db.execute("INSERT INTO targets(position,name,host,port,protocol,enabled,rocket_alert) VALUES(?,?,?,?,?,?,?)",(pos,t["name"],t["host"],t["port"],t["protocol"],t["enabled"],t["rocket_alert"]));db.commit();return jsonify({"id":x.lastrowid}),201
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.put("/api/targets/<int:tid>")
def edit(tid):
 try:
  t=clean(request.get_json(force=True) or {})
  with LOCK,connect() as db:
   if not db.execute("SELECT 1 FROM targets WHERE id=?",(tid,)).fetchone():return jsonify({"error":"Target not found"}),404
   if duplicate_target(db,t,tid):raise ValueError("This Host / URL, protocol and port already exist")
   db.execute("UPDATE targets SET name=?,host=?,port=?,protocol=?,enabled=?,rocket_alert=? WHERE id=?",(t["name"],t["host"],t["port"],t["protocol"],t["enabled"],t["rocket_alert"],tid));db.commit()
  return jsonify({"ok":True})
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.delete("/api/targets/<int:tid>")
def delete(tid):
 with LOCK,connect() as db:db.execute("DELETE FROM samples WHERE target_id=?",(tid,));db.execute("DELETE FROM targets WHERE id=?",(tid,));db.commit()
 return jsonify({"ok":True})
@app.delete("/api/targets")
def delete_all():
 with LOCK,connect() as db:
  db.execute("DELETE FROM samples");db.execute("DELETE FROM targets");db.commit()
 return jsonify({"ok":True})
@app.post("/api/targets/smart-sort")
def smart_sort_targets():
 with LOCK,connect() as db:
  count=smart_sort(db);db.commit()
 return jsonify({"ok":True,"count":count})
@app.post("/api/targets/reorder")
def reorder():
 ids=(request.get_json(force=True) or {}).get("ids",[])
 with LOCK,connect() as db:
  if sorted(ids)!=sorted(x[0] for x in db.execute("SELECT id FROM targets")):return jsonify({"error":"Target list changed; refresh and retry"}),409
  for i,x in enumerate(ids):db.execute("UPDATE targets SET position=? WHERE id=?",(i,x))
  db.commit()
 return jsonify({"ok":True})
@app.post("/api/import.csv")
def import_csv():
 f=request.files.get("file")
 if not f:return jsonify({"error":"Choose a CSV file"}),400
 try:rows=list(csv.DictReader(io.StringIO(f.read().decode("utf-8-sig"))))
 except:return jsonify({"error":"CSV must be UTF-8 encoded"}),400
 good=[];bad=[];seen=set()
 for n,r in enumerate(rows,2):
  try:
   t=clean({"name":r.get("Name") or r.get("name") or "","host":r.get("Host / URL") or r.get("Host") or r.get("host") or "","port":r.get("Port") or r.get("port") or "","protocol":r.get("Protocol") or r.get("protocol") or "PING","enabled":str(r.get("Enabled","true")).lower() not in {"0","false","no"},"rocket_alert":str(r.get("Rocket Alert","false")).lower() in {"1","true","yes"}})
   key=target_identity(t)
   if key in seen:bad.append("Row %d: duplicate target in CSV"%n)
   else:seen.add(key);good.append(t)
  except ValueError as e:bad.append("Row %d: %s"%(n,e))
 with LOCK,connect() as db:
  pos=db.execute("SELECT COALESCE(MAX(position),-1)+1 FROM targets").fetchone()[0]
  added=0
  for t in good:
   if duplicate_target(db,t):bad.append("%s: already exists"%t["host"]);continue
   db.execute("INSERT INTO targets(position,name,host,port,protocol,enabled,rocket_alert) VALUES(?,?,?,?,?,?,?)",(pos,t["name"],t["host"],t["port"],t["protocol"],t["enabled"],t["rocket_alert"]));pos+=1;added+=1
  db.commit()
 return jsonify({"imported":added,"errors":bad[:20]})
@app.post("/api/control/<action>")
def control(action):
 if action=="start":STATE.update(running=True,paused=False,started_at=stamp(),elapsed_base=0,last_resume_at=stamp(),last_hourly_report_at=stamp(),last_hourly_attempt_at=None)
 elif action=="pause" and STATE["running"]:STATE["elapsed_base"]=runtime_view()["elapsed_seconds"];STATE["paused"]=True;STATE["last_resume_at"]=None
 elif action=="resume" and STATE["running"]:STATE["paused"]=False;STATE["last_resume_at"]=stamp()
 elif action=="stop":
  elapsed=runtime_view()["elapsed_seconds"]
  STATE.update(running=False,paused=False,started_at=None,elapsed_base=elapsed,last_resume_at=None,last_hourly_report_at=None,last_hourly_attempt_at=None)
  with LOCK,connect() as db:
   rows=list(db.execute("SELECT * FROM targets ORDER BY position,id"));weak=weak_targets(rows);smart_sort(db)
   db.commit()
 elif action=="reset":
  STATE.update(running=False,paused=False,started_at=None,elapsed_base=0,last_resume_at=None,last_hourly_report_at=None,last_hourly_attempt_at=None)
  with LOCK,connect() as db:
   db.execute("UPDATE targets SET status='UNKNOWN',detail='',last_ms=NULL,ok_count=0,fail_count=0,consecutive_failures=0,consecutive_successes=0,updated_at=NULL")
   db.execute("DELETE FROM samples")
   db.commit()
 else:return jsonify({"error":"Unknown action"}),400
 return jsonify({"ok":True,"weak":weak if action=="stop" else []})
@app.put("/api/settings")
def set_settings():
 x=request.get_json(force=True) or {};s=settings()
 try:
  for k in s:
   if k in x:s[k]=x[k]
  s.update(interval=max(1,min(3600,int(s["interval"]))),timeout=max(250,min(60000,int(s["timeout"]))),workers=max(1,min(128,int(s["workers"]))),failure_threshold=max(1,min(20,int(s["failure_threshold"]))),recovery_threshold=max(1,min(20,int(s["recovery_threshold"]))))
  s["dns_domain"]=str(s["dns_domain"]).strip()
  if not HOST.match(s["dns_domain"]):raise ValueError("Invalid DNS test domain")
 except Exception as e:return jsonify({"error":str(e)}),400
 with LOCK,connect() as db:
  for k,v in s.items():db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",(k,str(v)))
  db.commit()
 return jsonify({"ok":True,"settings":s})
@app.get("/api/rocket/config")
def rocket_get():return jsonify(rocket())
@app.put("/api/rocket/config")
def rocket_put():
 try:return jsonify({"ok":True,"rocket":save_rocket(request.get_json(force=True) or {})})
 except ValueError as e:return jsonify({"error":str(e)}),400
@app.post("/api/rocket/test")
def rocket_test():
 try:
  incoming=request.get_json(silent=True) or {};c=rocket(True)
  for key in ("base_url","username","user_id","room_ids","channels","recipients"):
   if key in incoming:c[key]=str(incoming[key]).strip()
  for key in ("password","auth_token"):
   if incoming.get(key) not in {None,"","••••••"}:c[key]=str(incoming[key])
  c["base_url"]=c["base_url"].rstrip("/");h=rocket_auth(c);x=req_json(c["base_url"]+"/api/v1/me",headers=h)
  if not x.get("success"):raise RuntimeError("Rocket.Chat authentication failed")
  configured=bool(destinations(c.get("room_ids"))+destinations(c.get("channels"))+destinations(c.get("recipients")))
  count=len(rooms(c,h)) if configured else 0
  return jsonify({"ok":True,"message":"Rocket.Chat connection is valid"+("; %d destination(s) resolved"%count if configured else "; no destinations configured yet")})
 except Exception as e:return jsonify({"error":str(e)}),400
@app.post("/api/reports")
def report():
 d=request.get_json(force=True) or {}
 try:sec=max(60,min(60*86400,int(d.get("range",3600))));ids=[int(x) for x in d.get("ids",[])]
 except:return jsonify({"error":"Invalid report options"}),400
 html=report_html(sec,ids)
 if d.get("mode")=="rocket":
  try:rocket_send("📊 **PingPilot monitoring report**\nRange: last %d minutes · Targets: %d\nThe HTML report is attached."%(sec//60,len(ids) or len(all_targets())),html);return jsonify({"ok":True,"message":"HTML report sent to Rocket.Chat"})
  except Exception as e:return jsonify({"error":str(e)}),400
 return Response(html,mimetype="text/html",headers={"Content-Disposition":"attachment; filename=pingpilot-report.html"})
@app.get("/api/export.csv")
def export():
 o=io.StringIO();w=csv.writer(o);w.writerow(["Name","Host / URL","Port","Protocol","Enabled","Rocket Alert","Status","Detail","OK","Fail","Availability","Last latency (ms)","Updated"])
 for t in map(serial,all_targets()):w.writerow([t["name"],t["host"],t["port"],t["protocol"],t["enabled"],t["rocket_alert"],t["status"],t["detail"],t["ok_count"],t["fail_count"],t["availability"],t["last_ms"],t["updated_at"]])
 return Response(o.getvalue(),mimetype="text/csv",headers={"Content-Disposition":"attachment; filename=pingpilot-export.csv"})
init();start_background_services()
if __name__=="__main__":app.run(host="127.0.0.1",port=8219)
