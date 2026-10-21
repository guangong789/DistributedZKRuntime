"""Non-cryptographic fault peer; Python is the actual identified executable."""
import json,os,signal,socket,sys,time
mode=sys.argv[1]
if mode=='preexit':sys.exit(7)
s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(2);s.connect(os.environ['DZKR_MIMCCHAIN_BARRIER_SOCKET'])
hello=dict(version=1,event='ready',pid=os.getpid(),role='probe',token=os.environ['DZKR_MIMCCHAIN_BARRIER_TOKEN'])
if mode=='badpid':hello['pid']+=1
if mode=='broken':s.close();sys.exit(8)
s.sendall(json.dumps(hello).encode()+b'\n')
f=s.makefile('rb');release=json.loads(f.readline());assert release==dict(version=1,event='release')
s.sendall(json.dumps(dict(version=1,event='released',pid=os.getpid(),role='probe')).encode()+b'\n')
print('PEER_WORK_AFTER_RELEASE',flush=True)
if mode=='hold-fork':
 signal.signal(signal.SIGTERM,signal.SIG_IGN)
 pid=os.fork()
 if pid==0:
  while True:signal.pause()
 while True:signal.pause()
if mode=='exit-nonzero':sys.exit(7)
if mode=='hold':
 while True:signal.pause()
