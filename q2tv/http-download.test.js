const test=require('node:test'),assert=require('node:assert/strict'),http=require('node:http');
const {Readable}=require('node:stream'),fs=require('node:fs'),vm=require('node:vm');
const {createHTTPDownloads,publicIPv4,resourcePath,baseURL}=require('./http-download');
const packet=text=>Buffer.concat([Buffer.from([255,255,255,255]),Buffer.from(text)]);
test('HTTP resources require an advertised public base, strict resource paths, and pinned public IPv4',()=>{
  for(const address of ['127.0.0.1','10.1.2.3','172.16.0.2','192.168.2.1','169.254.169.254','100.64.0.1','0.0.0.0','::1','::ffff:127.0.0.1','224.0.0.1'])assert.equal(publicIPv4(address),false,address);
  assert.equal(publicIPv4('91.227.18.67'),true);
  for(const file of ['../config.cfg','maps/../../secret.bsp','maps/test.cfg','/maps/test.bsp','maps/test.bsp?x','maps/a%2fb.bsp','maps/.env'])assert.equal(resourcePath(file),false,file);
  assert.equal(resourcePath('textures/e1u1/+0metal.wal'),true);
  for(const base of ['file:///x','http://127.0.0.1/','https://user:pass@example.com/','http://[::1]/'])assert.throws(()=>baseURL(base));
});
test('download uses advertised game-dir URL and legacy fallback; absence, 404, expiry, invalid input do not fetch',async()=>{
  let clock=0;const requests=[];
  const downloads=createHTTPDownloads({now:()=>clock,request:async url=>{requests.push(String(url));const r=Readable.from([Buffer.from('IBSPtest')]);r.headers={};r.statusCode=url.pathname==='/files/maps/test.bsp'?200:404;return r;}});
  const server=http.createServer(downloads.handle);await new Promise(r=>server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+server.address().port;
  const get=async path=>{const r=await fetch(base+path);return {status:r.status,text:await r.text()};};
  const url='/?server=91.227.18.67:27950&game=opentdm-x&path=maps/test.bsp';
  try{
    assert.equal((await get(url)).status,404);assert.equal(requests.length,0);
    downloads.observe('91.227.18.67:27950',packet('client_connect dlserver=http://downloads.example/files/\n'));
    assert.deepEqual(await get(url),{status:200,text:'IBSPtest'});
    assert.deepEqual(requests,['http://downloads.example/files/opentdm-x/maps/test.bsp','http://downloads.example/files/maps/test.bsp']);
    assert.equal((await get(url.replace('maps/test.bsp','../secret.cfg'))).status,400);
    assert.equal((await get(url.replace('test.bsp','missing.bsp'))).status,404);
    clock=7*3600000;assert.equal((await get(url)).status,404);
    downloads.observe('91.227.18.67:27950',packet('client_connect\n'));assert.equal((await get(url)).status,404);
  }finally{await new Promise(r=>server.close(r));}
});
test('two HTTP transfers, demand joins prefetch, missing file remains a UDP fallback, cancellation writes nothing',async()=>{
  const sandbox={window:{},Module:{arguments:[]},AbortController,AbortSignal,URL,URLSearchParams,location:{href:'https://example.com/q2tv/?s=eA=='}};
  vm.runInNewContext(fs.readFileSync(__dirname+'/http-client.js','utf8'),sandbox);
  const releases=[];let active=0,max=0;const writes=[];
  sandbox.window.q2tvDownloadFile=async(file,_game,signal)=>{active++;max=Math.max(max,active);await new Promise(r=>releases.push(r));active--;if(file==='missing'){const e=Error('404');e.fileMissing=true;throw e;}if(signal.aborted)throw Error('aborted');return new Uint8Array([1]);};
  const queue=sandbox.window.q2tvCreateDownloadQueue((path)=>writes.push(path));
  const one=queue.add('a','baseq2','a'),two=queue.add('missing','baseq2','missing'),three=queue.add('c','baseq2','c');
  assert.equal(queue.add('a','baseq2','a',true),one);
  await new Promise(setImmediate);assert.equal(active,2);releases.shift()();releases.shift()();await Promise.allSettled([one,two]);await new Promise(setImmediate);assert.equal(max,2);
  releases.shift()();await three;assert.deepEqual(writes,['a','c']);
  const pending=queue.add('late','baseq2','late');await new Promise(setImmediate);queue.cancel();releases.shift()();await assert.rejects(pending);assert.deepEqual(writes,['a','c']);
  assert.equal(sandbox.Module.arguments.at(-1),'250000');
});
