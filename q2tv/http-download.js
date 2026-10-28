'use strict';
// Only resources from an HTTP base actually advertised during a Q2TV game
// connection are reachable. The browser cannot submit a download server URL.
const http=require('node:http'),https=require('node:https'),dns=require('node:dns/promises');
const {isIP}=require('node:net');
const {Transform}=require('node:stream');
const {pipeline}=require('node:stream/promises');
const MAX_BYTES=64*1024*1024;
function publicIPv4(address){
  if(isIP(address)!==4)return false;
  const [a,b,c]=address.split('.').map(Number);
  return !(a===0||a===10||a===127||a>=224||(a===100&&b>=64&&b<=127)||(a===169&&b===254)||(a===172&&b>=16&&b<=31)||(a===192&&(b===168||b===0||b===2))||(a===198&&(b===18||b===19||(b===51&&c===100)))||(a===203&&b===0&&c===113));
}
function resourcePath(value){
  return typeof value==='string'&&value.length<256&&!value.includes('..')&&/^(?:maps|models|players|sound|textures|pics|env)\/(?:[a-zA-Z0-9_+!~-]+\/)*[a-zA-Z0-9_+!~.-]+\.(?:bsp|md2|md3|sp2|pcx|wal|tga|png|jpg|jpeg|wav|ogg)$/i.test(value);
}
function baseURL(value){
  const url=new URL(value);
  if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.search||url.hash||url.hostname.includes(':'))throw Error('Invalid HTTP download base');
  if(isIP(url.hostname)&&!publicIPv4(url.hostname))throw Error('Private download host');
  if(!url.pathname.endsWith('/'))url.pathname+='/';
  return url;
}
async function requestPublic(url,signal,redirects=0){
  const checked=baseURL(url.origin); // Reject credentials/protocol/private literals before DNS.
  if(url.username||url.password||url.hash)throw Error('Invalid resource URL');
  const addresses=isIP(checked.hostname)?[{address:checked.hostname,family:4}]:await dns.lookup(checked.hostname,{all:true,family:4});
  if(!addresses.length||addresses.some(a=>!publicIPv4(a.address)))throw Error('Non-public download host');
  const address=addresses[0].address;
  const response=await new Promise((resolve,reject)=>{
    // Pin the validated DNS result: a second lookup cannot rebind to a LAN host.
    const req=(url.protocol==='https:'?https:http).get(url,{signal,headers:{'User-Agent':'Q2TV-HTTP/1','Accept-Encoding':'identity'},lookup:(_host,options,cb)=>options.all?cb(null,[{address,family:4}]):cb(null,address,4)},resolve);
    req.on('error',reject);
  });
  if([301,302,303,307,308].includes(response.statusCode)){
    response.resume();if(redirects>=3||!response.headers.location)throw Error('Redirect limit');
    return requestPublic(new URL(response.headers.location,url),signal,redirects+1);
  }
  return response;
}
function createHTTPDownloads({request=requestPublic,now=Date.now}={}){
  const advertised=new Map(),clients=new Map();let active=0;
  function observe(target,packet){
    if(packet.length>4096||packet.length<5||packet.readUInt32LE(0)!==0xffffffff)return;
    const text=packet.subarray(4).toString('latin1');
    if(!/^client_connect(?:\s|$)/.test(text))return;
    advertised.delete(target);
    const found=text.match(/(?:^|\s)dlserver=(?:"([^"\s]+)"|([^\s]+))/);
    if(!found)return;
    try{const base=baseURL(found[1]||found[2]);if(advertised.size>=256)advertised.delete(advertised.keys().next().value);advertised.set(target,{base,at:now()});}catch{}
  }
  async function handle(req,res){
    const url=new URL(req.url,'http://localhost'),target=url.searchParams.get('server'),file=url.searchParams.get('path'),game=url.searchParams.get('game')||'baseq2';
    const entry=advertised.get(target);
    if(!resourcePath(file)||!/^[-a-zA-Z0-9_]{1,64}$/.test(game)){res.writeHead(400);res.end();return;}
    if(!entry||now()-entry.at>6*3600000){res.writeHead(404);res.end();return;}
    const client=req.socket.remoteAddress||'unknown';
    if(active>=16||(clients.get(client)||0)>=4){res.writeHead(429);res.end();return;}
    active++;clients.set(client,(clients.get(client)||0)+1);
    const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),20000);
    res.on('close',()=>abort.abort());
    try{
      // Q2PRO game-dir URL, legacy R1Q2 root, then shared baseq2 assets.
      const paths=[game+'/'+file,file,...(game==='baseq2'?[]:['baseq2/'+file])];let response;
      for(const relative of paths){
        response=await request(new URL(relative,entry.base),abort.signal);
        if(response.statusCode!==404)break;
        response.resume();response=null;
      }
      if(!response){res.writeHead(404);res.end();return;}
      if(response.statusCode!==200||/text\/html/i.test(response.headers['content-type']||'')||Number(response.headers['content-length'])>MAX_BYTES){response.destroy();res.writeHead(502);res.end();return;}
      let bytes=0;const limit=new Transform({transform(chunk,_enc,cb){bytes+=chunk.length;cb(bytes>MAX_BYTES?Error('Resource too large'):null,chunk);}});
      res.writeHead(200,{'Content-Type':'application/octet-stream','Cache-Control':'private, max-age=3600','Cross-Origin-Resource-Policy':'same-origin','X-Content-Type-Options':'nosniff'});
      await pipeline(response,limit,res);
    }catch{if(!res.headersSent){res.writeHead(502);res.end();}else res.destroy();}
    finally{clearTimeout(timer);active--;const count=clients.get(client)-1;if(count)clients.set(client,count);else clients.delete(client);}
  }
  return {observe,handle};
}
module.exports={createHTTPDownloads,publicIPv4,resourcePath,baseURL};
