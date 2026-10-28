'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const known=new Map();
const helpers=['asset-cache.js','http-client.js','viewer-only.js'];
function stamp(s){return [s.size,s.mtimeMs,s.ctimeMs,s.ino].join(':');}
async function assetInfo(file){
  const before=await fs.promises.stat(file),key=stamp(before),old=known.get(file);
  if(old?.stamp===key)return old.promise;
  const promise=(async()=>{
    const hash=crypto.createHash('sha256');
    for await(const chunk of fs.createReadStream(file))hash.update(chunk);
    if(stamp(await fs.promises.stat(file))!==key)throw Error('Q2TV asset changed while hashing');
    return {sha256:hash.digest('hex'),size:before.size,modified:before.mtime.toUTCString()};
  })();
  known.set(file,{stamp:key,promise});
  try{return await promise;}catch(error){if(known.get(file)?.promise===promise)known.delete(file);throw error;}
}
async function assetManifest(webroot){
  const engine=fs.existsSync(path.join(webroot,'quake2.js'))?'quake2':'index';
  const names=(await fs.promises.readdir(webroot,{withFileTypes:true})).filter(e=>e.isFile()&&/^[a-z0-9_.-]+\.(?:js|wasm|data)$/i.test(e.name)&&!helpers.includes(e.name)).map(e=>e.name);
  for(const ext of ['.js','.wasm','.data'])if(!names.includes(engine+ext))throw Error('Missing Q2TV asset: '+engine+ext);
  const files={};
  // One large-file read at a time; no whole data package is buffered in Node.
  for(const name of names.sort())files[name]=await assetInfo(path.join(webroot,name));
  for(const name of helpers)files[name]=await assetInfo(path.join(__dirname,name));
  return {schema:1,engine,files};
}
async function assetHeaders(req,res,file){
  const info=await assetInfo(file),requested=req.query?.v;
  // A page opened before deployment must not save new bytes under an old hash.
  if(/^[a-f0-9]{64}$/.test(requested||'')&&requested!==info.sha256){res.setHeader('Cache-Control','no-store');res.status(409).end('Q2TV updated; reload the player');return false;}
  const etag='"'+info.sha256+'"';
  res.setHeader('ETag',etag);res.setHeader('Last-Modified',info.modified);
  res.setHeader('X-Q2TV-Content-Hash',info.sha256);
  res.setHeader('Cache-Control',requested===info.sha256?'public, max-age=31536000, immutable':'no-cache');
  const tags=String(req.headers['if-none-match']||'').split(',').map(s=>s.trim().replace(/^W\//,''));
  if(tags.includes(etag)||tags.includes('*')){res.status(304).end();return false;}
  return true;
}
module.exports={assetInfo,assetManifest,assetHeaders};
