const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),path=require('node:path'),os=require('node:os');
const {assetInfo,assetHeaders}=require('./asset-manifest');
test('content identity survives timestamp changes; changed bytes invalidate and stale pages are rejected',async()=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'q2tv-cache-')),file=path.join(root,'test.data');
  await fs.writeFile(file,'first');const first=await assetInfo(file);
  await fs.utimes(file,new Date(),new Date(Date.now()+2000));assert.equal((await assetInfo(file)).sha256,first.sha256);
  await fs.writeFile(file,'other');const next=await assetInfo(file);assert.notEqual(next.sha256,first.sha256);
  function response(){return {headers:{},setHeader(k,v){this.headers[k]=v;},status(code){this.code=code;return this;},end(text){this.body=text;}};}
  let res=response();assert.equal(await assetHeaders({query:{v:first.sha256},headers:{}},res,file),false);assert.equal(res.code,409);
  res=response();assert.equal(await assetHeaders({query:{v:next.sha256},headers:{'if-none-match':'W/"'+next.sha256+'"'}},res,file),false);assert.equal(res.code,304);
  res=response();assert.equal(await assetHeaders({query:{v:next.sha256},headers:{}},res,file),true);assert.match(res.headers['Cache-Control'],/immutable/);
  await fs.unlink(file);await fs.rmdir(root);
});
