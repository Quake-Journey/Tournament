const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
test('player bootstrap precedes engine and changing a helper does not change native asset versions',async()=>{
  let stamp=1;const module={exports:{}};
  const native='a'.repeat(64),assetManifest=async root=>({schema:1,engine:root==='desktop'?'quake2':'index',files:Object.fromEntries(['quake2.js','quake2.wasm','quake2.data','index.js','index.data','index.wasm','asset-cache.js','http-client.js','viewer-only.js'].map(n=>[n,{sha256:n==='viewer-only.js'?String(stamp).repeat(64):native,size:1}]))});
  vm.runInNewContext(fs.readFileSync(__dirname+'/player-page.js','utf8'),{require:()=>({assetManifest}),module,__dirname});
  const {playerPage}=module.exports;
  for(const [root,tag] of [['desktop','<script async src="quake2.js"></script>'],['mobile','<script async src=index.js></script>']]){
    const html='<html><body><script>var Module={};</script>'+tag+'<div>controls</div></body></html>',first=await playerPage(html,root);
    assert.ok(first.indexOf('var Module')<first.indexOf('window.q2tvAssets'));
    assert.ok(first.indexOf('src="viewer-only.js')<first.indexOf('q2tvLoadEngine'));
    assert.match(first,/asset-cache\.js\?v=[a-f0-9]{64}/);
    assert.equal(await playerPage(html,root),first);stamp++;const updated=await playerPage(html,root);assert.notEqual(updated,first);
    const manifest=JSON.parse(updated.match(/window.q2tvAssets=([^<]+);<\/script>/)[1]);assert.equal(manifest.files['quake2.data'].sha256,native);
  }
  const dynamic='<script>var Module={};</script><script>var Q2_DATA_SIZE=42;function loadQuake2JS(){var s=document.createElement("script");s.src="quake2.js";document.body.append(s);}fetch("quake2.data").then(loadQuake2JS);</script>';
  const rendered=await playerPage(dynamic,'desktop');
  assert.ok(rendered.indexOf('var Module')<rendered.indexOf('window.q2tvAssets'));
  assert.ok(rendered.indexOf('src="viewer-only.js')<rendered.indexOf('var Q2_DATA_SIZE'));
  assert.match(rendered,/Q2_DATA_SIZE=1;/);
  assert.ok(rendered.includes("q2tvLoadEngine('quake2.js')"));
  assert.ok(rendered.includes('fetch("quake2.data")'));
  await assert.rejects(()=>playerPage('<html>no engine</html>','desktop'));
  for(const root of ['desktop','mobile']){
    const html='<title>Q2TV alpha 0.1</title><body><strong>Qwasm2</strong><script>var Module={};</script><script async src="index.js"></script></body>';
    const rendered=await playerPage(html,root);assert.match(rendered,/<title>Q2TV 0\.2 beta<\/title>/);assert.ok(!rendered.includes('alpha 0.1'));assert.match(rendered,/<strong>Q2TV 0\.2 beta<\/strong>/);
  }
});
