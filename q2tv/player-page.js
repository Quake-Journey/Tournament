'use strict';
const {assetManifest}=require('./asset-manifest');
async function playerPage(html,webroot){
  html=html.replace(/Q2TV alpha 0\.1/g,'Q2TV 0.2 beta').replace(/<title>[^<]*<\/title>/i,'<title>Q2TV 0.2 beta</title>').replace(/<strong>Qwasm2<\/strong>/g,'<strong>Q2TV 0.2 beta</strong>');
  const manifest=await assetManifest(webroot),version=name=>manifest.files[name].sha256;
  const bridge='<script>window.q2tvAssets='+JSON.stringify(manifest)+';</script>'+
    ['asset-cache.js','http-client.js','viewer-only.js'].map(name=>`<script src="${name}?v=${version(name)}"></script>`).join('');
  const pattern=/<script\b[^>]*\bsrc\s*=\s*(?:"(?:quake2|index)\.js(?:\?[^"<>]*)?"|'(?:quake2|index)\.js(?:\?[^'<>]*)?'|(?:quake2|index)\.js(?:\?[^\s<>]*)?)[^>]*>\s*<\/script>/i;
  if(pattern.test(html))return html.replace(pattern,bridge+`<script>q2tvLoadEngine('${manifest.engine}.js');</script>`);
  // The deployed desktop shell creates its engine script dynamically and on
  // mobile preloads the data file with fetch before starting WASM. Preserve
  // this path, letting the cache resolve each file's own content version.
  let found=false;
  const rendered=html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,tag=>{
    if(!/\.src\s*=\s*['"]quake2\.js(?:\?[^'"]*)?['"]/.test(tag))return tag;
    const loader=/function\s+loadQuake2JS\(\)\s*\{[^{}]*\.src\s*=\s*['"]quake2\.js(?:\?[^'"]*)?['"][^{}]*\}/;
    if(!loader.test(tag))throw Error('Unrecognized Q2TV desktop loader');
    found=true;
    return bridge+tag.replace(loader,"function loadQuake2JS(){ q2tvLoadEngine('quake2.js'); }")
      .replace(/(\bQ2_DATA_SIZE\s*=\s*)\d+/,(_,prefix)=>prefix+manifest.files['quake2.data'].size);
  });
  if(!found)throw Error('Q2TV engine script not found');
  return rendered;
}
module.exports={playerPage};
