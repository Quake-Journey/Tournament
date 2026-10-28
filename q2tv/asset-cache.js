// Persistent, content-addressed resources shared by the two Q2TV loaders.
// Only the manifest's static files are cached; status, RCON and game traffic
// never enter this store. No service worker controls the surrounding website.
(()=>{
  'use strict';
  const manifest=window.q2tvAssets,originalFetch=window.fetch.bind(window),base=new URL('.',location.href);
  const cacheName='q2tv-assets-v1',pending=new Map();
  const state=window.q2tvCacheState={hits:0,downloads:0,networkBytes:0,storedBytes:0,pendingWrites:0,unavailable:false,files:{}};
  const ru=(navigator.language||'').toLowerCase().startsWith('ru');
  const label=document.createElement('div');label.id='q2tv-cache-status';label.setAttribute('role','status');
  label.style.cssText='position:fixed;bottom:48px;left:50%;transform:translateX(-50%);max-width:calc(100% - 32px);box-sizing:border-box;padding:8px 12px;border-radius:8px;background:#101827ee;color:#e7edf7;font:14px/1.4 system-ui;text-align:center;z-index:10000;pointer-events:none';
  document.body.append(label);
  let initialized=false,lastLabel=0;
  function show(message){if(initialized)return;label.hidden=false;label.textContent=message;}
  function unavailable(){state.unavailable=true;show(ru?'Кэш недоступен — обычная загрузка':'Cache unavailable — loading normally');}
  const storage=(async()=>{try{if(!window.caches)throw Error('No Cache API');return await caches.open(cacheName);}catch{unavailable();return null;}})();
  function assetURL(file,prefix){
    // An inline engine script still resolves resources from its player URL.
    const url=new URL((prefix||'')+file,base),name=url.pathname.slice(base.pathname.length);
    if(url.origin===base.origin&&manifest.files[name])url.search='?v='+manifest.files[name].sha256;
    return url.href;
  }
  window.q2tvAssetURL=assetURL;Module.locateFile=assetURL;
  const previous=Module.onRuntimeInitialized;
  Module.onRuntimeInitialized=function(...args){initialized=true;label.hidden=true;return previous?.apply(this,args);};
  function keyFor(name,info){return new URL('/__q2tv_cache__/'+encodeURIComponent(name)+'?v='+info.sha256,base.origin).href;}
  async function prune(cache,key){
    const pathname=new URL(key).pathname;
    for(const saved of await cache.keys())if(new URL(saved.url).pathname===pathname&&saved.url!==key)await cache.delete(saved);
  }
  async function fetchAsset(name,options){
    const info=manifest.files[name],key=keyFor(name,info),cache=await storage;
    if(cache){try{
      const saved=await cache.match(key);
      if(saved&&saved.headers.get('X-Q2TV-Content-Hash')===info.sha256&&Number(saved.headers.get('Content-Length'))===info.size){
        state.hits++;state.files[name]='cache';show(ru?'Чтение сохранённых ресурсов':'Loading saved resources');return saved;
      }
      if(saved)await cache.delete(key);
    }catch{unavailable();}}
    // Concurrent requests wait for the disk write, then open their own stream.
    // Large response bodies are never cloned into a second buffer in JS.
    if(cache&&pending.has(key)){await pending.get(key);const saved=await cache.match(key).catch(()=>null);if(saved){state.hits++;state.files[name]='cache';return saved;}}
    state.downloads++;state.files[name]='network';
    show(ru?'Скачивание ресурсов…':'Downloading resources…');
    const response=await originalFetch(assetURL(name),options);
    if(!response.ok)return response;
    if(response.headers.get('X-Q2TV-Content-Hash')!==info.sha256)throw Error('Q2TV resource version changed; reload the player');
    if(!response.body)return response;
    const headers=new Headers(response.headers);headers.delete('Content-Encoding');headers.delete('Transfer-Encoding');headers.set('Content-Length',String(info.size));
    let writer,writing,finishPending,received=0;
    if(cache){
      const stream=new TransformStream();writer=stream.writable.getWriter();
      state.pendingWrites++;
      pending.set(key,new Promise(resolve=>{finishPending=resolve;}));
      writing=cache.put(key,new Response(stream.readable,{status:200,headers})).then(()=>{
        state.storedBytes+=info.size;return prune(cache,key).catch(()=>{});
      }).catch(()=>{unavailable();writer?.abort().catch(()=>{});}).finally(()=>{state.pendingWrites--;pending.delete(key);finishPending();});
    }
    const reader=response.body.getReader();
    const body=new ReadableStream({
      async pull(controller){
        try{
          const {done,value}=await reader.read();
          if(done){
            if(received!==info.size)throw Error('Incomplete Q2TV resource');
            if(writer){try{await writer.close();}catch{}await writing;}
            controller.close();return;
          }
          received+=value.byteLength;state.networkBytes+=value.byteLength;
          if(received>info.size)throw Error('Invalid Q2TV resource size');
          // Disk backpressure bounds the extra JS buffer to one chunk. If
          // storage fails, continue this network stream without re-downloading.
          if(writer){try{await writer.write(value);}catch{writer=null;unavailable();}}
          if(Date.now()-lastLabel>200){lastLabel=Date.now();if(!state.unavailable)show((ru?'Скачивание: ':'Downloading: ')+Math.round(received/1048576)+' / '+Math.round(info.size/1048576)+(ru?' МиБ':' MiB'));}
          controller.enqueue(value);
        }catch(error){if(writer)await writer.abort(error).catch(()=>{});controller.error(error);reader.cancel(error).catch(()=>{});}
      },
      async cancel(reason){if(writer)await writer.abort(reason).catch(()=>{});await reader.cancel(reason);}
    });
    return new Response(body,{status:response.status,statusText:response.statusText,headers});
  }
  window.fetch=function(input,options){
    const url=new URL(typeof input==='string'||input instanceof URL?input:input.url,location.href);
    const name=url.pathname.startsWith(base.pathname)?url.pathname.slice(base.pathname.length):'';
    const method=(options?.method||input?.method||'GET').toUpperCase();
    if(method!=='GET'||url.origin!==base.origin||!manifest.files[name]||new Headers(options?.headers||input?.headers).has('Range'))return originalFetch(input,options);
    return fetchAsset(name,options);
  };
  let engineStarted=false;
  window.q2tvLoadEngine=async function(name){
    if(engineStarted)return;engineStarted=true;
    try{
      const response=await window.fetch(assetURL(name));if(!response.ok)throw Error('Q2TV engine HTTP '+response.status);
      const script=document.createElement('script');script.textContent=await response.text()+'\n//# sourceURL='+assetURL(name);document.body.append(script);
    }catch(error){state.error=String(error.message);show(ru?'Не удалось загрузить Q2TV. Обнови страницу.':'Could not load Q2TV. Reload the page.');console.error('[q2tv-cache]',error);}
  };
})();
