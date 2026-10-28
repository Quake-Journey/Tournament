// Shared by both native engines. The same-origin Tournament endpoint uses only
// the HTTP base advertised by the connected game server, avoiding mixed content
// and CORS restrictions without allowing an arbitrary URL in the browser.
window.q2tvDownloadFile=async function(file,game,signal){
  const page=new URL(location.href);let server=page.searchParams.get('server');
  try{if(page.searchParams.has('s'))server=atob(page.searchParams.get('s'));}catch{}
  if(!server)throw Error('No game server');
  const endpoint=new URL('http-file',location.href);
  endpoint.search=new URLSearchParams({server,path:file,game:game||'baseq2'});
  const response=await fetch(endpoint,{signal:AbortSignal.any([signal,AbortSignal.timeout(25000)]),credentials:'same-origin'});
  if(!response.ok){const error=Error('HTTP '+response.status);error.fileMissing=response.status===404||response.status===400;throw error;}
  const bytes=new Uint8Array(await response.arrayBuffer());
  if(!bytes.length||bytes.length>64*1024*1024)throw Error('Invalid resource size');
  const tag=String.fromCharCode(...bytes.subarray(0,4));
  if((file.endsWith('.bsp')&&tag!=='IBSP')||(file.endsWith('.md2')&&tag!=='IDP2')||(file.endsWith('.wav')&&tag!=='RIFF'))throw Error('Invalid resource format');
  return bytes;
};

// One demand request plus speculative resources already listed by the game
// server. At most two transfers run, including a demand that joins a prefetch.
window.q2tvCreateDownloadQueue=function(writeFile){
  const jobs=new Map(),waiting=[],abort=new AbortController();let active=0,failed=false;
  function pump(){
    while(active<2&&waiting.length){
      const job=waiting.shift();active++;
      Promise.resolve().then(()=>{
        if(failed||abort.signal.aborted)throw Error('HTTP unavailable');
        return window.q2tvDownloadFile(job.file,job.game,abort.signal);
      }).then(bytes=>{
        if(abort.signal.aborted)throw Error('Cancelled');
        writeFile(job.destination,bytes);job.resolve();
      }).catch(error=>{if(!error.fileMissing)failed=true;job.reject(error);}).finally(()=>{active--;pump();});
    }
  }
  return {
    add(file,game,destination,priority=false){
      const key=game+'/'+file;
      if(jobs.has(key))return jobs.get(key).promise;
      let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});
      // Background failures are deliberately consumed; the same promise is
      // awaited again when native precache reaches that file and starts UDP.
      promise.catch(()=>{});
      const job={file,game,destination,promise,resolve,reject};jobs.set(key,job);
      if(priority)waiting.unshift(job);else waiting.push(job);pump();return promise;
    },
    cancel(){abort.abort();for(const job of waiting.splice(0))job.reject(Error('Cancelled'));jobs.clear();},
  };
};

// Apply after saved configs as an explicit launch setting on both players.
if(typeof Module!=='undefined')Module.arguments=[...(Module.arguments||[]),'+set','rate','250000'];
