// Shared presentation for both engines; the authoritative restriction lives in
// spectator-policy.js at the Tournament gateway and also covers console/menu use.
(()=>{
  let lastExit=0,ready=false,failed=false;const state=window.q2tvViewerState={status:0,autoExits:0};
  const sockets=new Set(),NativeSocket=window.WebSocket;
  if(NativeSocket)window.WebSocket=class extends NativeSocket{constructor(...args){super(...args);sockets.add(this);this.addEventListener('close',()=>sockets.delete(this),{once:true});}};
  function closeSockets(){for(const socket of sockets){try{socket.close();}catch{}}sockets.clear();}
  if(typeof Module!=='undefined'){
    const initialized=Module.onRuntimeInitialized;
    Module.onRuntimeInitialized=function(...args){ready=true;return initialized?.apply(this,args);};
    const aborted=Module.onAbort;
    Module.onAbort=function(...args){failed=true;ready=false;closeSockets();state.status=0;state.error='engine-aborted';return aborted?.apply(this,args);};
  }
  const timer=setInterval(()=>{
    if(!ready||typeof Module==='undefined'||typeof Module._Q2TV_ViewState!=='function')return;
    state.status=Module._Q2TV_ViewState();
    if(state.status===1&&Date.now()-lastExit>=1000){lastExit=Date.now();Module._Q2TV_Observe();}
    if(typeof Module._Q2TV_AutoExits==='function')state.autoExits=Module._Q2TV_AutoExits();
  },100);
  window.q2tvClose=()=>{clearInterval(timer);titleObserver?.disconnect();try{if(ready&&!failed&&typeof Module!=='undefined'&&typeof Module._Q2TV_Stop==='function')Module._Q2TV_Stop();}finally{ready=false;closeSockets();}};
  window.addEventListener('pagehide',window.q2tvClose,{once:true});
  const ru=(navigator.language||'').toLowerCase().startsWith('ru');
  const version='Q2TV 0.2 beta';document.title=version;
  // SDL renderer initialization also changes the window title. Keep the TV
  // product version consistent without changing upstream renderer identities.
  const titleObserver=typeof MutationObserver==='function'?new MutationObserver(()=>{if(document.title!==version)document.title=version;}):null;
  titleObserver?.observe(document.head,{subtree:true,childList:true,characterData:true});
  const label=document.createElement('div');label.textContent=version+(ru?' · Только просмотр':' · Spectator only');
  label.style.cssText='position:fixed;right:10px;bottom:10px;z-index:9998;border-radius:7px;padding:5px 9px;background:#101827dd;color:#dbe9ff;font:12px/1.4 system-ui;pointer-events:none';
  document.body.append(label);
  // The original fixed 28px controls row clips its text/buttons under browser
  // scaling. Give the row its natural height and keep the canvas in its own box.
  const layout=document.createElement('style');layout.textContent='#controls{display:flex;align-items:center;justify-content:flex-end;gap:12px;flex-wrap:wrap;box-sizing:border-box;position:relative;z-index:1;flex:0 0 auto;height:auto;min-height:40px;line-height:1.4;padding:6px 10px;overflow:visible;background:#151d2b;color:#e7edf7;font:14px/1.4 system-ui}#controls>span:first-child{display:none}#controls>span{display:inline-flex;align-items:center;gap:6px}#controls input[type=button]{font:inherit;padding:3px 10px}.emscripten_border{min-height:0;min-width:0;flex:1 1 0}';document.head.append(layout);
  // Fullscreen belongs to the player document, so controls and the game retain
  // the same layout. Canvas-only fullscreen lets the old shell cover controls.
  layout.textContent+='body.q2-fullscreen canvas.emscripten{position:absolute!important;width:100%!important;height:100%!important;z-index:auto}';
  function focusCanvas(){const canvas=document.querySelector('#canvas');if(canvas){window.focus();canvas.focus({preventScroll:true});}}
  const canvas=document.querySelector('#canvas');
  canvas?.addEventListener('pointerdown',focusCanvas,true);
  canvas?.addEventListener('mousedown',focusCanvas,true);
  window.addEventListener('message',event=>{if(event.source===parent&&event.origin===location.origin&&event.data==='q2tv-focus-canvas')focusCanvas();});
  function controls(){
  const fullscreen=document.querySelector('#controls input[type=button]');
  if(fullscreen)fullscreen.onclick=()=>{
    const active=document.fullscreenElement||document.webkitFullscreenElement;
    const target=active?document:document.documentElement;
    const action=active?(document.exitFullscreen||document.webkitExitFullscreen):(target.requestFullscreen||target.webkitRequestFullscreen);
    if(action)Promise.resolve(action.call(target)).then(focusCanvas).catch(()=>{});
  };
  for(const button of document.querySelectorAll('#q2-cmds-panel button'))if(/^(join|ready|unready|invuse|vote)$/i.test(button.textContent.trim()))button.remove();
  const panel=document.querySelector('#q2-cmds-panel');
  if(panel){
    const button=document.createElement('button');button.textContent=ru?'Закрыть меню':'Close menu';
    button.style.cssText='background:#1e233c;color:#c4d8ff;border:1px solid #6382ff66;border-radius:6px;padding:8px;font:13px system-ui;cursor:pointer';
    button.onclick=()=>{if(typeof Module!=='undefined'&&Module._Cbuf_AddText)Module.ccall('Cbuf_AddText',null,['string'],['putaway\n']);};panel.prepend(button);
  }
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',controls,{once:true});else controls();
})();
