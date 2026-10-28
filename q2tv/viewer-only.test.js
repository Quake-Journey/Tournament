const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
test('auto-spectate waits for WASM, exits actual play, leaves chase alone and disconnects on close',()=>{
  let tick,hide,now=2000,nativeState=1,reads=0,exits=0,stops=0,initialized=0;
  const sandbox={window:{addEventListener:(event,fn)=>{if(event==='pagehide')hide=fn;}},navigator:{language:'en'},Date:{now:()=>now},
    document:{createElement:()=>({style:{}}),head:{append(){}},body:{append(){}},querySelectorAll:()=>[],querySelector:()=>null},
    setInterval:fn=>{tick=fn;return 1;},clearInterval:()=>{},
    Module:{onRuntimeInitialized:()=>initialized++,_Q2TV_ViewState:()=>{reads++;return nativeState;},_Q2TV_Observe:()=>exits++,_Q2TV_AutoExits:()=>exits,_Q2TV_Stop:()=>stops++}};
  vm.runInNewContext(fs.readFileSync(__dirname+'/viewer-only.js','utf8'),sandbox);
  tick();assert.equal(reads,0,'must not call exported stub before runtime init');
  sandbox.Module.onRuntimeInitialized();assert.equal(initialized,1,'preserve original player init callback');
  tick();assert.equal(exits,1);tick();assert.equal(exits,1,'rate limited');
  nativeState=2;now+=2000;tick();assert.equal(exits,1,'do not toggle existing spectator/chase');
  nativeState=1;tick();assert.equal(exits,2,'later server auto-join is handled too');hide();assert.equal(stops,1);
});
