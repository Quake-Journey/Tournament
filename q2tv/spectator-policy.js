'use strict';

// Quake II client wire commands, protocols 34 and R1Q2 35. Enforce the TV
// boundary at the shared WS -> UDP gateway, not in a bypassable menu button.
// Never scan arbitrary bytes for words: movement/checksums may contain them.
const passive = new Set(['new','disconnect','nextdl','score','scores','stats','oldscore','players','playerlist','highscores','id','help','commands','menu','inven','putaway','invprev','invnext','observe','spectate','chase','chasecam']);
function allowedCommand(text) {
  // Engines terminate sign-on commands with a newline inside the stringcmd.
  // Strip framing whitespace, but reject separators inside a command.
  text=text.trim();
  if(/^\x7fc [a-z0-9_]+(?: [^;\r\n\x00-\x1f\x7f]*)?$/i.test(text))return true;
  if(/^download\s/i.test(text)){
    const match=/^download\s+(?:"([a-z0-9_/+!~.-]+)"|([a-z0-9_/+!~.-]+))(?:\s+\d+)?(?:\s+udp-zlib)?$/i.exec(text);
    const file=match&&(match[1]||match[2]);return !!file&&!file.includes('..')&&!file.startsWith('/');
  }
  if(/[;\r\n\x00-\x1f\x7f"\\]/.test(text))return false;
  const tokens=text.trim().split(/\s+/),name=tokens[0].toLowerCase();
  if(['configstrings','baselines'].includes(name))return tokens.length===3&&/^-?\d+$/.test(tokens[1])&&/^\d+$/.test(tokens[2]);
  if(name==='begin')return tokens.length===2&&/^-?\d+$/.test(tokens[1]);
  if(name==='spectator')return tokens.length===2&&tokens[1]==='1';
  if(name==='chase'||name==='chasecam')return true;
  return tokens.length===1&&passive.has(name);
}

function spectatorInfo(text) {
  if(!text.startsWith('\\')||/[";\x00-\x1f\x7f]/.test(text))return null;
  const tokens=text.slice(1).split('\\');if(tokens.length%2)return null;
  const pairs=[];
  for(let i=0;i<tokens.length;i+=2){if(!tokens[i])return null;if(tokens[i].toLowerCase()!=='spectator')pairs.push(tokens[i],tokens[i+1]);}
  pairs.push('spectator','1');const result='\\'+pairs.join('\\');
  return result.length<512?result:null;
}

function skipDelta(buf,offset,minor) {
  const bits=buf[offset++];if(bits===undefined)throw new Error('short usercmd');
  let buttons=0;
  if(minor>=1904&&bits&64)buttons=buf[offset++];
  const widths=[[1,32],[2,64],[4,0],[8,4],[16,8],[32,16]];
  for(const [bit,compact] of widths)if(bits&bit)offset+=(compact&&buttons&compact)?1:2;
  if(minor<1904&&bits&64)offset++;
  if(bits&128)offset++;
  offset+=2; // msec and lightlevel, always present
  if(offset>buf.length)throw new Error('short usercmd');return offset;
}

function createSpectatorPolicy({onBlocked=()=>{}}={}) {
  let protocol=0,qport=0,minor=0,lastHeader=null;
  return {
    disconnectPackets(){
      if(!lastHeader)return [];
      const saved=lastHeader;lastHeader=null;
      const sequence=saved.readUInt32LE(0)&0x7fffffff;
      return [1,2,3].map(step=>{const h=Buffer.from(saved);h.writeUInt32LE((sequence+step)&0x7fffffff,0);return Buffer.concat([h,Buffer.from('\x04disconnect\0','latin1')]);});
    },
    filter(packet) {
      if(!Buffer.isBuffer(packet)||packet.length<4||packet.length>65535)return null;
      if(packet.readInt32LE(0)===-1){
        const text=packet.toString('latin1',4).replace(/\x00$/,'');
        if(/^getchallenge\s*$/.test(text)||/^status\s*$/.test(text)||/^info\s+\d+\s*$/.test(text)||/^ping\s*$/.test(text))return packet;
        const match=/^connect (34|35) (\d{1,5}) (-?\d+) "([^"\r\n]*)"(?: (\d+) (\d+))?\s*$/.exec(text);
        if(!match){onBlocked('connectionless');return null;}
        const info=spectatorInfo(match[4]);if(info===null)return null;
        protocol=Number(match[1]);qport=Number(match[2]);minor=protocol===35?Number(match[6]||0):0;lastHeader=null;
        if(qport>65535||protocol===35&&(qport>255||![1903,1904,1905].includes(minor)))return null;
        const replacement=`connect ${protocol} ${qport} ${match[3]} "${info}"${protocol===35?' '+match[5]+' '+minor:''}\n`;
        return Buffer.concat([Buffer.from([255,255,255,255]),Buffer.from(replacement,'latin1')]);
      }
      if(!protocol)return null;
      const header=8+(protocol===34?2:qport?1:0);if(packet.length<header)return null;
      let offset=header,changed=false;const parts=[packet.subarray(0,header)];
      try{
        while(offset<packet.length){
          const start=offset,op=packet[offset++];
          if(op===1){parts.push(packet.subarray(start,offset));continue;}
          if(op===3||op===4){
            const end=packet.indexOf(0,offset);if(end<0)return null;
            const text=packet.toString('latin1',offset,end);offset=end+1;
            if(op===3){const info=spectatorInfo(text);if(info===null)return null;if(info!==text){parts.push(Buffer.concat([Buffer.from([3]),Buffer.from(info,'latin1'),Buffer.from([0])]));changed=true;}else parts.push(packet.subarray(start,offset));}
            else if(allowedCommand(text)){
              parts.push(packet.subarray(start,offset));
              // Standard Quake II command: dismiss the automatic join menu on
              // entering a map. It never joins a team or changes spectator state.
              if(/^begin\s+-?\d+\s*$/i.test(text.trim())){parts.push(Buffer.from('\x04putaway\0','latin1'));changed=true;}
            }
            else {parts.push(Buffer.from([1]));changed=true;onBlocked('command',text.trim().split(/\s+/)[0].toLowerCase());}
          }else if(op===2){
            offset+=4+(protocol===34?1:0);
            for(let n=0;n<3;n++)offset=skipDelta(packet,offset,minor);
            // Native clients append movement last. Reject commands hidden after
            // it rather than changing the legacy move checksum or skipping them.
            if(offset!==packet.length)return null;
            parts.push(packet.subarray(start,offset));
          }else if(op===5&&protocol===35){offset+=4;if(offset>packet.length)return null;parts.push(packet.subarray(start,offset));}
          else return null; // Unsupported extensions fail closed.
        }
      }catch{return null;}
      lastHeader=Buffer.from(packet.subarray(0,header));
      return changed?Buffer.concat(parts):packet;
    }
  };
}
// Own connection only: retain its exact qport/ack and send the standard
// disconnect before releasing its UDP socket, even after a crashed/closed tab.
function closeSpectatorUDP(udp,policy,port,host){
  let done=false;const close=()=>{if(done)return;done=true;clearTimeout(timer);try{udp.close();}catch{}};
  const timer=setTimeout(close,1000);timer.unref?.();
  const packets=policy.disconnectPackets();let pending=packets.length;
  if(!pending){close();return;}
  for(const packet of packets){try{udp.send(packet,port,host,()=>{if(--pending===0)close();});}catch{if(--pending===0)close();}}
}
module.exports={createSpectatorPolicy,allowedCommand,spectatorInfo,closeSpectatorUDP};
