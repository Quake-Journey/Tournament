'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createSpectatorPolicy,allowedCommand}=require('./spectator-policy');
const oob=text=>Buffer.concat([Buffer.from([255,255,255,255]),Buffer.from(text,'latin1')]);
const command=text=>Buffer.concat([Buffer.from([4]),Buffer.from(text,'latin1'),Buffer.from([0])]);
function state(protocol=34,qport=23){const policy=createSpectatorPolicy();const packet=policy.filter(oob(`connect ${protocol} ${qport} 123 "\\name\\TV\\spectator\\0"${protocol===35?' 1390 1905':''}\n`));assert.ok(packet.toString('latin1').includes('\\spectator\\1'));return policy;}
const header=(protocol=34,qport=23)=>{const b=Buffer.alloc(8+(protocol===34?2:qport?1:0));b.writeUInt32LE(0x80000001);if(protocol===34)b.writeUInt16LE(qport,8);else if(qport)b[8]=qport;return b;};
test('TV disallows join/team/ready, console forwarding, menu activation and compound commands',()=>{
  for(const cmd of ['join','JOIN','join red','team red','ready','cmd join','invuse','use Blaster','vote map q2dm1','spectator 0','spectator','chase;join','chase\njoin','begin 1;join','download ../private','rcon password join'])assert.equal(allowedCommand(cmd),false,cmd);
  for(const cmd of ['new','begin 1','begin 1\n','begin -1\n','baselines 1 0','configstrings -1 0\n','\x7fc version WebQuake2 1.0\n','nextdl','download maps/q2duel1.bsp 0','spectator 1','chase','chase David-X[200]','invnext','invprev','score','stats','observe','disconnect'])assert.equal(allowedCommand(cmd),true,cmd);
});
test('TV enforces spectator on connect and every userinfo update, retaining reliable headers',()=>{
  for(const cmd of ['download "maps/q2duel1.bsp" 0 udp-zlib','download "maps/q2duel1.bsp"','download sound/world/test.wav 128','download textures/a/+0.wal 0'])assert.equal(allowedCommand(cmd),true,cmd);
  for(const cmd of ['download "maps/a.bsp";join','download "../config.cfg"','download maps/a.bsp\njoin','download /absolute.bsp'])assert.equal(allowedCommand(cmd),false,cmd);
  for(const proto of [34,35]){
    const p=state(proto),h=header(proto),info=Buffer.from('\x03\\name\\Changed\\spectator\\0\x00','latin1');
    const result=p.filter(Buffer.concat([h,command('join'),command('invuse'),info,command('score')]));assert.deepEqual(result.subarray(0,h.length),h);assert.ok(!result.includes(Buffer.from('join')));assert.ok(!result.includes(Buffer.from('invuse')));assert.ok(result.includes(Buffer.from('spectator\\1')));assert.ok(result.includes(Buffer.from('score')));
    for(const text of ['rcon secret map q2dm1','connect 36 1 3 "\\name\\TV"','connect 34 1 3 "\\name\\TV"\njoin'])assert.equal(p.filter(oob(text)),null);
    const begin=p.filter(Buffer.concat([h,command('begin 123\n')]));assert.ok(begin.includes(Buffer.from('begin 123\n\0','latin1')));assert.ok(begin.includes(Buffer.from('\x04putaway\0','latin1')));
  }
});
test('TV preserves protocol 34 and 35 movement, including R1Q2 compressed usercmd fields',()=>{
  for(const proto of [34,35])for(const qport of [0,23]){
    const p=state(proto,qport),h=header(proto,qport);
    const delta=proto===34?Buffer.from([255,1,0,2,0,3,0,4,0,5,0,6,0,128,0,16,0]):Buffer.from([255,124,1,2,3,0,4,5,6,0,16,0]);
    const move=Buffer.concat([Buffer.from(proto===34?[2,197,1,0,0,0]:[2,1,0,0,0]),delta,delta,delta]);
    const original=Buffer.concat([h,move]);assert.deepEqual(p.filter(original),original,`move ${proto}/${qport}`);
    const mixed=p.filter(Buffer.concat([h,command('join'),move]));assert.deepEqual(mixed,Buffer.concat([h,Buffer.from([1]),move]));
    assert.equal(p.filter(Buffer.concat([original,command('join')])),null,'commands after movement fail closed');
    assert.equal(p.filter(original.subarray(0,-1)),null,'truncated usercmd fails closed');
  }
});
test('closing a TV sends only its own sequenced disconnect, even without a native close callback',()=>{
  for(const proto of [34,35])for(const qport of [0,23]){
    const policy=state(proto,qport),h=header(proto,qport);h.writeUInt32LE(0x80000042,4);
    policy.filter(Buffer.concat([h,command('score')]));const packets=policy.disconnectPackets();
    assert.equal(packets.length,3);
    packets.forEach((packet,i)=>{assert.equal(packet.readUInt32LE(0),i+2);assert.deepEqual(packet.subarray(4,h.length),h.subarray(4));assert.equal(packet.toString('latin1',h.length),'\x04disconnect\0');});
    assert.deepEqual(policy.disconnectPackets(),[],'close is idempotent');
  }
  assert.deepEqual(createSpectatorPolicy().disconnectPackets(),[],'no guessed packet before connection');
});
