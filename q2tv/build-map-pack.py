"""Build a deterministic Q2TV map add-on with BSP/WAL/entity dependencies.
No engines, player configs or existing game packs are overwritten.
"""
import argparse, hashlib, json, re, struct
from pathlib import Path

parser=argparse.ArgumentParser()
parser.add_argument('--base',required=True,type=Path)
parser.add_argument('--output',required=True,type=Path)
parser.add_argument('--fallback',action='append',default=[],type=Path)
args=parser.parse_args()
sources={}
for pak in sorted(args.base.glob('*.pak'),key=lambda p:p.name.lower()):
    with pak.open('rb') as stream:
        if stream.read(4)!=b'PACK':continue
        off,size=struct.unpack('<ii',stream.read(8));stream.seek(off)
        for _ in range(size//64):
            name,pos,length=struct.unpack('<56sii',stream.read(64))
            sources[name.split(b'\0')[0].decode('ascii','replace').lower()]=(pak,pos,length)

def read(name):
    if '..' in name or name.startswith('/') or ':' in name:raise ValueError(name)
    for root in [args.base,*args.fallback]:
        file=root/name
        if file.is_file():return file.read_bytes()
    if name.lower() in sources:
        file,pos,length=sources[name.lower()]
        with file.open('rb') as stream:stream.seek(pos);return stream.read(length)
    return None

files={};missing=set();dependencies={};unused_models=[]
def add(name,optional=False):
    name=name.replace('\\','/').lower()
    if name in files:return files[name]
    data=read(name)
    if data is None:
        if not optional:missing.add(name)
        return None
    files[name]=data
    if name.endswith('.wal') and len(data)>=100:
        anim=data[56:88].split(b'\0')[0].decode('ascii','replace')
        if anim:
            relative=str(Path(name).parent/anim)+'.wal'
            add(relative if '/' not in anim and read(relative) is not None else 'textures/'+anim+'.wal')
    if name.endswith('.md2') and data[:4]==b'IDP2':
        count=struct.unpack_from('<i',data,20)[0];offset=struct.unpack_from('<i',data,44)[0]
        if 0<=count<256:
            for i in range(count):
                skin=data[offset+i*64:offset+(i+1)*64].split(b'\0')[0].decode('ascii','replace')
                if skin:add(skin)
    return data

maps=sorted(p for p in (args.base/'maps').glob('*.bsp') if re.match(r'^(?:q2duel\d+|ztn2dm\d+(?:_c)?|koldduel1|omf|q3t2g?|cor(?:_ffa_d|_d|_f|l)?)$',p.stem,re.I))
for file in maps:
    before=set(files);data=add('maps/'+file.name)
    if data[:8]!=b'IBSP&\0\0\0':raise ValueError('Not Quake II BSP: '+file.name)
    lumps=[struct.unpack_from('<ii',data,8+i*8) for i in range(19)]
    off,size=lumps[5]
    for i in range(off,off+size,76):
        texture=data[i+40:i+72].split(b'\0')[0].decode('ascii','replace')
        if texture:add('textures/'+texture+'.wal')
    off,size=lumps[0];entities=data[off:off+size].decode('latin1')
    for key,value in re.findall(r'"([^"\n]+)"\s*"([^"\n]*)"',entities):
        if key=='sky':
            for suffix in ['rt','bk','lf','ft','up','dn']:
                found=False
                for ext in ['tga','pcx']:
                    if add('env/'+value+suffix+'.'+ext,optional=True) is not None:found=True
                if not found:missing.add('env/'+value+suffix+'.tga|pcx')
        if key in ['noise','noise1','noise2'] and value and value[0]!='*':add('sound/'+value.removeprefix('sound/')+('' if value.endswith('.wav') else '.wav'))
    for block in re.findall(r'\{[^}]*\}',entities):
        fields=dict(re.findall(r'"([^"\n]+)"\s*"([^"\n]*)"',block))
        model=fields.get('model','');kind=fields.get('classname','')
        if not model or model.startswith('*'):continue
        if kind=='misc_banner':
            add('models/objects/banner/tris.md2')
            if model!='models/objects/banner/tris.md2':unused_models.append({'map':file.stem,'model':model,'reason':'misc_banner uses stock models/objects/banner/tris.md2'})
        elif kind=='misc_model' and read(model) is None:
            unused_models.append({'map':file.stem,'model':model,'reason':'misc_model is not registered by OpenTDM-X/baseq2'})
        else:add(model)
    dependencies[file.stem]=sorted(set(files)-before)

if missing:raise SystemExit('Missing map dependencies:\n'+'\n'.join(sorted(missing)))
args.output.parent.mkdir(parents=True,exist_ok=True)
with args.output.open('wb') as stream:
    stream.write(b'PACK'+b'\0'*8);directory=[]
    for name,data in sorted(files.items()):
        encoded=name.encode('ascii')
        if len(encoded)>55:raise ValueError('PAK name too long: '+name)
        directory.append(struct.pack('<56sii',encoded,stream.tell(),len(data)));stream.write(data)
    offset=stream.tell()
    for entry in directory:stream.write(entry)
    stream.seek(4);stream.write(struct.pack('<ii',offset,len(directory)*64))
manifest={'maps':[p.stem for p in maps],'files':len(files),'size':args.output.stat().st_size,'sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),'unusedEntityModels':unused_models,'entries':[{'name':n,'size':len(d),'sha256':hashlib.sha256(d).hexdigest()} for n,d in sorted(files.items())]}
args.output.with_suffix('.manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in manifest.items() if k!='entries'},ensure_ascii=False))
