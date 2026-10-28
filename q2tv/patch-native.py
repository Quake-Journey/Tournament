"""Apply the shared HTTP adapter to the two existing WSL Q2TV build trees.
Stores exact originals for review/recovery; does not touch player HTML/configs.
"""
from pathlib import Path
import shutil,argparse
import re
parser=argparse.ArgumentParser()
parser.add_argument('--webquake2',type=Path,default=Path.home()/'webquake2')
parser.add_argument('--qwasm2',type=Path,default=Path.home()/'qwasm2')
parser.add_argument('--backup-dir',type=Path,required=True)
args=parser.parse_args();root=Path(__file__).resolve().parent;backup=args.backup_dir
trees={'webquake2':args.webquake2,'qwasm2':args.qwasm2}
def edit(tree,relative,transform):
    p=trees[tree]/relative;dest=backup/tree/relative
    b=dest.read_bytes() if dest.exists() else p.read_bytes();s=b.decode('utf8',errors='surrogateescape');nl='\r\n' if b'\r\n' in b else '\n'
    out=transform(s.replace('\r\n','\n')).replace('\n',nl).encode('utf8',errors='surrogateescape')
    if not dest.exists():dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b)
    p.write_bytes(out)
def once(s,old,new):
    assert s.count(old)==1,(old[:120],s.count(old))
    return s.replace(old,new)
for tree,folder,header,source,network in [
    ('webquake2','client','client.h','cl_parse.c','cl_main.c'),
    ('qwasm2','src/client','header/client.h','cl_download.c','cl_network.c')]:
    desktop=tree=='webquake2'
    shutil.copyfile(root/'native-http.h',trees[tree]/folder/'q2tv_http.h')
    shutil.copyfile(root/'native-viewer.h',trees[tree]/folder/'q2tv_viewer.h')
    declarations='\n#ifdef __EMSCRIPTEN__\nvoid CL_Q2TVHTTP_Enable(void);\nvoid CL_Q2TVHTTP_Reset(void);\nvoid CL_Q2TVHTTP_NewMap(void);\nvoid Q2TV_CheckSpectator(void);\n#endif\n'
    edit(tree,folder+'/'+header,lambda s:s+declarations)
    def download(s):
        include='#include "'+header+'"'
        s=once(s,include,include+'\n#ifdef __EMSCRIPTEN__\n#define Q2TV_HTTP_FLUSH() '+('FS_FlushCache()' if desktop else '((void)0)')+'\n#include "q2tv_http.h"\n#define Q2TV_STOP_NATIVE() '+('CL_Disconnect(false)' if desktop else 'CL_Disconnect()')+'\n#include "q2tv_viewer.h"\n#endif')
        if desktop:
            s=once(s,'if (!strcmp (filename, lastfilename))','if (!strcmp (filename, lastfilename)\n#ifdef __EMSCRIPTEN__\n        && !q2tv_http_fallback\n#endif\n    )')
            s=once(s,'qboolean CL_ParseServerData (void)\n{','qboolean CL_ParseServerData (void)\n{\n#ifdef __EMSCRIPTEN__\n    CL_Q2TVHTTP_NewMap();\n#endif')
            needle='\t\t// download to a temp name, and only rename'
            hook='\t\tCL_DownloadFileName(name, sizeof(name), cls.downloadname);'
        else:
            needle='\t/* download to a temp name, and only rename'
            hook='\tCL_DownloadFileName(name, sizeof(name), cls.downloadname);'
        assert needle in s
        return s.replace(needle,'#ifdef __EMSCRIPTEN__\n'+hook+'\n    if(CL_Q2TVHTTP_Try(cls.downloadname,name))return false;\n#endif\n'+needle,1)
    edit(tree,folder+'/'+source,download)
    def net(s):
        signature='void CL_Disconnect (qboolean skipdisconnect)\n{' if desktop else 'CL_Disconnect(void)\n{'
        s=once(s,signature,signature+'\n#ifdef __EMSCRIPTEN__\n    CL_Q2TVHTTP_Reset();\n#endif')
        old='\t\t\t\tCom_Printf ("HTTP downloading supported by server but this client was built without USE_CURL, bad luck.\\n", LOG_CLIENT);' if desktop else '\t\t\t\tCom_Printf("HTTP downloading supported by server but not the client.\\n");'
        new='#ifdef __EMSCRIPTEN__\n                CL_Q2TVHTTP_Enable();\n'+('                Com_Printf("HTTP downloading enabled for Q2TV.\\n", LOG_CLIENT);' if desktop else '                Com_Printf("HTTP downloading enabled for Q2TV.\\n");')+'\n#else\n'+old+'\n#endif'
        return once(s,old,new)
    edit(tree,folder+'/'+network,net)
    if not desktop:
        edit(tree,folder+'/cl_parse.c',lambda s:once(once(s,'CL_ParseServerData(void)\n{','CL_ParseServerData(void)\n{\n#ifdef __EMSCRIPTEN__\n    CL_Q2TVHTTP_NewMap();\n#endif'),'\t\tCL_FireEntityEvents(&cl.frame);','#ifdef __EMSCRIPTEN__\n        Q2TV_CheckSpectator();\n#endif\n\t\tCL_FireEntityEvents(&cl.frame);'))
    else:
        edit(tree,folder+'/cl_ents.c',lambda s:once(s,'\t\tCL_FireEntityEvents (&cl.frame);','#ifdef __EMSCRIPTEN__\n        Q2TV_CheckSpectator();\n#endif\n\t\tCL_FireEntityEvents (&cl.frame);'))
edit('webquake2','emscripten/local.mk',lambda s:s+'\n# Current Emscripten equivalents of the existing memory/runtime settings.\nLDFLAGS := --preload-file $(BUILTIN_GAME) -lidbfs.js -s EXPORTED_RUNTIME_METHODS=ccall,cwrap,AL -s INITIAL_MEMORY=268435456 -s ALLOW_MEMORY_GROWTH=1 -s STACK_SIZE=16777216\n')
edit('webquake2','foreign/SDL2/src/video/emscripten/SDL_emscriptenopengles.c',lambda s:once(s,'    LOAD_FUNC(eglGetProcAddress);','    _this->egl_data->eglGetProcAddress = (void *(*)(const char *))eglGetProcAddress;'))
edit('webquake2','foreign/SDL2/src/thread/generic/module.mk',lambda s:once(s,'\tSDL_syssem.c \\', '\tSDL_sysmutex.c \\\n\tSDL_syssem.c \\'))
edit('webquake2','ref_gl/gl_rmain.c',lambda s:once(s,'emscripten_set_pointerlockchange_callback(NULL,','emscripten_set_pointerlockchange_callback(EMSCRIPTEN_EVENT_TARGET_DOCUMENT,'))
for name in ['SDL_emscriptenevents.c','SDL_emscriptenmouse.c','SDL_emscriptenvideo.c']:
    edit('webquake2','foreign/SDL2/src/video/emscripten/'+name,lambda s:re.sub(r'(emscripten_(?:get_element_css_size|set_element_css_size|request_pointerlock)\()NULL,',r'\1"#canvas",',s).replace('"#document"','EMSCRIPTEN_EVENT_TARGET_DOCUMENT').replace('"#window"','EMSCRIPTEN_EVENT_TARGET_WINDOW'))
def desktop_console(s):
    s=once(s,'"Q2TV alpha 0.1"','"Q2TV 0.2 beta"')
    s=once(s,'if (len >= (viddef.width * 0.125f) - (i+2))','int stamp_char_size = R_GetCharSize();\n\tint stamp_padding = stamp_char_size / 4;\n\tif (len >= (int)(viddef.width / stamp_char_size) - (i+2))')
    s=once(s,'offset = 20;','offset = stamp_char_size * 5 / 2;')
    s=once(s,'R_DrawString(viddef.width - 2 - (i * 8), lines-12-offset, version, 128, i);','R_DrawString(viddef.width - stamp_padding - i * stamp_char_size, lines-stamp_char_size-stamp_padding-offset, version, 128, i);')
    return once(s,'R_DrawString(viddef.width - 66, lines - 22 - offset, version, 128, i);','R_DrawString(viddef.width - stamp_padding - i * stamp_char_size, lines-stamp_char_size*2-stamp_char_size/2-offset, version, 128, i);')
edit('webquake2','client/console.c',desktop_console)
edit('qwasm2','src/client/cl_console.c',lambda s:once(s,'Com_sprintf(version, sizeof(version), "Yamagi Quake II v%s", YQ2VERSION);','Com_sprintf(version, sizeof(version), "Q2TV 0.2 beta");'))
print('Patched both native downloaders; originals: '+str(backup))
