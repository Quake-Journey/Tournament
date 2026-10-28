/* Shared Emscripten adapter, included once in each engine's download unit.
 * Existing precache sequencing and UDP fallback remain owned by that engine. */
#include <emscripten.h>
extern cvar_t *allow_download_models,*allow_download_sounds,*allow_download_maps;
void CL_DownloadFileName(char *dest,int destlen,char *fn);
static int q2tv_http_enabled, q2tv_http_fallback, q2tv_http_pending, q2tv_http_prefetched;
static char q2tv_http_filename[MAX_OSPATH];
static char q2tv_http_texturemap[MAX_OSPATH];
static void CL_Q2TVHTTP_PrefetchTextures(void);

EM_JS(void, Q2TV_HTTP_CancelJS, (), {
    if(Module.q2tvHTTP){Module.q2tvHTTP.cancel();Module.q2tvHTTP=null;}
});
void CL_Q2TVHTTP_Reset(void) {
    q2tv_http_enabled=0;q2tv_http_pending=0;q2tv_http_prefetched=0;q2tv_http_filename[0]=0;q2tv_http_texturemap[0]=0;
    Q2TV_HTTP_CancelJS();
}
void CL_Q2TVHTTP_Enable(void) {q2tv_http_enabled=1;}
void CL_Q2TVHTTP_NewMap(void) {int enabled=q2tv_http_enabled;CL_Q2TVHTTP_Reset();q2tv_http_enabled=enabled;}

EMSCRIPTEN_KEEPALIVE void Q2TV_HTTP_Done(int success) {
    char filename[MAX_OSPATH];
    if(!q2tv_http_pending)return;
    strcpy(filename,q2tv_http_filename);q2tv_http_pending=0;
    if(success){
        Q2TV_HTTP_FLUSH();
        cls.downloadpercent=0;cls.downloadname[0]=0;
        CL_Q2TVHTTP_PrefetchTextures();
        CL_RequestNextDownload();
    }else{
        /* Retry this exact file over UDP, even though precache already moved
         * to its next entry. Never skip a missing map after HTTP failure. */
        q2tv_http_fallback=1;
        if(CL_CheckOrDownloadFile(filename))CL_RequestNextDownload();
        q2tv_http_fallback=0;
    }
}
EM_JS(int, Q2TV_HTTP_StartJS, (const char *file,const char *game,const char *destination), {
    if(typeof window.q2tvCreateDownloadQueue!=='function')return 0;
    if(!Module.q2tvHTTP)Module.q2tvHTTP=window.q2tvCreateDownloadQueue((dest,bytes)=>{
        FS.mkdirTree(dest.slice(0,dest.lastIndexOf('/')));
        FS.writeFile(dest+'.http-tmp',bytes);FS.rename(dest+'.http-tmp',dest);
    });
    const state=Module.q2tvHTTP;
    state.add(UTF8ToString(file),UTF8ToString(game),UTF8ToString(destination),true).then(()=>{
        if(Module.q2tvHTTP!==state)return;
        _Q2TV_HTTP_Done(1);
    }).catch(()=>{
        if(Module.q2tvHTTP!==state)return;
        _Q2TV_HTTP_Done(0);
    });
    return 1;
});
EM_JS(void, Q2TV_HTTP_PrefetchJS, (const char *file,const char *game,const char *destination), {
    if(Module.q2tvHTTP)Module.q2tvHTTP.add(UTF8ToString(file),UTF8ToString(game),UTF8ToString(destination));
});
static void CL_Q2TVHTTP_PrefetchTextures(void) {
    void *data=NULL;int length,offset,bytes,i;dheader_t *header;texinfo_t *info;
    char texture[33],file[MAX_OSPATH],destination[MAX_OSPATH];
    const char *map=cl.configstrings[CS_MODELS+1];
    if(!allow_download_maps->value||!map[0]||!strcmp(map,q2tv_http_texturemap))return;
    length=FS_LoadFile(map,&data);
    if(length<(int)sizeof(dheader_t)||!data){if(data)FS_FreeFile(data);return;}
    header=(dheader_t *)data;offset=LittleLong(header->lumps[LUMP_TEXINFO].fileofs);bytes=LittleLong(header->lumps[LUMP_TEXINFO].filelen);
    if(LittleLong(header->ident)!=IDBSPHEADER||LittleLong(header->version)!=BSPVERSION||offset<0||bytes<0||offset>length||bytes>length-offset||bytes%(int)sizeof(texinfo_t)){FS_FreeFile(data);return;}
    Com_sprintf(q2tv_http_texturemap,sizeof(q2tv_http_texturemap),"%s",map);
    info=(texinfo_t *)((byte *)data+offset);
    for(i=0;i<bytes/(int)sizeof(texinfo_t);i++){
        memcpy(texture,info[i].texture,32);texture[32]=0;
        Com_sprintf(file,sizeof(file),"textures/%s.wal",texture);
        if(!texture[0]||strstr(file,"..")||strchr(file,':')||FS_LoadFile(file,NULL)!=-1)continue;
        CL_DownloadFileName(destination,sizeof(destination),file);
        Q2TV_HTTP_PrefetchJS(file,cl.gamedir,destination);
    }
    FS_FreeFile(data);
}
static void CL_Q2TVHTTP_Prefetch(void) {
    int i;char file[MAX_OSPATH],destination[MAX_OSPATH];const char *value;
    if(q2tv_http_prefetched)return;
    q2tv_http_prefetched=1;
    for(i=1;i<MAX_MODELS+MAX_SOUNDS;i++){
        if(i<MAX_MODELS){
            if(!allow_download_models->value)continue;
            value=cl.configstrings[CS_MODELS+i];
            if(!value[0]||value[0]=='*'||value[0]=='#'||i==1)continue;
            Com_sprintf(file,sizeof(file),"%s",value);
        }else{
            if(!allow_download_sounds->value)continue;
            value=cl.configstrings[CS_SOUNDS+i-MAX_MODELS];
            if(!value[0]||value[0]=='*')continue;
            Com_sprintf(file,sizeof(file),"sound/%s",value);
        }
        if(strstr(file,"..")||strchr(file,':')||file[0]=='/'||FS_LoadFile(file,NULL)!=-1)continue;
        CL_DownloadFileName(destination,sizeof(destination),file);
        Q2TV_HTTP_PrefetchJS(file,cl.gamedir,destination);
    }
}
static int CL_Q2TVHTTP_Try(const char *file,const char *destination) {
    if(!q2tv_http_enabled||q2tv_http_fallback)return 0;
    if(q2tv_http_pending)return 1;
    if(strlen(file)>=sizeof(q2tv_http_filename))return 0;
    strcpy(q2tv_http_filename,file);
    q2tv_http_pending=Q2TV_HTTP_StartJS(file,cl.gamedir,destination);
    if(q2tv_http_pending){CL_Q2TVHTTP_Prefetch();CL_Q2TVHTTP_PrefetchTextures();}
    return q2tv_http_pending;
}
