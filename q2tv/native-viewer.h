/* Read native playerstate: a server can auto-join a TV client without any
 * incoming join command. Leave only while actually playing; never toggle an
 * existing spectator/chase camera. No game-mod changes are required. */
EMSCRIPTEN_KEEPALIVE int Q2TV_ViewState(void) {
    if(cls.state!=ca_active||!cl.frame.valid)return 0;
    if(cl.frame.playerstate.stats[STAT_SPECTATOR]||
       cl.frame.playerstate.pmove.pm_type==PM_SPECTATOR)return 2;
    return 1;
}
static unsigned int q2tv_last_observe;
static int q2tv_auto_exits;
EMSCRIPTEN_KEEPALIVE int Q2TV_AutoExits(void) { return q2tv_auto_exits; }
EMSCRIPTEN_KEEPALIVE void Q2TV_Observe(void) {
    unsigned int now=Sys_Milliseconds();
    if(Q2TV_ViewState()!=1)return;
    if(q2tv_auto_exits&&now-q2tv_last_observe<1000)return;
    q2tv_last_observe=now;q2tv_auto_exits++;
    Cvar_Set("spectator","1");
    Cbuf_AddText("cmd spectate\n");
}
/* Run on each valid server frame, including the first frame after auto-join.
 * This must not depend on the browser scheduling a separate UI timer. */
void Q2TV_CheckSpectator(void) { Q2TV_Observe(); }
EMSCRIPTEN_KEEPALIVE void Q2TV_Stop(void) {
    Q2TV_STOP_NATIVE();
}
