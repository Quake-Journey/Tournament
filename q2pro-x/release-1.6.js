'use strict';
// Current website edition. The executable retains its actual Beta 5 label.
module.exports = function(c,docLinks,localizedDocLinks,url) {
 const {ui,hero,navItems,sections,docs,docsSection,release,beta,meta}=c;
 const revise=(x)=>{
   if(Array.isArray(x))return x.map(revise);
   if(x&&typeof x==='object'){for(const k of Object.keys(x))x[k]=revise(x[k]);return x;}
   return typeof x==='string'&&!x.includes('/q2pro-x/')?x.replace(/\b1\.5\b/g,'1.6'):x;
 };
 [ui,hero,navItems,sections,docsSection,meta].forEach(revise);
 ui.releaseCta={ru:'Скачать 1.6 Beta 5',en:'Download 1.6 Beta 5'};
 hero.eyebrow={ru:'Q2PRO-X 1.6 BETA 5 · ОСНОВНАЯ ВЕРСИЯ',en:'Q2PRO-X 1.6 BETA 5 · MAIN DOWNLOAD'};
 hero.releaseNote={
   ru:'В основной загрузке — Q2PRO-X 1.6 Beta 5. К возможностям 1.5 добавились эффекты контактов и кровь, объёмные материалы, растительность, отдельные пресеты света и видимости, музыкальный фон по состоянию игрока и новые инструменты для сетевой игры. Исправлены предсказание звука, поиск демок и работа рендера.',
   en:'The main download is now Q2PRO-X 1.6 Beta 5. It builds on 1.5 with contact and blood effects, relief materials, vegetation, separate lighting and visibility presets, health-based ambience and new multiplayer tools. Weapon audio prediction, demo search and renderer stability have also been improved.'
 };
 const overview=sections.find(s=>s.id==='overview');
 if(overview){overview.body.ru[0]='Q2PRO-X — клиент на базе Q2PRO, в котором сетевые инструменты, графические настройки, работа с демками и возможности одиночной игры собраны вместе. Текущая версия 1.6 Beta 5 сохраняет возможности 1.5 и добавляет перечисленные ниже системы.';overview.body.en[0]='Q2PRO-X is a Q2PRO-based client that brings network tools, visual settings, demo management and single-player features together. The current 1.6 Beta 5 build retains the 1.5 feature set and adds the systems described below.';}
 navItems.unshift({id:'new-in-16',icon:'✦',label:{ru:'Новое в 1.6',en:'New in 1.6'}});
 sections.splice(1,0,{
   id:'new-in-16',slug:'new-in-16',icon:'✦',title:{ru:'Что изменилось после 1.5',en:'What changed after 1.5'},
   summary:{ru:'Новые возможности и исправления, вошедшие в основной комплект 1.6 Beta 5.',en:'New features and fixes in the complete 1.6 Beta 5 download.'},
   body:{ru:[hero.releaseNote.ru,'Полное описание обновления от 1.5, настройки и примеры находятся в 29 руководствах на каждом языке. Книгу об истории проекта можно читать по главам или скачать в PDF.'],en:[hero.releaseNote.en,'The complete changes since 1.5, settings and examples are covered by 29 guides in each language. The project history can be read chapter by chapter or downloaded as a PDF.']},
   bullets:{
    ru:['Эффекты контактов: кровь, раны, осколки и следы боя; отдельные пресеты и 12 пользовательских слотов.','68 стандартных семейств частиц с переключателями и RGB; самостоятельные группы AVFX.','Новые металлические поверхности, объёмный кирпич, трава, плющ, папоротники, камни, почва и листья.','Отдельные пресеты освещения и видимости, по четыре пользовательских слота; подсветки частей MD2/MD5 и новые настройки Bloom.','Музыкальный фон для девяти состояний здоровья и защиты; согласованное поведение звука при потере фокуса.','Постоянная история серверов и переподключение; поиск демок во всех модах и архивах, фоновое обновление библиотеки.','Личные сообщения о фрагах и смерти, корректное оставшееся время выпавшего Quad, пауза живых таймеров в тайм-аутах.','Обновлённые подсказки переменных и команд, предсказание звука оружия, исправления EGL/ANGLE и безопасная проверка тесселяции.'],
    en:['Contact effects: blood, wounds, fragments and combat traces, with dedicated presets and 12 user slots.','68 standard particle families with enable and RGB controls, plus independent AVFX groups.','New metal surfaces, relief brickwork, grass, ivy, ferns, rocks, soil and leaves.','Separate lighting and visibility presets with four user slots each; MD2/MD5 model-part highlights and additional Bloom controls.','Music for nine health/protection states and consistent sound behaviour when the window loses focus.','Persistent server history and reconnection; demo search across mods and archives, with background library updates.','Personal kill/death messages, remaining-time handling for dropped Quad and live timers that pause during timeouts.','Updated variable and command help, corrected weapon sound prediction, EGL/ANGLE fixes and a safe tessellation capability check.']},
   images:[{file:'frag-messages.jpg',alt:{ru:'Личные сообщения о фрагах Q2PRO-X',en:'Q2PRO-X personal frag messages'},caption:{ru:'Новые уведомления о фрагах настраиваются отдельно от остального HUD.',en:'The new frag notifications can be configured independently of the rest of the HUD.'}}]
 });
 const stems={index:'Documentation_Index',release:'Release_Notes',network:'Network_Play_Guide',avfxAdvanced:'AVFX_Advanced_Features_Guide',avfxGenerator:'AVFX_Preset_Generator_Guide',avfxPresets:'AVFX_Presets_Guide',avfx:'AVFX_Settings_Guide',gameStatistics:'Game_Statistics_Demo_Auto_Statistics_Guide',startupIntro:'Startup_Intro_Settings_Guide',liveTimers:'Live_Item_Timers_Guide',hdr:'HDR_Guide',highlights:'Visual_Highlights_Guide',singleplayerMods:'Single_Player_Mods_Guide',interface:'Interface_Menu_Console_Guide',cvarReference:'Cvar_Command_Reference',video:'Video_Visuals_Guide',cvars:'Cvars_Cvar_Browser_Guide',voice:'Voice_Chat_Guide',controls:'Controls_Mouse_Zoom_Guide',demos:'Demo_Browser_Player_Guide',sound:'Sound_Guide'};
 for(const [key,stem] of Object.entries(stems))docs[key]=docLinks('1.6',stem);
 docs.networkGod=localizedDocLinks('1.6','Network_God_Руководство','Network_God_Guide');
 const extra=[
  ['contact','AVFX_Contact_Effects_Guide','Эффекты контактов AVFX','AVFX Contact Effects','Кровь, раны, осколки, пресеты контактов и пользовательские слоты.','Blood, wounds, fragments, contact presets and user slots.'],
  ['particles','Particle_Families_Enable_RGB_Guide','Семейства частиц и RGB','Particle Families and RGB','Включение семейств, цвет стандартных и AVFX-эффектов.','Family switches and colours for standard and AVFX effects.'],
  ['serverHistory','Server_History_Reconnect_Guide','История серверов и переподключение','Server History and Reconnection','Недавние подключения, размер истории и автоматическое восстановление связи.','Recent connections, history size and automatic reconnection.'],
  ['surfaceObjects','Surface_Objects_Guide','Объекты на поверхностях AVFX','AVFX Surface Objects','Растительность, камни, почва, листья, освещение и тени.','Vegetation, rocks, soil, leaves, lighting and shadows.'],
  ['lightingVisibility','Lighting_Visibility_Presets_Guide','Пресеты освещения и видимости','Lighting and Visibility Presets','Независимые наборы света и подсветок, заводские и пользовательские профили.','Independent lighting and highlight settings, factory and user profiles.'],
  ['healthAmbience','Health_Armor_Ambient_Guide','Музыка здоровья и защиты','Health and Protection Ambience','Девять состояний игрока и переходы между музыкальными слоями.','Nine player states and transitions between musical layers.'],
  ['audioAssets','Audio_Assets_Guide','Звуковые ресурсы','Audio Assets','Комплект звуков, интро и музыкальных ресурсов клиента.','The client sound, intro and music asset collection.']
 ];
 for(const [key,stem,ru,en,dr,de] of extra){docs[key]=docLinks('1.6',stem);docsSection.items.push({key,title:{ru,en},desc:{ru:dr,en:de}});}
 docsSection.summary={ru:'29 руководств на русском и английском: открыть в браузере или скачать DOCX.',en:'29 guides in Russian and English: read in your browser or download the DOCX.'};
 const notes=docsSection.items.find(x=>x.key==='release');if(notes)notes.desc={ru:'Полные изменения от 1.5 до текущей 1.6 Beta 5.',en:'Complete changes from 1.5 to the current 1.6 Beta 5.'};
 Object.assign(release,{
   version:'1.6 Beta 5',available:true,downloadHref:url('/q2pro-x/downloads/q2pro-x-1.6-beta5.zip'),
   popupMessage:{ru:'Полный клиентский комплект Q2PRO-X 1.6 Beta 5 с документацией RU/EN.',en:'The complete Q2PRO-X 1.6 Beta 5 client package with RU/EN documentation.'},
   sectionTitle:{ru:'Q2PRO-X 1.6 Beta 5 — основная загрузка',en:'Q2PRO-X 1.6 Beta 5 — main download'},
   sectionSummary:{ru:'Текущая основная версия сайта вместо 1.5. Полный клиентский пакет для Windows x64.',en:'The current main website version, replacing 1.5. Complete Windows x64 client package.'},
   sectionBody:{ru:['Сборка сохраняет обозначение 1.6 Beta 5. Это полный комплект клиентских файлов с исправлениями от 4 октября и обновлённой документацией; отдельный раздел бета-версий не требуется.','Для игры нужны данные оригинального Quake II. Сохраните свои конфиги и пользовательские пресеты; распакуйте клиентский слой в папку игры. Стандартная game DLL не заменяется: модуль одиночной игры Q2PRO-X находится в собственном подкаталоге.'],en:['The build retains the 1.6 Beta 5 label. This complete client package includes the October 4 fixes and updated documentation. It is offered directly as the main download.','Original Quake II game data is required. Keep your configurations and user presets when extracting the client layer into your game folder. The stock game DLL is not replaced: the Q2PRO-X single-player module resides in its own subfolder.']},
   quickStartSteps:{ru:[{title:'1. Сохраните личные настройки',text:'Для обновления сохраните копию своих конфигов и пользовательских пресетов. Для новой установки подготовьте папку с данными оригинального Quake II.'},{title:'2. Распакуйте весь комплект',text:'Распакуйте q2pro-x-1.6-beta5.zip в папку игры с заменой файлов клиента. Переносите также DLL, меню и заводские ресурсы, а не только EXE.'},{title:'3. Запустите клиент',text:'Запустите Q2PRO-X.exe. Через меню выберите настройки видео, пресеты, сервер или одиночную игру.'},{title:'4. Откройте руководство',text:'Все 29 руководств доступны здесь для чтения и скачивания; копии находятся и в папке docs архива.'}],en:[{title:'1. Keep your personal settings',text:'Before updating, keep a copy of your configurations and user presets. For a new installation, prepare a folder containing the original Quake II game data.'},{title:'2. Extract the complete package',text:'Extract q2pro-x-1.6-beta5.zip into the game folder, replacing client files. Include the DLLs, menus and factory assets, not just the EXE.'},{title:'3. Start the client',text:'Run Q2PRO-X.exe. Use the menus to select video settings, presets, a server or single-player play.'},{title:'4. Read the guides',text:'All 29 guides are available here for reading and download. Copies are also included in the archive under docs.'}]}
 });
 beta.visible=false;beta.available=false;
 meta.title={ru:'Q2PRO-X 1.6 Beta 5 — клиент Quake II',en:'Q2PRO-X 1.6 Beta 5 — Quake II client'};
 meta.ogImage='/q2pro-x/history/assets/cover-project-photo.jpg';
};
