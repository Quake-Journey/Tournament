// index.js
// Telegram bot: Telegraf + MongoDB
// Функции:
// - Администраторы (владельцы глобально + админы по чату; добавление через reply/@username/user_id)
// - Скилл-группы: /skillgroup | /sg
// - Карты: /map | /m
// - Игровые группы: /groups | /g (алгоритмы 1|2|3, min/rec/max/maxcount, Waiting для 2/3)
// - Рейтинг: /groups rating [name1,name2,...] — порядок (первый = лучший), /groups rating — показать
// - Результаты групп: /groups N result ...
// - Финалы: /finals (algo 1|2, maxplayers, totalplayers, make)
//   - algo=1 — прежняя логика по результатам групп
//   - algo=2 — альтернативная логика по рейтингу (деление на High/Low половины, игнорирует totalplayers, учитывает maxplayers)
// - /info | /i, /help | /h
// - Пагинация/чанкование, проверки дубликатов (case-insensitive), сортировка показа игроков по SG
// - Обновление chatId при миграции группы
// - Крипто-рандом для тасовки карт и перемешивания списков

require('dotenv').config();
const { Telegraf } = require('telegraf');
const { MongoClient, ObjectId } = require('mongodb');

const crypto = require('crypto');

const fs = require('fs');
const path = require('path');
const https = require('https');

const SCREENSHOTS_DIR = process.env.SCREENSHOTS_DIR || path.resolve(process.cwd(), 'screenshots');
const ACHIEVEMENTS_ROLE = 'Achievements';
const NEWS_ROLE = 'News';

const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGODB_URI = process.env.MONGODB_URI;
const OWNER_IDS = (process.env.OWNER_IDS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)
  .map(s => Number(s));


if (!BOT_TOKEN) {
  console.error('BOT_TOKEN is required');
  process.exit(1);
}
if (!MONGODB_URI) {
  console.error('MONGODB_URI is required');
  process.exit(1);
}
if (!OWNER_IDS.length) {
  console.warn('Warning: OWNER_IDS is empty. Set at least one owner user_id to ensure control.');
}

const bot = new Telegraf(BOT_TOKEN, {
  handlerTimeout: 90_000,
});

let db;
let colChats, colAdmins, colSkillGroups, colMaps, colGameGroups, colCounters, colUserIndex, colFinalGroups, colWaitingPlayers, colRatings;
let colGroupPoints, colFinalPoints, colScreenshots, colFinalRatings, colSuperFinalGroups, colSuperFinalRatings, colNews, colFeedback, colUsers, colTeams;
let colRegistrationSettings; // настройки регистрации турнира
let colSignups;              // заявки игроков и команд на турнир

let colCustomGroups, colCustomPoints;
let colAchievements; // NEW
let colRoles; // NEW: roles per user per chat
// NEW: результаты по картам
let colGroupResults, colFinalResults, colSuperFinalResults;


const MAX_MSG_LEN = 4096;
const SAFE_CHUNK = 3800; // запас под форматирование
const DEFAULT_MAX_PLAYERS = 6;

// Simple rate limiter per user per chat
const rateBuckets = new Map();
function rateLimit(ctx, limit = 8, intervalMs = 5000) {
  const key = `${ctx.chat?.id}:${ctx.from?.id}`;
  const now = Date.now();
  const bucket = rateBuckets.get(key) || { count: 0, ts: now };
  if (now - bucket.ts > intervalMs) {
    bucket.count = 0;
    bucket.ts = now;
  }
  bucket.count++;
  rateBuckets.set(key, bucket);
  return bucket.count <= limit;
}

// Utility:
function extractCommandArgText(ctx, aliasList) {
  // пример: "/feedback add какой-то текст" -> вернёт "какой-то текст"
  // aliasList: ["feedback", "fb"]
  const raw = (ctx.message?.text || '').trim();
  const lowered = raw.toLowerCase();
  // найдём первый совпадающий префикс
  for (const alias of aliasList) {
    const p1 = `/${alias} add`;
    const p2 = `/${alias} edit`;
    if (lowered.startsWith(p1)) return raw.slice(p1.length).trim();
    if (lowered.startsWith(p2)) return raw.slice(p2.length).trim();
  }
  return '';
}

function getUserIdentity(ctx) {
  const from = ctx.from || {};
  const userId = from.id;
  const username = from.username ? `@${from.username}` : null;
  const name = [from.first_name, from.last_name].filter(Boolean).join(' ').trim() || 'Unknown';
  const display = username || name;
  return { userId, username, name, display };
}


// Utility: chunked replies
async function replyChunked(ctx, text, extra = {}) {
  if (!text) return;
  if (text.length <= MAX_MSG_LEN) {
    return ctx.reply(text, { disable_web_page_preview: true, ...extra });
  }
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    chunks.push(text.slice(i, i + SAFE_CHUNK));
    i += SAFE_CHUNK;
  }
  for (const chunk of chunks) {
    // eslint-disable-next-line no-await-in-loop
    await ctx.reply(chunk, { disable_web_page_preview: true, ...extra });
  }
}

function escapeHtml(s = '') {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function replyPre(ctx, text) {
  if (!text) return;
  // Разбиваем, чтобы не превысить лимит Telegram
  const MAX = 4000; // чуть меньше 4096, с запасом под <pre>... теги
  for (let i = 0; i < text.length; i += MAX) {
    const chunk = text.slice(i, i + MAX);
    // eslint-disable-next-line no-await-in-loop
    await ctx.reply(`<pre>${escapeHtml(chunk)}</pre>`, { parse_mode: 'HTML', disable_web_page_preview: true });
  }
}


// Utility: normalize string (case-insensitive uniqueness)
function norm(s) {
  return (s || '').trim().replace(/\s+/g, ' ').toLowerCase();
}
function cleanListParam(param) {
  if (!param) return [];
  return param
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}
function dedupByNorm(items) {
  const seen = new Set();
  const out = [];
  for (const nameOrig of items) {
    const n = norm(nameOrig);
    if (!seen.has(n)) {
      seen.add(n);
      out.push({ nameOrig, nameNorm: n });
    }
  }
  return out;
}

// --- Users: helpers ---
// Глобальные профили игроков по Telegram ID

async function findUserByTelegramId(telegramId) {
  if (!telegramId) return null;
  return colUsers.findOne({ telegramId: Number(telegramId) });
}

async function findUserByNick(nick, excludeTelegramId = null) {
  const nickNorm = norm(nick);
  if (!nickNorm) return null;
  const query = { nickNorm };
  if (excludeTelegramId != null) {
    query.telegramId = { $ne: Number(excludeTelegramId) };
  }
  return colUsers.findOne(query);
}

function formatUserProfileForDisplay(u) {
  if (!u) return 'Профиль не найден.';
  const lines = [];

  lines.push('Профиль игрока');
  lines.push('----------------');

  lines.push(`Telegram ID: ${u.telegramId}`);
  const username = u.username ? `@${u.username}` : '(нет username)';
  lines.push(`Telegram: ${username}`);

  if (u.nick) {
    lines.push(`Ник: ${u.nick}`);
  } else {
    lines.push('Ник: (не задан)');
  }

  if (u.bio) {
    lines.push('');
    lines.push('Описание:');
    lines.push(u.bio);
  }

  const createdAt = u.createdAt ? new Date(u.createdAt) : null;
  const updatedAt = u.updatedAt ? new Date(u.updatedAt) : null;
  if (createdAt || updatedAt) {
    lines.push('');
    if (createdAt) lines.push(`Создан: ${createdAt.toLocaleString('ru-RU')}`);
    if (updatedAt) lines.push(`Обновлён: ${updatedAt.toLocaleString('ru-RU')}`);
  }

  return lines.join('\n');
}

function formatUsersListForDisplay(users = []) {
  if (!users.length) return 'Пока нет зарегистрированных пользователей.';

  const lines = [];
  lines.push(`Всего зарегистрированных пользователей: ${users.length}`);
  lines.push('');

  let i = 1;
  for (const u of users) {
    const parts = [];
    if (u.nick) parts.push(`"${u.nick}"`);
    if (u.username) parts.push(`@${u.username}`);
    parts.push(`#${u.telegramId}`);
    let line = `${i}. ${parts.join(' ')}`;
    if (u.bio) {
      const trimmed = String(u.bio).trim();
      if (trimmed) {
        const short = trimmed.length > 80 ? `${trimmed.slice(0, 80)}…` : trimmed;
        line += ` — ${short}`;
      }
    }
    lines.push(line);
    i += 1;
  }

  return lines.join('\n');
}

// --- Teams: helpers ---

async function findTeamById(teamId) {
  if (!teamId) return null;
  try {
    return await colTeams.findOne({ _id: new ObjectId(String(teamId)) });
  } catch {
    return null;
  }
}

function formatTeamForDisplay(team) {
  if (!team) return 'Команда не найдена.';

  const lines = [];
  lines.push('Команда');
  lines.push('----------------');

  lines.push(`ID: ${team._id}`);
  lines.push(`Название: ${team.name || '(без названия)'}`);

  if (team.description) {
    lines.push('');
    lines.push('Описание:');
    lines.push(team.description);
  }

  if (Array.isArray(team.memberNicks) && team.memberNicks.length) {
    lines.push('');
    lines.push('Состав:');
    lines.push(team.memberNicks.join(', '));
  }

  const createdAt = team.createdAt ? new Date(team.createdAt) : null;
  const updatedAt = team.updatedAt ? new Date(team.updatedAt) : null;
  if (createdAt || updatedAt) {
    lines.push('');
    if (createdAt) lines.push(`Создана: ${createdAt.toLocaleString('ru-RU')}`);
    if (updatedAt) lines.push(`Обновлена: ${updatedAt.toLocaleString('ru-RU')}`);
  }

  return lines.join('\n');
}

function formatTeamsListForDisplay(teams = []) {
  if (!teams.length) return 'Пока нет зарегистрированных игровых команд.';

  const lines = [];
  lines.push(`Всего игровых команд: ${teams.length}`);
  lines.push('');

  let i = 1;
  for (const t of teams) {
    const name = t.name || '(без названия)';
    lines.push(`${i}. ${name} — ID: ${t._id}`);

    if (Array.isArray(t.memberNicks) && t.memberNicks.length) {
      lines.push(`Игроки: ${t.memberNicks.join(', ')}`);
    }

    const desc = (t.description || '').trim();
    if (desc) {
      lines.push('Описание:');
      lines.push(desc);
    }

    lines.push(''); // пустая строка между командами
    i += 1;
  }

  return lines.join('\n');
}

// --- Signups: хелперы ---

function generateSignupId() {
  // crypto уже подключён вверху файла
  return crypto.randomBytes(12).toString('hex');
}

function formatRegistrationSettingsForDisplay(reg) {
  const lines = [];

  lines.push('Настройки регистрации турнира:');
  lines.push(`- тип турнира: ${reg.tournamentType || '(not set)'}`);
  lines.push(`- регистрация: ${reg.registrationEnabled ? 'открыта' : 'закрыта'}`);

  if (reg.registrationOpenedAt) {
    lines.push(`- открыта: ${formatMoscowDateTime(reg.registrationOpenedAt)}`);
  }
  if (reg.registrationClosedAt) {
    lines.push(`- закрыта: ${formatMoscowDateTime(reg.registrationClosedAt)}`);
  }

  lines.push(`- maxPlayers: ${reg.maxPlayers != null ? reg.maxPlayers : '(not set)'}`);
  lines.push(`- deadline: ${reg.deadline ? formatMoscowDateTime(reg.deadline) : '(not set)'}`);

  return lines.join('\n');
}

function formatPlayerSignupsList(signups = []) {
  if (!signups.length) return 'Заявок игроков пока нет.';

  const lines = [];
  lines.push(`Всего заявок игроков: ${signups.length}`);
  lines.push('');

  let i = 1;
  for (const s of signups) {
    const name = s.playerNick || '(без ника)';
    const dt = s.createdAt ? formatMoscowDateTime(s.createdAt) : '(дата неизвестна)';
    const conf = s.confirmed ? 'подтверждена' : 'ожидает подтверждения';
    lines.push(`${i}. ${name}`);
    lines.push(`   ID: ${s.signupId}`);
    lines.push(`   Дата регистрации: ${dt}`);
    lines.push(`   Статус: ${conf}`);
    lines.push('');
    i += 1;
  }

  return lines.join('\n');
}

function formatTeamSignupsList(signups = []) {
  if (!signups.length) return 'Заявок команд пока нет.';

  const lines = [];
  lines.push(`Всего заявок команд: ${signups.length}`);
  lines.push('');

  let i = 1;
  for (const s of signups) {
    const name = s.teamName || '(без названия)';
    const dt = s.createdAt ? formatMoscowDateTime(s.createdAt) : '(дата неизвестна)';
    const conf = s.confirmed ? 'подтверждена' : 'ожидает подтверждения';

    lines.push(`${i}. ${name}`);
    lines.push(`   ID: ${s.signupId}`);
    lines.push(`   Дата регистрации: ${dt}`);
    lines.push(`   Статус: ${conf}`);

    if (Array.isArray(s.teamMembers) && s.teamMembers.length) {
      lines.push(`   Состав: ${s.teamMembers.join(', ')}`);
    }

    lines.push('');
    i += 1;
  }

  return lines.join('\n');
}


// --- NEW: News2 (конструктор новостей с блоками) ---

// Черновики конструкторов: key = `${chatId}:${userId}`
const news2Sessions = new Map();
function n2Key(chatId, userId) { return `${chatId}:${userId}`; }

function purgeExpiredNews2Sessions() {
  const now = Date.now();
  for (const [k, s] of news2Sessions) {
    if (s.expiresAt && s.expiresAt < now) news2Sessions.delete(k);
  }
}

function blocksToPlainText(blocks = []) {
  return blocks
    .filter(b => b && b.type === 'text' && b.text && String(b.text).trim())
    .map(b => String(b.text).trim())
    .join('\n\n')
    .trim();
}

async function promptNews2Next(ctx) {
  await replySafe(ctx,
    'Добавьте текст (/text) или картинку (/image). ' +
    'Завершить — /done, отменить — /cancel.'
  );
}

// Показ одной новости с сохранением порядка блоков.
// Если блоков нет (старая новость) — печатаем текст.
async function showNews(ctx, newsDoc) {
  const idStr = String(newsDoc._id);
  const when = newsDoc.createdAt ? new Date(newsDoc.createdAt).toLocaleString() : '';
  const head = ['Новость', `ID: ${idStr}`];
  if (when) head.push(`Дата: ${when}`);
  await replySafe(ctx, head.join('\n'));

  const blocks = Array.isArray(newsDoc.blocks) ? newsDoc.blocks : [];
  if (!blocks.length) {
    const text = (newsDoc.text || '').trim() || '(пусто)';
    await replySafe(ctx, text);
    return;
  }

  // Идём по блокам в порядке добавления.
  // Последовательные изображения группируем в mediaGroup пачками по 10.
  const flushMediaGroup = async (arr) => {
    if (!arr.length) return;
    if (arr.length === 1) {
      await replyWithPhotoSafe(ctx, arr[0].media);
    } else {
      await telegramCallWithRetry(ctx, () =>
        ctx.replyWithMediaGroup(arr.map(m => ({ type: 'photo', media: m.media })))
      );
    }
  };

  let acc = [];
  for (const b of blocks) {
    if (b.type === 'text') {
      // Сначала выгрузим накопленные фото
      await flushMediaGroup(acc); acc = [];
      await replySafe(ctx, String(b.text || '').trim());
    } else if (b.type === 'image' && b.image) {
      const media = b.image.tgFileId
        ? b.image.tgFileId
        : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, b.image.relPath)) };
      acc.push({ media });
      if (acc.length === 10) { await flushMediaGroup(acc); acc = []; }
    }
  }
  await flushMediaGroup(acc);
}

// --- Time helpers (Moscow TZ) ---
function toMoscowIso(datePart /* YYYY-MM-DD */, timePart /* HH:MM */) {
  // Москва круглый год UTC+3
  return `${datePart}T${timePart}:00+03:00`;
}
function toUnixTsFromMoscow(datePart, timePart) {
  const iso = toMoscowIso(datePart, timePart);
  const d = new Date(iso);
  return Number.isFinite(d.getTime()) ? d.getTime() : null;
}

function formatMoscowDateTime2(date = new Date()) {
  const pad = n => String(n).padStart(2, '0');
  // Получаем время в МСК без смены системной TZ
  const dt = new Date(date.toLocaleString('en-US', { timeZone: 'Europe/Moscow' }));
  const Y = dt.getFullYear();
  const M = pad(dt.getMonth() + 1);
  const D = pad(dt.getDate());
  const h = pad(dt.getHours());
  const m = pad(dt.getMinutes());
  const s = pad(dt.getSeconds());
  return `${D}.${M}.${Y}, ${h}:${m}:${s} (МСК)`;
}

// --- Safe outbound send helpers (rate-limit aware) ---

// Минимальный интервал между исходящими сообщениями в один и тот же чат (мс).
// Можно переопределить через ENV: OUTBOUND_MIN_INTERVAL_MS
const OUTBOUND_MIN_INTERVAL_MS = Number(process.env.OUTBOUND_MIN_INTERVAL_MS || 1100);

// Последнее время отправки сообщения по каждому чату
const outboundChatPace = new Map(); // chatId -> timestamp

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

async function paceChatSend(ctx, minMs = OUTBOUND_MIN_INTERVAL_MS) {
  const chatId = ctx.chat?.id;
  if (!chatId || minMs <= 0) return;
  const last = outboundChatPace.get(chatId) || 0;
  const now = Date.now();
  const wait = last ? Math.max(0, minMs - (now - last)) : 0;
  if (wait > 0) await sleep(wait);
  outboundChatPace.set(chatId, Date.now());
}

/**
 * Универсальный вызов Telegram API с повтором при 429 Too Many Requests.
 * executor: () => Promise<any> — функция, делающая собственно вызов (например, () => ctx.reply(...))
 */
async function telegramCallWithRetry(ctx, executor, opts = {}) {
  const {
    maxRetries = 6,
    baseDelayMs = 1000,
    jitterMs = 150,
  } = opts;

  let attempt = 0;
  while (true) {
    // Пейсинг по чату (1 сообщение ~ в секунду по умолчанию)
    await paceChatSend(ctx);
    try {
      const res = await executor();
      return res;
    } catch (e) {
      const is429 = e && (
        e.code === 429 ||
        e.response?.error_code === 429 ||
        /Too Many Requests/i.test(String(e.description || e.message))
      );
      if (!is429) throw e;

      const retryAfterSec =
        Number(e.parameters?.retry_after) ||
        Number(e.response?.parameters?.retry_after) ||
        Number((e.on && e.on.payload && e.on.payload.retry_after)) ||
        1;

      attempt++;
      if (attempt > maxRetries) throw e;

      const delay = Math.max(retryAfterSec * 1000, baseDelayMs) + Math.floor(Math.random() * jitterMs);

      // Сдвигаем вперёд "последнюю отправку" — чтобы следующий вызов тоже подождал
      const chatId = ctx.chat?.id;
      if (chatId) outboundChatPace.set(chatId, Date.now() + delay);

      await sleep(delay);
      continue;
    }
  }
}

async function replySafe(ctx, text, extra = {}) {
  if (!text) return;
  return telegramCallWithRetry(ctx, () =>
    ctx.reply(text, { disable_web_page_preview: true, ...extra })
  );
}

async function replyWithPhotoSafe(ctx, media, extra = {}) {
  return telegramCallWithRetry(ctx, () =>
    ctx.replyWithPhoto(media, extra)
  );
}


// Crypto-random helpers
function randInt(maxExclusive) {
  return crypto.randomInt(0, maxExclusive);
}
function shuffle(arr) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = randInt(i + 1);
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// Permissions
function isOwner(userId) {
  return OWNER_IDS.includes(Number(userId));
}

// We store chat-level admins in colAdmins: { chatId, admins: [{ userId?, username? }] }
async function getChatAdmins(chatId) {
  const doc = await colAdmins.findOne({ chatId });
  return doc?.admins || [];
}
async function setChatAdmins(chatId, admins) {
  await colAdmins.updateOne(
    { chatId },
    { $set: { chatId, admins } },
    { upsert: true }
  );
}

// Роли: хранение по пользователю в чате
// colRoles: { chatId, userId, username?: string, roles: [string], updatedAt }
async function addUserRole(chatId, user, roleName) {
  const userId = Number(user.userId);
  const username = user.username || null;

  const role = String(roleName).trim();

  await colRoles.updateOne(
    { chatId, userId },
    {
      // Вставляем базовые поля только при upsert
      $setOnInsert: { chatId, userId },
      // Обновляем username и метку времени всегда
      $set: { username, updatedAt: new Date() },
      // Добавляем роль (создаст массив, если поле отсутствует)
      $addToSet: { roles: role },
    },
    { upsert: true }
  );
}

async function hasUserRole(chatId, userId, roleName) {
  const doc = await colRoles.findOne({ chatId, userId: Number(userId) }, { projection: { roles: 1 } });
  if (!doc?.roles?.length) return false;
  return doc.roles.some(r => r.toLowerCase() === String(roleName).toLowerCase());
}

function isSupportedRoleName(roleName = '') {
  const v = String(roleName).trim().toLowerCase();
  return v === 'achievements' || v === 'news';
}

function normalizeRoleName(roleName = '') {
  const v = String(roleName).trim().toLowerCase();
  if (v === 'news') return NEWS_ROLE;
  // по умолчанию считаем Achievements
  return ACHIEVEMENTS_ROLE;
}



// Удаление роли у пользователя (и удаляем документ, если ролей не осталось)
async function removeUserRole(chatId, user, roleName) {
  const userId = Number(user.userId);
  const role = normalizeRoleName(roleName);

  await colRoles.updateOne(
    { chatId, userId },
    { $pull: { roles: role }, $set: { updatedAt: new Date() } }
  );

  const doc = await colRoles.findOne({ chatId, userId }, { projection: { roles: 1 } });
  if (doc && (!Array.isArray(doc.roles) || doc.roles.length === 0)) {
    await colRoles.deleteOne({ chatId, userId });
  }
}

// Табличка ролей: группируем пользователей по ролям
function formatRolesTable(docs = []) {
  if (!docs.length) return 'Roles: (none)';

  const byRole = new Map(); // role -> [{userId, username}]
  for (const d of docs) {
    const roles = Array.isArray(d.roles) ? d.roles : [];
    for (const r of roles) {
      const role = normalizeRoleName(r);
      if (!byRole.has(role)) byRole.set(role, []);
      byRole.get(role).push({ userId: d.userId, username: d.username || null });
    }
  }

  // Сортируем роли и пользователей внутри
  const rolesSorted = Array.from(byRole.keys()).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  const lines = ['Roles:'];
  for (const role of rolesSorted) {
    lines.push(`${role}:`);
    const users = byRole.get(role).slice().sort((a, b) => {
      const ua = a.username ? a.username.toLowerCase() : '';
      const ub = b.username ? b.username.toLowerCase() : '';
      if (ua && ub) return ua.localeCompare(ub);
      if (ua || ub) return ua ? -1 : 1;
      return Number(a.userId) - Number(b.userId);
    });
    if (!users.length) {
      lines.push('  (none)');
      continue;
    }
    for (const u of users) {
      const label = u.username ? `@${u.username}` : `#${u.userId}`;
      lines.push(`  - ${label}`);
    }
  }
  return lines.join('\n');
}

async function isAchievementsEditor(ctx) {
  const chatId = getEffectiveChatId(ctx);
  if (await isChatAdminOrOwner(ctx, chatId)) return true;
  if (!ctx.from?.id) return false;
  return hasUserRole(chatId, ctx.from.id, ACHIEVEMENTS_ROLE);
}

async function requireAchievementsGuard(ctx, opts = {}) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в контексте чатов (группы или личные), не в каналах.');
    return false;
  }

  const chatId = getEffectiveChatId(ctx);

  if (!(await isAchievementsEditor(ctx))) {
    await ctx.reply('Недостаточно прав. Требуется админ целевого чата или роль Achievements.');
    return false;
  }

  // Блокируем изменения ачивок, если турнир залочен (если не ignoreLock)
  if (!opts.ignoreLock) {
    if (await isTournamentLocked(chatId)) {
      await ctx.reply('Турнир заблокирован для изменений.');
      return false;
    }
  }

  return true;
}

async function isNewsEditor(ctx) {
  const chatId = getEffectiveChatId(ctx);
  // админ / владелец чата — всегда имеет права на новости
  if (await isChatAdminOrOwner(ctx, chatId)) return true;
  if (!ctx.from?.id) return false;
  // отдельная роль News
  return hasUserRole(chatId, ctx.from.id, NEWS_ROLE);
}

async function requireNewsGuard(ctx, opts = {}) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в контексте чатов (группы или личные), не в каналах.');
    return false;
  }
  const chatId = getEffectiveChatId(ctx);

  if (!(await isNewsEditor(ctx))) {
    await ctx.reply('Недостаточно прав. Требуется админ целевого чата или роль News.');
    return false;
  }

  if (!opts.ignoreLock) {
    if (await isTournamentLocked(chatId)) {
      await ctx.reply('Турнир заблокирован для изменений. Разрешено только чтение данных.');
      return false;
    }
  }

  return true;
}

function userMatchesAdmin(user, adminEntry) {
  // Match by userId if present; else by username (case-insensitive)
  if (adminEntry.userId && user.id) {
    if (Number(adminEntry.userId) === Number(user.id)) return true;
  }
  if (adminEntry.username && user.username) {
    if (norm(adminEntry.username) === norm(user.username)) return true;
  }
  return false;
}

// ПОЛНАЯ ЗАМЕНА функций прав — проверяем права по ЦЕЛЕВОМУ чату (getEffectiveChatId)
async function isChatAdminOrOwner(ctx, targetChatId = null) {
  const userId = ctx.from?.id;
  if (isOwner(userId)) return true;

  const chatId = targetChatId ?? getEffectiveChatId(ctx);
  const admins = await getChatAdmins(chatId);
  return admins.some(a => userMatchesAdmin(ctx.from, a));
}

function requireGroupContext(ctx) {
  return ctx.chat && (
    ctx.chat.type === 'group' ||
    ctx.chat.type === 'supergroup' ||
    ctx.chat.type === 'private' // PM тоже ок
  );
}

// --- NEW: helpers for tournament lock (Chats.locked) ---

async function isTournamentLocked(chatId) {
  const doc = await colChats.findOne(
    { chatId },
    { projection: { locked: 1 } }
  );
  return Boolean(doc?.locked);
}

async function setTournamentLocked(chatId, locked) {
  await colChats.updateOne(
    { chatId },
    {
      $set: {
        chatId,
        locked: !!locked,
      },
    },
    { upsert: true }
  );
}

// --- UPDATED: admin guard с учётом lock ---

async function requireAdminGuard(ctx, opts = {}) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в контексте чатов (группы или личные), не в каналах.');
    return false;
  }

  const chatId = getEffectiveChatId(ctx);

  if (!(await isChatAdminOrOwner(ctx, chatId))) {
    await ctx.reply(`Недостаточно прав. Требуются владельцы или администраторы целевого чата #${chatId}.`);
    return false;
  }

  // Если не указано ignoreLock:true — режем все изменения при locked = true
  if (!opts.ignoreLock) {
    if (await isTournamentLocked(chatId)) {
      await ctx.reply('Турнир заблокирован для изменений.');
      return false;
    }
  }

  return true;
}

// Chat settings
// ПОЛНАЯ ЗАМЕНА getChatSettings (добавлено поле tournamentNewsChannel)
// Chat settings
// ПОЛНАЯ ЗАМЕНА getChatSettings (добавлено: tournamentStatsUrl, tournamentStatsEnabled)
async function getChatSettings(chatId) {
  const doc = await colChats.findOne({ chatId });
  const baseMax = doc?.maxPlayers || DEFAULT_MAX_PLAYERS;
  return {
    maxPlayers: baseMax,
    groupsAlgo: doc?.groupsAlgo || 1,
    minPlayers2: doc?.minPlayers2 ?? 1,
    recPlayers2: doc?.recPlayers2 ?? baseMax,
    maxCount3: doc?.maxCount3 ?? null,
    finalMaxPlayers: doc?.finalMaxPlayers || baseMax,
    finalTotalPlayers: doc?.finalTotalPlayers ?? null,
    finalsAlgo: doc?.finalsAlgo || 1,

    // Superfinals
    superfinalMaxPlayers: doc?.superfinalMaxPlayers || baseMax,
    superfinalTotalPlayers: doc?.superfinalTotalPlayers ?? null,
    superfinalsAlgo: doc?.superfinalsAlgo || 1,

    // Турнир
    tournamentName: doc?.tournamentName || null,
    tournamentSite: doc?.tournamentSite || null,
    tournamentDesc: doc?.tournamentDesc || null,

    // Новые поля турнира
    tournamentServers: Array.isArray(doc?.tournamentServers) ? doc.tournamentServers : [],
    tournamentPack: doc?.tournamentPack || null,
    tournamentStreams: Array.isArray(doc?.tournamentStreams) ? doc.tournamentStreams : [],

    // Привязанный новостной канал
    tournamentNewsChannel: doc?.tournamentNewsChannel || null,

    // Новые поля: персональная статистика (хранятся в БД, не в .env)
    tournamentStatsUrl: doc?.tournamentStatsUrl || null,           // строка или null (пусто по умолчанию)
    tournamentStatsEnabled: Boolean(doc?.tournamentStatsEnabled),    // false по умолчанию

    // NEW: признак блокировки турнира
    locked: Boolean(doc?.locked),
  };
}


async function setChatSettings(chatId, patch) {
  await colChats.updateOne(
    { chatId },
    { $set: { chatId, ...patch } },
    { upsert: true }
  );
}

// --- Registration settings (по турниру / chatId) ---

async function getRegistrationSettings(chatId) {
  const doc = await colRegistrationSettings.findOne({ chatId });

  return {
    chatId,
    tournamentType: doc?.tournamentType || null,          // 'FFA' | '1v1' | 'TDM' | null
    registrationEnabled: !!doc?.registrationEnabled,
    registrationOpenedAt: doc?.registrationOpenedAt || null,
    registrationClosedAt: doc?.registrationClosedAt || null,
    maxPlayers: doc?.maxPlayers ?? null,
    deadline: doc?.deadline || null,
    createdAt: doc?.createdAt || null,
    updatedAt: doc?.updatedAt || null,
  };
}

async function updateRegistrationSettings(chatId, patch = {}) {
  const now = new Date();
  await colRegistrationSettings.updateOne(
    { chatId },
    {
      $set: {
        ...patch,
        updatedAt: now,
      },
      $setOnInsert: {
        chatId,
        createdAt: now,
      },
    },
    { upsert: true },
  );
}

function parseMoscowDateTime(str) {
  if (!str) return null;
  const m = str.trim().match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})$/);
  if (!m) return null;
  const [, year, month, day, hour, minute] = m;
  // Строим ISO-строку с часовым поясом +03:00 (МСК)
  const iso = `${year}-${month}-${day}T${hour}:${minute}:00+03:00`;
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return null;
  return dt;
}

function formatMoscowDateTime(dt) {
  if (!dt) return '(not set)';
  try {
    const d = new Date(dt);
    // Приводим к МСК
    const s = d.toLocaleString('ru-RU', {
      timeZone: 'Europe/Moscow',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
    // по умолчанию формат "DD.MM.YYYY, HH:MM", уберём запятую
    return s.replace(',', '');
  } catch {
    return String(dt);
  }
}


// Seen usernames index (helps resolving @username -> userId if seen before)
async function recordUser(ctx, user) {
  if (!user) return;
  const rec = {
    userId: Number(user.id),
    username: user.username || null,
    first_name: user.first_name || null,
    last_name: user.last_name || null,
    ts: new Date(),
  };
  await colUserIndex.updateOne(
    { userId: rec.userId },
    { $set: rec },
    { upsert: true }
  );
}

// ДОБАВИТЬ НОВЫЕ ХЕЛПЕРЫ для постинга новостей в канал (разместите рядом с другими helpers)

// ПОЛНАЯ ЗАМЕНА: helpers для отправки новостей в канал
// Разместите рядом с другими helper-функциями

// Возвращает идентификатор канала для отправки (строка @channel или numeric id), либо null
async function getNewsChannelTarget(chatId) {
  const s = await getChatSettings(chatId);
  const ch = s.tournamentNewsChannel;
  if (!ch) return null;
  if (typeof ch === 'string' && ch.trim()) return ch.trim();
  if (typeof ch === 'number') return ch;
  return null;
}

function newsScopeLabel(scope) {
  switch (scope) {
    case 'tournament': return 'Турнирные новости';
    case 'group': return 'Новости квалификаций';
    case 'final': return 'Новости финального раунда';
    case 'superfinal': return 'Новости суперфинала';
    default: return 'Новости';
  }
}

// Формирование текста поста для канала по любой новости
function formatNewsForChannel(settings, scope, newsDoc) {
  const tn = settings.tournamentName || 'Турнир';
  const lines = [];
  lines.push(`[${tn}] ${newsScopeLabel(scope)}`);
  lines.push('');
  if (newsDoc?.text) lines.push(newsDoc.text);
  if (settings.tournamentSite) {
    lines.push('');
    lines.push(`Подробнее: ${settings.tournamentSite}`);
  }
  return lines.join('\n').trim();
}

// Отправка одной новости указанного scope в канал (если канал задан)
async function postOneNewsToChannel(chatId, scope, doc) {
  try {
    const target = await getNewsChannelTarget(chatId);
    if (!target) return { posted: 0, error: 'Канал не задан' };

    const text = (doc?.text || '').trim();
    const hasImg = !!doc?.news_img_file_name;

    if (hasImg) {
      // путь до сохранённой картинки новости
      const abs = path.join(SCREENSHOTS_DIR, String(chatId), 'news', doc.news_img_file_name);
      // отправляем фото с подписью
      await bot.telegram.sendPhoto(
        target,
        { source: fs.createReadStream(abs) },
        text ? { caption: text } : {}
      );
      return { posted: 1 };
    } else {
      // обычная текстовая новость
      await bot.telegram.sendMessage(target, text || '(пустая новость)');
      return { posted: 1 };
    }
  } catch (e) {
    console.error('postOneNewsToChannel error', e);
    return { posted: 0, error: String(e?.message || e) };
  }
}


// Опубликовать все новости всех стадий в канал (tournament, group, final, superfinal)
async function postAllNewsToChannel(chatId) {
  const target = await getNewsChannelTarget(chatId);
  if (!target) return { error: 'Канал не задан' };

  const settings = await getChatSettings(chatId);

  // Собираем все новости по всем scope
  const scopes = ['tournament', 'group', 'final', 'superfinal'];
  const all = [];
  for (const sc of scopes) {
    // eslint-disable-next-line no-await-in-loop
    const arr = await colNews.find({ chatId, scope: sc }).sort({ createdAt: 1 }).toArray(); // от старых к новым
    for (const n of arr) all.push({ scope: sc, n });
  }

  // Если хотите строгую хронологию между всеми scope — отсортируем по createdAt
  all.sort((a, b) => {
    const ta = a.n?.createdAt ? new Date(a.n.createdAt).getTime() : 0;
    const tb = b.n?.createdAt ? new Date(b.n.createdAt).getTime() : 0;
    return ta - tb;
  });

  let posted = 0;
  for (const { scope, n } of all) {
    const text = formatNewsForChannel(settings, scope, n);
    try {
      // eslint-disable-next-line no-await-in-loop
      await bot.telegram.sendMessage(target, text, { disable_web_page_preview: false });
      posted++;
    } catch (e) {
      console.error('postAllNewsToChannel send error:', e);
    }
  }
  return { posted };
}


// ---- Points: groups
async function getGroupPoints(chatId) {
  const doc = await colGroupPoints.findOne({ chatId });
  return doc?.points || []; // [{nameNorm,nameOrig,pts}]
}
async function setGroupPoints(chatId, points) {
  await colGroupPoints.updateOne(
    { chatId },
    { $set: { chatId, points, updatedAt: new Date() } },
    { upsert: true }
  );
}
function groupPointsToMap(arr = []) {
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}

// ---- Points: finals
async function getFinalPoints(chatId) {
  const doc = await colFinalPoints.findOne({ chatId });
  return doc?.points || [];
}
async function setFinalPoints(chatId, points) {
  await colFinalPoints.updateOne(
    { chatId },
    { $set: { chatId, points, updatedAt: new Date() } },
    { upsert: true }
  );
}

function finalPointsToMap(arr = []) {
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}

async function getSuperFinalPoints(chatId) {
  const col = db.collection('super_final_points');
  const doc = await col.findOne({ chatId });
  return doc?.points || []; // [{nameNorm,nameOrig,pts}]
}
async function setSuperFinalPoints(chatId, points) {
  const col = db.collection('super_final_points');
  await col.updateOne(
    { chatId },
    { $set: { chatId, points, updatedAt: new Date() } },
    { upsert: true }
  );
}
function superFinalPointsToMap(arr = []) {
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}

// ---- Parser: "name[points]" list
function parsePointsList(tail) {
  const raw = tail.split(',').map(s => s.trim()).filter(Boolean);
  if (!raw.length) return [];
  const out = [];
  for (const item of raw) {
    const m = item.match(/^(.+?)\[(\d+)\]$/);
    if (!m) {
      return { error: `Некорректный формат "${item}". Используйте: name[points]` };
    }
    const name = m[1].trim();
    const pts = Number(m[2]);
    if (!Number.isInteger(pts) || pts < 0) {
      return { error: `Очки для "${name}" должны быть неотрицательным целым числом.` };
    }
    out.push({ nameOrig: name, nameNorm: norm(name), pts });
  }
  // Удаляем дубли по нормализованному имени — берём первое
  const seen = new Set();
  const dedup = [];
  for (const p of out) {
    if (!seen.has(p.nameNorm)) {
      seen.add(p.nameNorm);
      dedup.push(p);
    }
  }
  return dedup;
}

// --- Map results parsing helpers ---

function splitTopLevelByComma(s = '') {
  const parts = [];
  let buf = '';
  let depth = 0;
  for (const ch of s) {
    if (ch === '[') depth++;
    if (ch === ']') depth = Math.max(0, depth - 1);
    if (ch === ',' && depth === 0) {
      const trimmed = buf.trim();
      if (trimmed) parts.push(trimmed);
      buf = '';
    } else {
      buf += ch;
    }
  }
  const last = buf.trim();
  if (last) parts.push(last);
  return parts;
}

// str: "pp[41,25,62,360,5370,3963],slonik[29,21,58,254,3408,3449]"
function parseMapResultPlayers(str) {
  if (!str || !str.trim()) {
    return { error: 'Не указан список игроков. Формат: name[frags,kills,eff,fph,dgiv,drec],...' };
  }
  const rawItems = splitTopLevelByComma(str);
  if (!rawItems.length) {
    return { error: 'Не удалось разобрать список игроков. Проверьте формат.' };
  }

  const players = [];
  for (const item of rawItems) {
    const m = item.match(/^(.+?)\[(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\]$/);
    if (!m) {
      return { error: `Некорректный формат "${item}". Ожидается name[frags,kills,eff,fph,dgiv,drec]` };
    }
    const nameOrig = m[1].trim();
    if (!nameOrig) {
      return { error: `Пустое имя игрока в фрагменте "${item}".` };
    }
    const nums = m.slice(2).map(x => Number(x));
    if (nums.some(n => !Number.isFinite(n))) {
      return { error: `Некорректные числовые значения в "${item}".` };
    }
    const [frags, kills, eff, fph, dgiv, drec] = nums;
    players.push({
      nameOrig,
      nameNorm: norm(nameOrig),
      frags,
      kills,
      eff,
      fph,
      dgiv,
      drec,
    });
  }

  return { players };
}

// -----------

// Ключ: `${chatId}:${userId}`; значение: { mode: 'add'|'edit', buffer: string[] }
const feedbackSessions = new Map();
const fbKey = (chatId, userId) => `${chatId}:${userId}`;

function feedbackSessionKey(chatId, userId) {
  return `${chatId}:${userId}`;
}

// --- Профили пользователей (глобальные) ---
// Ключ: userId (Telegram ID), значение: { mode: 'add'|'edit', step: 'nick'|'bio', nick, nickNorm, bio, startedAt }
const userProfileSessions = new Map();
function userProfileKey(userId) {
  return String(userId);
}

// --- Игровые команды (глобальные) ---

// Сессии создания/редактирования команд
// Ключ: userId (Telegram ID)
const teamSessions = new Map();
function teamSessionKey(userId) {
  return String(userId);
}

// Сессии выбора команды для редактирования (/teams edit, если команд несколько)
// Ключ: userId (Telegram ID)
const teamSelectSessions = new Map();
function teamSelectKey(userId) {
  return String(userId);
}

// Сессии выбора команды для /signup add (TDM, когда у игрока несколько команд)
const signupTeamSelectSessions = new Map();
function signupTeamSelectKey(chatId, userId) {
  return `signupTeam:${chatId}:${userId}`;
}

// Сессии мастера регистрации /register (регистрация игрока/команды с автоподачей заявки)
// Ключ: `${chatId}:${userId}`
const signupWizardSessions = new Map();
function signupWizardKey(chatId, userId) {
  return `${chatId}:${userId}`;
}

// Разрешения на использование /register (выдаются только после /signup)
// Ключ: `${chatId}:${userId}`
const signupRegisterAllowed = new Map();
function signupRegisterKey(chatId, userId) {
  return `${chatId}:${userId}`;
}


// -------- Achievements (ачивки) --------

function achvKey(chatId, userId) { return `${chatId}:${userId}`; }

// Временные черновики (после /done изображения ждём текст описания)
const achvDrafts = new Map(); // key = chatId:userId -> { chatId, userId, name, image: {...}, startedAt }

// Новые сессии редактирования (name/desc, а также цепочка all)
const achvEditSessions = new Map(); // key = chatId:userId -> { chatId, userId, idx, mode: 'name'|'desc', chain?: true }

async function listAchievements(chatId) {
  return colAchievements.find({ chatId }).sort({ idx: 1 }).toArray();
}

async function getAchievement(chatId, idx) {
  return colAchievements.findOne({ chatId, idx: Number(idx) });
}

async function getNextAchievementIdx(chatId) {
  const last = await colAchievements.find({ chatId }).sort({ idx: -1 }).limit(1).toArray();
  return last.length ? Number(last[0].idx) + 1 : 1;
}

async function addAchievement(chatId, data) {
  // data: { name, desc, image:{relPath,mime,size,tgFileId,tgUniqueId}, createdBy, type? }
  const idx = await getNextAchievementIdx(chatId);
  const doc = {
    chatId,
    idx,
    name: data.name,
    desc: data.desc || '',
    image: data.image || null,
    type: normalizeAchievementType(data.type || 'achievement'),
    createdBy: data.createdBy || null,
    createdAt: new Date(),
    updatedAt: new Date(),
  };
  await colAchievements.insertOne(doc);
  return idx;
}


async function deleteAchievement(chatId, idx) {
  const ach = await getAchievement(chatId, idx);
  if (!ach) return { ok: false, notFound: true };
  // удалить файл
  if (ach.image?.relPath) {
    const abs = path.join(SCREENSHOTS_DIR, ach.image.relPath);
    try { await fs.promises.unlink(abs); } catch (_) { /* ignore */ }
  }
  await colAchievements.deleteOne({ chatId, idx: Number(idx) });
  // перенумерация: всем > idx — idx--
  await colAchievements.updateMany({ chatId, idx: { $gt: Number(idx) } }, { $inc: { idx: -1 } });
  return { ok: true };
}

async function deleteAllAchievements(chatId) {
  const all = await colAchievements.find({ chatId }).toArray();
  for (const a of all) {
    if (a.image?.relPath) {
      const abs = path.join(SCREENSHOTS_DIR, a.image.relPath);
      try { await fs.promises.unlink(abs); } catch (_) { /* ignore */ }
    }
  }
  const res = await colAchievements.deleteMany({ chatId });
  return res.deletedCount || 0;
}

async function showAchievement(ctx, ach) {
  const typeStr = ach.type ? String(ach.type) : 'achievement';
  const header = [`${ach.idx}) [${typeStr}] ${ach.name}`];
  if (ach.player?.nameOrig) {
    header.push(`Player: ${ach.player.nameOrig}`);
  }

  // Заголовок
  await replySafe(ctx, header.join('\n'));

  // Лого (если есть)
  if (ach.image) {
    const media = ach.image.tgFileId
      ? ach.image.tgFileId
      : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, ach.image.relPath)) };
    await replyWithPhotoSafe(ctx, media);
  }

  // Описание
  const desc = ach.desc && ach.desc.trim() ? ach.desc : '(no description)';
  await replySafe(ctx, desc);
}

async function showAllAchievements(ctx, chatId) {
  const arr = await listAchievements(chatId);
  if (!arr.length) {
    await replySafe(ctx, 'Achievements: (none)');
    return;
  }

  // Опциональное вступление (чтобы пользователь понимал, что сейчас придёт много сообщений)
  await replySafe(ctx, `Achievements: ${arr.length} item(s). Будет отправлено несколько сообщений, пожалуйста, подождите.`);

  // Последовательно выводим каждую ачивку.
  // Внутри showAchievement используются безопасные методы с ретраями и троттлингом.
  for (const a of arr) {
    // eslint-disable-next-line no-await-in-loop
    await showAchievement(ctx, a);
  }
}


function isValidAchievementType(t) {
  const v = String(t || '').trim().toLowerCase();
  return v === 'achievement' || v === 'perc';
}
function normalizeAchievementType(t) {
  return isValidAchievementType(t) ? String(t).trim().toLowerCase() : 'achievement';
}


// Chat migration handler
async function migrateChat(oldId, newId) {
  const updates = [];
  updates.push(colChats.updateOne({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colAdmins.updateOne({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colSkillGroups.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colMaps.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colGameGroups.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colFinalGroups.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colCounters.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colWaitingPlayers.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colRatings.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));

  // NEW:
  updates.push(colCustomGroups.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));
  updates.push(colCustomPoints.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));

  updates.push(colAchievements.updateMany({ chatId: oldId }, { $set: { chatId: newId } }));

  await Promise.all(updates);
}

// Helpers for Skill Groups
async function getSkillGroup(chatId, sgNum) {
  return colSkillGroups.findOne({ chatId, groupNumber: Number(sgNum) });
}
async function listSkillGroups(chatId) {
  const cur = colSkillGroups.find({ chatId }).sort({ groupNumber: 1 });
  return cur.toArray();
}
async function upsertSkillGroup(chatId, sgNum, players) {
  await colSkillGroups.updateOne(
    { chatId, groupNumber: Number(sgNum) },
    { $set: { chatId, groupNumber: Number(sgNum), players: players || [] } },
    { upsert: true }
  );
}
async function delSkillGroup(chatId, sgNum) {
  await colSkillGroups.deleteOne({ chatId, groupNumber: Number(sgNum) });
}
async function delAllSkillGroups(chatId) {
  await colSkillGroups.deleteMany({ chatId });
}

// Helpers for Maps
async function addMaps(chatId, maps) {
  if (!maps.length) return { added: [], skippedExists: [] };
  const existing = await colMaps.find({ chatId, nameNorm: { $in: maps.map(m => m.nameNorm) } }).toArray();
  const existsSet = new Set(existing.map(m => m.nameNorm));
  const toInsert = maps.filter(m => !existsSet.has(m.nameNorm));
  const skippedExists = maps.filter(m => existsSet.has(m.nameNorm)).map(m => m.nameOrig);
  if (toInsert.length) {
    await colMaps.insertMany(toInsert.map(m => ({ chatId, nameNorm: m.nameNorm, nameOrig: m.nameOrig })));
  }
  return { added: toInsert.map(m => m.nameOrig), skippedExists };
}
async function removeMaps(chatId, maps) {
  const existing = await colMaps.find({ chatId, nameNorm: { $in: maps.map(m => m.nameNorm) } }).toArray();
  const existSet = new Set(existing.map(m => m.nameNorm));
  const toRemoveNorms = maps.filter(m => existSet.has(m.nameNorm)).map(m => m.nameNorm);
  const notFound = maps.filter(m => !existSet.has(m.nameNorm)).map(m => m.nameOrig);
  if (toRemoveNorms.length) {
    await colMaps.deleteMany({ chatId, nameNorm: { $in: toRemoveNorms } });
  }
  return { removed: existing.map(m => m.nameOrig), notFound };
}
async function listMaps(chatId) {
  const cur = colMaps.find({ chatId }).sort({ nameNorm: 1 });
  return cur.toArray();
}
async function delAllMaps(chatId) {
  await colMaps.deleteMany({ chatId });
}

// Counters for Group IDs (reserved)
async function getNextGroupId(chatId) {
  const res = await colCounters.findOneAndUpdate(
    { chatId, key: 'groupId' },
    { $inc: { value: 1 } },
    { upsert: true, returnDocument: 'after' }
  );
  return res.value.value;
}
async function resetGroupIdCounter(chatId) {
  await colCounters.deleteOne({ chatId, key: 'groupId' });
}

// Game Groups
async function listGameGroups(chatId) {
  const cur = colGameGroups.find({ chatId }).sort({ groupId: 1 });
  return cur.toArray();
}
async function getGameGroup(chatId, groupId) {
  return colGameGroups.findOne({ chatId, groupId: Number(groupId) });
}
async function upsertGameGroup(chatId, groupId, data) {
  await colGameGroups.updateOne(
    { chatId, groupId: Number(groupId) },
    { $set: { chatId, groupId: Number(groupId), ...data } },
    { upsert: true }
  );
}
async function deleteGameGroup(chatId, groupId) {
  await colGameGroups.deleteOne({ chatId, groupId: Number(groupId) });
}
async function deleteAllGameGroups(chatId) {
  await colGameGroups.deleteMany({ chatId });
  await resetGroupIdCounter(chatId);
}

// Finals
async function listFinalGroups(chatId) {
  return colFinalGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}
async function upsertFinalGroup(chatId, groupId, data) {
  await colFinalGroups.updateOne(
    { chatId, groupId: Number(groupId) },
    { $set: { chatId, groupId: Number(groupId), ...data } },
    { upsert: true }
  );
}
async function deleteAllFinalGroups(chatId) {
  await colFinalGroups.deleteMany({ chatId });
}

// Waiting list (for algo=2/3)
async function getWaitingPlayers(chatId) {
  const doc = await colWaitingPlayers.findOne({ chatId });
  return doc?.players || [];
}
async function setWaitingPlayers(chatId, players) {
  await colWaitingPlayers.updateOne(
    { chatId },
    { $set: { chatId, players: players || [] } },
    { upsert: true }
  );
}
async function clearWaitingPlayers(chatId) {
  await colWaitingPlayers.deleteOne({ chatId });
}
async function removeFromWaiting(chatId, norms) {
  if (!norms?.length) return;
  const doc = await colWaitingPlayers.findOne({ chatId });
  if (!doc?.players?.length) return;
  const set = new Set(norms);
  const next = doc.players.filter(p => !set.has(p.nameNorm));
  await setWaitingPlayers(chatId, next);
}

// Rating storage
async function getRating(chatId) {
  const doc = await colRatings.findOne({ chatId });
  return doc?.players || [];
}
async function setRating(chatId, players) {
  // players: [{ nameOrig, nameNorm }]
  const rated = players.map((p, i) => ({ ...p, rank: i + 1 }));
  await colRatings.updateOne(
    { chatId },
    { $set: { chatId, players: rated, updatedAt: new Date() } },
    { upsert: true }
  );
}
function formatRatingList(players = []) {
  if (!players.length) return 'Rating: (none)';
  const sorted = players.slice().sort((a, b) => (a.rank ?? 9999) - (b.rank ?? 9999));
  const lines = sorted.map(p => `${p.rank}) ${p.nameOrig}`);
  return `Rating:\n${lines.join('\n')}`;
}

// Validation helpers
async function playerExistsInAnySkillGroup(chatId, nameNorm) {
  const sg = await colSkillGroups.findOne({ chatId, 'players.nameNorm': nameNorm });
  return Boolean(sg);
}
async function findSkillGroupForPlayer(chatId, nameNorm) {
  return colSkillGroups.findOne({ chatId, 'players.nameNorm': nameNorm }, { projection: { groupNumber: 1 } });
}
async function playerExistsInAnyGameGroup(chatId, nameNorm) {
  const gg = await colGameGroups.findOne({ chatId, 'players.nameNorm': nameNorm });
  return Boolean(gg);
}

// Parsers
function parseCommandArgs(text) {
  if (!text) return '';
  const idx = text.indexOf(' ');
  if (idx === -1) return '';
  return text.slice(idx + 1).trim();
}

function extractUserRef(ctx, arg) {
  if (ctx.message && ctx.message.reply_to_message) {
    const u = ctx.message.reply_to_message.from;
    if (u) return { userId: Number(u.id), username: u.username || null };
  }
  if (!arg) return null;
  const trimmed = arg.trim();
  if (/^\d+$/.test(trimmed)) {
    return { userId: Number(trimmed), username: null };
  }
  const m = trimmed.match(/^@?([A-Za-z0-9_]{5,})$/);
  if (m) {
    return { userId: null, username: m[1] };
  }
  return null;
}

function formatAdminsList(admins) {
  if (!admins.length) return 'current admins list: (empty)';
  const items = admins.map(a => {
    if (a.username && a.userId) return `@${a.username} (#${a.userId})`;
    if (a.username) return `@${a.username}`;
    if (a.userId) return `#${a.userId}`;
    return '(unknown)';
  });
  return `current admins list: ${items.join(', ')}`;
}

function formatSkillGroupsList(docs) {
  if (!docs.length) return 'Skill-groups: (none)';
  return docs
    .sort((a, b) => a.groupNumber - b.groupNumber)
    .map(d => {
      const names = (d.players || []).map(p => p.nameOrig).join(', ') || '(empty)';
      return `Skill-group ${d.groupNumber} players: ${names}`;
    })
    .join('\n');
}

function formatMapsList(maps) {
  if (!maps.length) return 'Maps: (none)';
  const names = maps.map(m => m.nameOrig).join(', ');
  return `Maps: ${names}`;
}

// SG map helper
function buildSGMap(sgs) {
  const map = new Map();
  for (const sg of sgs) {
    for (const p of sg.players || []) {
      map.set(p.nameNorm, sg.groupNumber);
    }
  }
  return map;
}

// Players formatter with positions + сортировка по SG, затем по месту
function sortPlayersForDisplay(players = []) {
  return (players || []).slice().sort((a, b) => {
    const pa = (typeof a.pos === 'number' && a.pos >= 1) ? a.pos : Number.POSITIVE_INFINITY;
    const pb = (typeof b.pos === 'number' && b.pos >= 1) ? b.pos : Number.POSITIVE_INFINITY;
    if (pa !== pb) return pa - pb; // сначала по месту: 1, 2, 3, ...
    const sgA = a.sg ?? 999, sgB = b.sg ?? 999;
    if (sgA !== sgB) return sgA - sgB; // затем по SG (для стабильности внутри одного места)
    return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }); // затем по имени
  });
}

/*function playersToString(players = []) {
  const out = sortPlayersForDisplay(players).map(p => (p.pos ? `${p.nameOrig} (#${p.pos})` : p.nameOrig));
  return out.join(', ') || '(empty)';
}*/

function playersToString(players = []) {
  // Сортировка остаётся прежней: по SG, затем по месту, затем по имени.
  // В строке вывода позиция (#N) больше не показывается.
  const out = sortPlayersForDisplay(players).map(p => p.nameOrig);
  return out.join(', ') || '(empty)';
}

// Рядом с существующими сортировщиками
function playersToStringWithPos(players = []) {
  const out = sortPlayersForDisplay(players).map(p =>
    p.pos ? `${p.nameOrig} (#${p.pos})` : p.nameOrig
  );
  return out.join(', ') || '(empty)';
}

function formatGameGroupsList(groups, waiting = [], groupPtsMap = new Map(), opts = {}) {
  const twoCols = opts.twoCols !== false; // по умолчанию 2 колонки (для /info)

  if (!groups.length) {
    const parts = ['Game groups: (none)'];
    if (waiting.length) parts.push(`\nWaiting (not assigned): ${waiting.map(p => p.nameOrig).join(', ')}`);
    return parts.join('\n');
  }

  function sortPlayersInGroup(players = []) {
    if (!players.length) return [];
    const hasPts = players.some(p => groupPtsMap.has(p.nameNorm));
    if (hasPts) {
      return players.slice().sort((a, b) => {
        const aHas = groupPtsMap.has(a.nameNorm);
        const bHas = groupPtsMap.has(b.nameNorm);
        if (aHas && bHas) {
          const da = groupPtsMap.get(a.nameNorm);
          const db = groupPtsMap.get(b.nameNorm);
          if (da !== db) return da - db;
          return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
        }
        if (aHas !== bHas) return aHas ? -1 : 1; // с очками первыми
        const pa = (typeof a.pos === 'number' && a.pos >= 1) ? a.pos : Number.POSITIVE_INFINITY;
        const pb = (typeof b.pos === 'number' && b.pos >= 1) ? b.pos : Number.POSITIVE_INFINITY;
        if (pa !== pb) return pa - pb;
        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      });
    }
    return players.slice().sort((a, b) => {
      const pa = (typeof a.pos === 'number' && a.pos >= 1) ? a.pos : Number.POSITIVE_INFINITY;
      const pb = (typeof b.pos === 'number' && b.pos >= 1) ? b.pos : Number.POSITIVE_INFINITY;
      if (pa !== pb) return pa - pb;
      return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
    });
  }

  function buildBlock(g) {
    const title = `Group ${g.groupId}`;
    const timeLine = `Time: ${g.time ? g.time : '(not set)'}`;

    const players = sortPlayersInGroup(g.players || []);
    const playerLines = [];
    if (!players.length) {
      playerLines.push('(empty)');
    } else {
      for (const p of players) {
        const pts = groupPtsMap.get(p.nameNorm);
        const base = pts != null ? `${p.nameOrig}[${pts}]` : p.nameOrig;
        const suffix = (typeof p.pos === 'number' && p.pos >= 1) ? ` (#${p.pos})` : '';
        playerLines.push(`${base}${suffix}`);
      }
    }

    const maps = g.maps || [];
    const mapsLines = maps.length
      ? ['Maps:'].concat(maps.map(m => `- ${m}`))
      : ['Maps: (none)'];

    return { title, timeLine, playerLines, mapsLines };
  }

  const blocks = groups.map(buildBlock);
  const lines = [];

  if (!twoCols) {
    for (let i = 0; i < blocks.length; i++) {
      const B = blocks[i];
      lines.push(B.title);
      lines.push(B.timeLine);
      for (const pl of B.playerLines) lines.push(pl);
      for (const ml of B.mapsLines) lines.push(ml);
      if (i + 1 < blocks.length) lines.push('');
    }
    if (waiting.length) {
      const w = waiting.slice().sort((a, b) => (a.sgNumber ?? 999) - (b.sgNumber ?? 999)
        || (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
      lines.push('');
      lines.push(`Waiting (not assigned): ${w.map(p => p.nameOrig).join(', ')}`);
    }
    return lines.join('\n');
  }

  const GAP = '    '; // 4 spaces
  for (let i = 0; i < blocks.length; i += 2) {
    const L = blocks[i];
    const R = blocks[i + 1] || { title: '', timeLine: '', playerLines: [], mapsLines: [] };

    const maxPlayers = Math.max(L.playerLines.length, R.playerLines.length);
    const maxMaps = Math.max(L.mapsLines.length, R.mapsLines.length);

    const leftLinesForWidth = [];
    leftLinesForWidth.push(L.title, L.timeLine);
    for (let k = 0; k < maxPlayers; k++) leftLinesForWidth.push(L.playerLines[k] || '');
    for (let k = 0; k < maxMaps; k++) leftLinesForWidth.push(L.mapsLines[k] || '');
    const leftWidth = Math.max(...leftLinesForWidth.map(s => s.length), 0) + GAP.length;

    // title row
    lines.push((L.title || '').padEnd(leftWidth, ' ') + (R.title || ''));
    // time row
    lines.push((L.timeLine || '').padEnd(leftWidth, ' ') + (R.timeLine || ''));
    // players rows
    for (let k = 0; k < maxPlayers; k++) {
      const lp = L.playerLines[k] || '';
      const rp = R.playerLines[k] || '';
      lines.push(lp.padEnd(leftWidth, ' ') + rp);
    }
    // maps rows
    for (let k = 0; k < maxMaps; k++) {
      const lm = L.mapsLines[k] || '';
      const rm = R.mapsLines[k] || '';
      lines.push(lm.padEnd(leftWidth, ' ') + rm);
    }
    if (i + 2 < blocks.length) lines.push('');
  }

  if (waiting.length) {
    const w = waiting.slice().sort((a, b) => (a.sgNumber ?? 999) - (b.sgNumber ?? 999)
      || (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
    lines.push('');
    lines.push(`Waiting (not assigned): ${w.map(p => p.nameOrig).join(', ')}`);
  }

  return lines.join('\n');
}

function formatFinalGroupsList(groups, finalPtsMap = new Map(), opts = {}) {
  const twoCols = opts.twoCols !== false; // по умолчанию 2 колонки (для /info)
  if (!groups.length) return 'Final groups: (none)';

  function sortPlayers(players = []) {
    const hasPts = players.some(p => finalPtsMap.has(p.nameNorm));
    if (hasPts) {
      return players.slice().sort((a, b) => {
        const aHas = finalPtsMap.has(a.nameNorm);
        const bHas = finalPtsMap.has(b.nameNorm);
        if (aHas && bHas) {
          const da = finalPtsMap.get(a.nameNorm);
          const db = finalPtsMap.get(b.nameNorm);
          if (da !== db) return da - db;
          return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
        }
        if (aHas !== bHas) return aHas ? -1 : 1;
        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      });
    }
    return players.slice().sort((a, b) =>
      (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' })
    );
  }

  function buildBlock(g) {
    const title = `Final ${g.groupId}`;
    const timeLine = `Time: ${g.time ? g.time : '(not set)'}`;

    const ps = sortPlayers(g.players || []);
    const playerLines = [];
    if (!ps.length) {
      playerLines.push('(empty)');
    } else {
      for (const p of ps) {
        const pts = finalPtsMap.get(p.nameNorm);
        playerLines.push(pts != null ? `${p.nameOrig}[${pts}]` : p.nameOrig);
      }
    }

    const maps = g.maps || [];
    const mapsLines = maps.length
      ? ['Maps:'].concat(maps.map(m => `- ${m}`))
      : ['Maps: (none)'];

    return { title, timeLine, playerLines, mapsLines };
  }

  const blocks = groups.map(buildBlock);
  const lines = [];

  if (!twoCols) {
    for (let i = 0; i < blocks.length; i++) {
      const B = blocks[i];
      lines.push(B.title);
      lines.push(B.timeLine);
      for (const pl of B.playerLines) lines.push(pl);
      for (const ml of B.mapsLines) lines.push(ml);
      if (i + 1 < blocks.length) lines.push('');
    }
    return lines.join('\n');
  }

  const GAP = '    ';
  for (let i = 0; i < blocks.length; i += 2) {
    const L = blocks[i];
    const R = blocks[i + 1] || { title: '', timeLine: '', playerLines: [], mapsLines: [] };

    const maxPlayers = Math.max(L.playerLines.length, R.playerLines.length);
    const maxMaps = Math.max(L.mapsLines.length, R.mapsLines.length);

    const leftLinesForWidth = [];
    leftLinesForWidth.push(L.title, L.timeLine);
    for (let k = 0; k < maxPlayers; k++) leftLinesForWidth.push(L.playerLines[k] || '');
    for (let k = 0; k < maxMaps; k++) leftLinesForWidth.push(L.mapsLines[k] || '');
    const leftWidth = Math.max(...leftLinesForWidth.map(s => s.length), 0) + GAP.length;

    lines.push((L.title || '').padEnd(leftWidth, ' ') + (R.title || ''));
    lines.push((L.timeLine || '').padEnd(leftWidth, ' ') + (R.timeLine || ''));
    for (let k = 0; k < maxPlayers; k++) {
      const lp = L.playerLines[k] || '';
      const rp = R.playerLines[k] || '';
      lines.push(lp.padEnd(leftWidth, ' ') + rp);
    }
    for (let k = 0; k < maxMaps; k++) {
      const lm = L.mapsLines[k] || '';
      const rm = R.mapsLines[k] || '';
      lines.push(lm.padEnd(leftWidth, ' ') + rm);
    }
    if (i + 2 < blocks.length) lines.push('');
  }

  return lines.join('\n');
}


// --- Map results formatting ---

function formatMapResultsTable(kindLabel, groupId, results = []) {
  const headerTitle = `${kindLabel} ${groupId} map results:`;

  if (!results.length) {
    return `${headerTitle}\n(none)`;
  }

  const lines = [headerTitle, ''];

  for (const r of results) {
    const players = Array.isArray(r.players) ? r.players : [];
    const mapName = r.map || '(unknown map)';
    const dt = r.matchDateTime || '(no date/time)';
    const play = r.matchPlaytime || '(no duration)';

    lines.push(`Map: ${mapName}`);
    lines.push(`Finished: ${dt}   Duration: ${play}`);

    if (!players.length) {
      lines.push('(no players)');
      lines.push('');
      continue;
    }

    const nameHeader = 'Player';
    const cols = ['frags', 'kills', 'eff', 'fph', 'dgiv', 'drec'];

    let nameWidth = nameHeader.length;
    const colWidths = cols.map(c => c.length);

    for (const p of players) {
      const nm = p.nameOrig || '';
      if (nm.length > nameWidth) nameWidth = nm.length;
      const vals = [p.frags, p.kills, p.eff, p.fph, p.dgiv, p.drec];
      vals.forEach((v, idx) => {
        const s = String(v ?? '');
        if (s.length > colWidths[idx]) colWidths[idx] = s.length;
      });
    }

    const headerLine =
      nameHeader.padEnd(nameWidth + 1) +
      cols
        .map((c, idx) => c.padStart(colWidths[idx] + 1))
        .join('');
    lines.push(headerLine);

    const sepLine =
      ''.padEnd(nameWidth + 1, '-') +
      cols.map((_, idx) => ''.padStart(colWidths[idx] + 1, '-')).join('');
    lines.push(sepLine);

    for (const p of players) {
      const vals = [p.frags, p.kills, p.eff, p.fph, p.dgiv, p.drec];
      const row =
        (p.nameOrig || '').padEnd(nameWidth + 1) +
        vals
          .map((v, idx) => String(v ?? '').padStart(colWidths[idx] + 1))
          .join('');
      lines.push(row);
    }

    lines.push('');
  }

  return lines.join('\n').trimEnd();
}


// Distribution helpers

// capacities: массив длиной groupCount — цель/ёмкость каждой группы
function distributeBySG(selectedBySG, groupCount, capacities) {
  const groups = Array.from({ length: groupCount }, () => []);
  const sizes = new Array(groupCount).fill(0);
  const sgKeys = Array.from(selectedBySG.keys()).sort((a, b) => a - b);
  let maxLen = 0;
  for (const key of sgKeys) maxLen = Math.max(maxLen, (selectedBySG.get(key)?.length || 0));

  for (let layer = 0; layer < maxLen; layer++) {
    for (let j = 0; j < sgKeys.length; j++) {
      const sgNum = sgKeys[j];
      const arr = selectedBySG.get(sgNum) || [];
      if (layer >= arr.length) continue;
      const player = arr[layer];
      // старт для этого SG на этом слое — вращающийся
      let idx = (layer + j) % groupCount;
      let tried = 0;
      while (tried < groupCount && sizes[idx] >= capacities[idx]) {
        idx = (idx + 1) % groupCount;
        tried++;
      }
      if (tried === groupCount) continue; // все заполнены
      groups[idx].push(player);
      sizes[idx]++;
    }
  }
  return groups;
}

// Pair from ends across groups for finals algo=2
function distributeByEndsRoundRobin(list, groupCount, capPerGroup) {
  const groups = Array.from({ length: groupCount }, () => []);
  const sizes = new Array(groupCount).fill(0);
  let left = 0;
  let right = list.length - 1;
  let gi = 0;

  function placeToCurrent(p) {
    let tries = 0;
    while (tries < groupCount && sizes[gi] >= capPerGroup) {
      gi = (gi + 1) % groupCount;
      tries++;
    }
    if (tries === groupCount) return false;
    groups[gi].push(p);
    sizes[gi]++;
    return true;
  }

  while (left <= right && groupCount > 0) {
    // place left
    if (!placeToCurrent(list[left])) break;
    left++;
    // place right (if any)
    if (left <= right) {
      if (!placeToCurrent(list[right])) break;
      right--;
    }
    // next group
    gi = (gi + 1) % groupCount;
  }
  return groups;
}

// Distribution logic (router)
async function makeGameGroups(chatId, C) {
  const settings = await getChatSettings(chatId);
  if (settings.groupsAlgo === 1) {
    return makeGameGroupsAlgo1(chatId, C, settings);
  }
  if (settings.groupsAlgo === 2) {
    return makeGameGroupsAlgo2(chatId, C, settings);
  }
  if (settings.groupsAlgo === 3) {
    return makeGameGroupsAlgo3(chatId, C, settings);
  }
  return makeGameGroupsAlgo1(chatId, C, settings);
}

// ---- Finals/Superfinals makers ----

// Все игроки из игровых групп с выставленными местами (pos)
async function collectGroupResultsPlayers(chatId) {
  const ggs = await listGameGroups(chatId);
  const arr = [];
  for (const g of ggs) {
    for (const p of (g.players || [])) {
      if (typeof p.pos === 'number' && p.pos >= 1) {
        arr.push({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg ?? null, pos: p.pos });
      }
    }
  }
  // сортировка по месту, затем SG, затем по имени
  arr.sort((a, b) => {
    const pa = a.pos, pb = b.pos;
    if (pa !== pb) return pa - pb;
    const sga = a.sg ?? 999, sgb = b.sg ?? 999;
    if (sga !== sgb) return sga - sgb;
    return a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' });
  });
  // dedup по имени, оставляя первое вхождение
  const seen = new Set();
  const out = [];
  for (const p of arr) {
    if (!seen.has(p.nameNorm)) {
      seen.add(p.nameNorm);
      out.push(p);
    }
  }
  return out;
}

// Игроки, присутствующие в текущих игровых группах (Map by nameNorm)
async function collectPlayersFromGameGroups(chatId) {
  const ggs = await listGameGroups(chatId);
  const m = new Map();
  for (const g of ggs) {
    for (const p of (g.players || [])) {
      if (!m.has(p.nameNorm)) m.set(p.nameNorm, { nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg ?? null });
    }
  }
  return m;
}

// Игроки, присутствующие в текущих финалах (Map by nameNorm)
async function collectPlayersFromFinalGroups(chatId) {
  const fgs = await listFinalGroups(chatId);
  const m = new Map();
  for (const g of fgs) {
    for (const p of (g.players || [])) {
      if (!m.has(p.nameNorm)) m.set(p.nameNorm, { nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg ?? null });
    }
  }
  return m;
}

async function makeFinals(chatId, C) {
  const settings = await getChatSettings(chatId);
  const maps = await listMaps(chatId);
  if (maps.length < C) return { error: `Not enough maps. Need at least ${C}, have ${maps.length}.` };

  const cap = Math.max(1, Number(settings.finalMaxPlayers || DEFAULT_MAX_PLAYERS));
  let source = [];

  if (settings.finalsAlgo === 1) {
    const all = await collectGroupResultsPlayers(chatId);
    if (!all.length) return { error: 'No group results found. Set /groups N result ... first.' };
    const limit = settings.finalTotalPlayers != null ? Number(settings.finalTotalPlayers) : null;
    source = limit && limit > 0 ? all.slice(0, limit) : all;
  } else {
    // algo=2 — по рейтингу (игнорирует totalplayers)
    const rating = await getRating(chatId);
    if (!rating.length) return { error: 'Rating is empty. Set it via /groups rating name1,name2,...' };
    const presentMap = await collectPlayersFromGameGroups(chatId);
    source = rating
      .map(r => presentMap.get(r.nameNorm))
      .filter(Boolean); // пересечение рейтинга с участниками текущих игровых групп
    if (!source.length) return { error: 'No rated players found in current game groups.' };
  }

  const total = source.length;
  const groupCount = Math.ceil(total / cap);
  if (groupCount < 1) return { error: 'Unable to create final groups.' };

  const placed = distributeByEndsRoundRobin(source, groupCount, cap);
  const mapNames = maps.map(m => m.nameOrig);

  await deleteAllFinalGroups(chatId);
  const groups = [];
  for (let i = 0; i < placed.length; i++) {
    const players = placed[i].map(p => ({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg ?? null }));
    const mapsPick = shuffle(mapNames).slice(0, C);
    const groupId = i + 1;
    // eslint-disable-next-line no-await-in-loop
    await upsertFinalGroup(chatId, groupId, { players, maps: mapsPick, createdAt: new Date() });
    groups.push({ groupId, players, maps: mapsPick });
  }
  return { groups };
}

async function makeSuperFinals(chatId, C) {
  const settings = await getChatSettings(chatId);
  const maps = await listMaps(chatId);
  if (maps.length < C) return { error: `Not enough maps. Need at least ${C}, have ${maps.length}.` };

  const cap = Math.max(1, Number(settings.superfinalMaxPlayers || DEFAULT_MAX_PLAYERS));
  let source = [];

  if (settings.superfinalsAlgo === 1) {
    // По очкам финалов
    const ptsArr = await getFinalPoints(chatId);
    if (!ptsArr.length) return { error: 'Final points are empty. Set them via /finals points name[p],...' };
    const presentMap = await collectPlayersFromFinalGroups(chatId);
    const presentPts = ptsArr
      .filter(p => presentMap.has(p.nameNorm))
      .map(p => ({ ...presentMap.get(p.nameNorm), pts: p.pts }));
    if (!presentPts.length) return { error: 'No players with points found in current finals.' };
    presentPts.sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
    const limit = settings.superfinalTotalPlayers != null ? Number(settings.superfinalTotalPlayers) : null;
    source = limit && limit > 0 ? presentPts.slice(0, limit) : presentPts;
  } else {
    // По рейтингу финалов (игнорирует totalplayers)
    const rating = await getFinalRating(chatId);
    if (!rating.length) return { error: 'Finals rating is empty. Set it via /finals rating name1,name2,...' };
    const presentMap = await collectPlayersFromFinalGroups(chatId);
    source = rating
      .map(r => presentMap.get(r.nameNorm))
      .filter(Boolean);
    if (!source.length) return { error: 'No rated players found in current finals.' };
  }

  const total = source.length;
  const groupCount = Math.ceil(total / cap);
  if (groupCount < 1) return { error: 'Unable to create superfinal groups.' };

  const placed = distributeByEndsRoundRobin(source, groupCount, cap);
  const mapNames = maps.map(m => m.nameOrig);

  await deleteAllSuperFinalGroups(chatId);
  const groups = [];
  for (let i = 0; i < placed.length; i++) {
    const players = placed[i].map(p => ({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg ?? null }));
    const mapsPick = shuffle(mapNames).slice(0, C);
    const groupId = i + 1;
    // eslint-disable-next-line no-await-in-loop
    await upsertSuperFinalGroup(chatId, groupId, { players, maps: mapsPick, createdAt: new Date() });
    groups.push({ groupId, players, maps: mapsPick });
  }
  return { groups };
}

// --- Map results storage (groups / finals / superfinals) ---

// GROUPS
async function upsertGroupMapResult(chatId, groupId, mapOrig, data) {
  const mapNorm = norm(mapOrig);
  await colGroupResults.updateOne(
    { chatId, groupId: Number(groupId), mapNorm },
    {
      $set: {
        chatId,
        groupId: Number(groupId),
        map: mapOrig,
        mapNorm,
        matchDateTime: data.matchDateTime,         // "YYYY-MM-DD HH:MM" (для отображения)
        matchDateTimeIso: data.matchDateTimeIso,   // "YYYY-MM-DDTHH:MM:00+03:00"
        matchTs: data.matchTs,                     // Number (UTC ms)
        matchPlaytime: data.matchPlaytime,         // "MM:SS"
        players: data.players,                     // [{...}]
        updatedAt: new Date(),
      },
      $setOnInsert: { createdAt: new Date() },
    },
    { upsert: true }
  );
}

async function listGroupMapResults(chatId, groupId) {
  return colGroupResults
    .find({ chatId, groupId: Number(groupId) })
    .sort({ matchTs: 1, map: 1 })
    .toArray();
}

async function deleteGroupMapResultsForGroup(chatId, groupId) {
  await colGroupResults.deleteMany({ chatId, groupId: Number(groupId) });
}

async function deleteGroupMapResultsForChat(chatId) {
  await colGroupResults.deleteMany({ chatId });
}

// FINALS
async function upsertFinalMapResult(chatId, groupId, mapOrig, data) {
  const mapNorm = norm(mapOrig);
  await colFinalResults.updateOne(
    { chatId, groupId: Number(groupId), mapNorm },
    {
      $set: {
        chatId,
        groupId: Number(groupId),
        map: mapOrig,
        mapNorm,
        matchDateTime: data.matchDateTime,
        matchDateTimeIso: data.matchDateTimeIso,
        matchTs: data.matchTs,
        matchPlaytime: data.matchPlaytime,
        players: data.players,
        updatedAt: new Date(),
      },
      $setOnInsert: { createdAt: new Date() },
    },
    { upsert: true }
  );
}

async function listFinalMapResults(chatId, groupId) {
  return colFinalResults
    .find({ chatId, groupId: Number(groupId) })
    .sort({ matchTs: 1, map: 1 })
    .toArray();
}


async function deleteFinalMapResultsForGroup(chatId, groupId) {
  await colFinalResults.deleteMany({ chatId, groupId: Number(groupId) });
}

async function deleteFinalMapResultsForChat(chatId) {
  await colFinalResults.deleteMany({ chatId });
}

// SUPERFINALS
async function upsertSuperFinalMapResult(chatId, groupId, mapOrig, data) {
  const mapNorm = norm(mapOrig);
  await colSuperFinalResults.updateOne(
    { chatId, groupId: Number(groupId), mapNorm },
    {
      $set: {
        chatId,
        groupId: Number(groupId),
        map: mapOrig,
        mapNorm,
        matchDateTime: data.matchDateTime,
        matchDateTimeIso: data.matchDateTimeIso,
        matchTs: data.matchTs,
        matchPlaytime: data.matchPlaytime,
        players: data.players,
        updatedAt: new Date(),
      },
      $setOnInsert: { createdAt: new Date() },
    },
    { upsert: true }
  );
}

async function listSuperFinalMapResults(chatId, groupId) {
  return colSuperFinalResults
    .find({ chatId, groupId: Number(groupId) })
    .sort({ matchTs: 1, map: 1 })
    .toArray();
}


async function deleteSuperFinalMapResultsForGroup(chatId, groupId) {
  await colSuperFinalResults.deleteMany({ chatId, groupId: Number(groupId) });
}

async function deleteSuperFinalMapResultsForChat(chatId) {
  await colSuperFinalResults.deleteMany({ chatId });
}


// Algo 1: классический — maxPlayers, равномерно по SG послойно
async function makeGameGroupsAlgo1(chatId, C, settings) {
  const { maxPlayers } = settings;

  const sgs = await listSkillGroups(chatId);
  if (!sgs.length) return { error: 'No skill-groups found. Add players first.' };

  const maps = await listMaps(chatId);
  if (maps.length < C) return { error: `Not enough maps. Need at least ${C}, have ${maps.length}.` };

  const total = sgs.reduce((acc, sg) => acc + (sg.players?.length || 0), 0);
  if (!total) return { error: 'No players in skill-groups. Add players first.' };
  const groupCount = Math.ceil(total / maxPlayers);
  if (groupCount < 1) return { error: 'Unable to create groups.' };

  const selectedBySG = new Map();
  for (const sg of sgs.sort((a, b) => a.groupNumber - b.groupNumber)) {
    const bucket = shuffle(sg.players || []).map(p => ({ ...p, sg: sg.groupNumber }));
    selectedBySG.set(sg.groupNumber, bucket);
  }

  const capacities = Array.from({ length: groupCount }, () => maxPlayers);
  const placed = distributeBySG(selectedBySG, groupCount, capacities);

  const mapNames = maps.map(m => m.nameOrig);
  const groups = placed.map((arr, i) => ({
    groupId: i + 1,
    players: arr,
    maps: shuffle(mapNames).slice(0, C),
  }));

  await deleteAllGameGroups(chatId);
  await deleteGroupMapResultsForChat(chatId);
  await clearWaitingPlayers(chatId);
  for (const g of groups) {
    // eslint-disable-next-line no-await-in-loop
    await upsertGameGroup(chatId, g.groupId, { players: g.players, maps: g.maps, createdAt: new Date() });
  }
  return { groups, waiting: [] };
}

// Helpers for algo2
function computeBestGroupCount(total, minP, recP, maxP) {
  if (minP > maxP) return { error: 'minplayers cannot be greater than maxplayers' };
  const minCount = Math.ceil(total / maxP);
  const maxCount = Math.max(1, Math.floor(total / Math.max(minP, 1)));
  if (minCount > maxCount) {
    return { error: `Cannot satisfy constraints with total=${total}, min=${minP}, max=${maxP}` };
  }
  let best = minCount;
  let bestDiff = Math.abs(total / minCount - recP);
  for (let k = minCount + 1; k <= maxCount; k++) {
    const diff = Math.abs(total / k - recP);
    if (diff < bestDiff) {
      best = k;
      bestDiff = diff;
    }
  }
  return { count: best };
}
function buildTargets(total, groupCount) {
  const base = Math.floor(total / groupCount);
  const rem = total % groupCount;
  const targets = Array.from({ length: groupCount }, (_, i) => base + (i < rem ? 1 : 0));
  return targets;
}

// Algo 2: стремимся к recplayers при min..max, лишние — в Waiting (приоритет слабых SG)
async function makeGameGroupsAlgo2(chatId, C, settings) {
  const { maxPlayers, minPlayers2, recPlayers2 } = settings;
  const minP = Math.max(1, Number(minPlayers2 || 1));
  const recP = Math.max(1, Number(recPlayers2 || maxPlayers));
  const maxP = Math.max(1, Number(maxPlayers));

  const sgs = await listSkillGroups(chatId);
  if (!sgs.length) return { error: 'No skill-groups found. Add players first.' };
  const maps = await listMaps(chatId);
  if (maps.length < C) return { error: `Not enough maps. Need at least ${C}, have ${maps.length}.` };

  const total = sgs.reduce((acc, sg) => acc + (sg.players?.length || 0), 0);
  if (!total) return { error: 'No players in skill-groups. Add players first.' };

  const cc = computeBestGroupCount(total, minP, recP, maxP);
  if (cc.error) return { error: cc.error };
  const groupCount = cc.count;

  const targetsRaw = buildTargets(total, groupCount);
  const targets = targetsRaw.map(t => Math.max(minP, Math.min(maxP, t)));
  const capAll = targets.reduce((a, b) => a + b, 0);
  if (capAll <= 0) return { error: 'Cannot satisfy constraints. Check minplayers/maxplayers.' };

  const buckets = sgs
    .sort((a, b) => a.groupNumber - b.groupNumber)
    .map(sg => ({ sgNumber: sg.groupNumber, queue: shuffle(sg.players || []) }));

  const selectedBySG = new Map();
  const waiting = [];
  let picked = 0;
  for (const b of buckets) {
    for (const p of b.queue) {
      if (picked < capAll) {
        const arr = selectedBySG.get(b.sgNumber) || [];
        arr.push({ ...p, sg: b.sgNumber });
        selectedBySG.set(b.sgNumber, arr);
        picked++;
      } else {
        waiting.push({ ...p, sgNumber: b.sgNumber });
      }
    }
  }

  const placed = distributeBySG(selectedBySG, groupCount, targets);
  const mapNames = maps.map(m => m.nameOrig);
  const groups = placed.map((arr, i) => ({
    groupId: i + 1,
    players: arr,
    maps: shuffle(mapNames).slice(0, C),
  }));

  await deleteAllGameGroups(chatId);
  await deleteGroupMapResultsForChat(chatId);
  for (const g of groups) {
    // eslint-disable-next-line no-await-in-loop
    await upsertGameGroup(chatId, g.groupId, { players: g.players, maps: g.maps, createdAt: new Date() });
  }
  await setWaitingPlayers(chatId, waiting);

  return { groups, waiting };
}

// Algo 3: ровно maxcount групп по maxplayers; лишние — в Waiting; приоритет SG1→...
async function makeGameGroupsAlgo3(chatId, C, settings) {
  const { maxPlayers, maxCount3 } = settings;
  const maxCount = Number(maxCount3 || 0);
  if (!maxCount || maxCount <= 0) return { error: 'Set /groups maxcount <count> before using algo 3.' };

  const sgs = await listSkillGroups(chatId);
  if (!sgs.length) return { error: 'No skill-groups found. Add players first.' };
  const maps = await listMaps(chatId);
  if (maps.length < C) return { error: `Not enough maps. Need at least ${C}, have ${maps.length}.` };

  const buckets = sgs
    .sort((a, b) => a.groupNumber - b.groupNumber)
    .map(sg => ({ sgNumber: sg.groupNumber, queue: shuffle(sg.players || []) }));

  const capAll = maxCount * maxPlayers;
  const selectedBySG = new Map();
  const waiting = [];
  let picked = 0;
  for (const b of buckets) {
    for (const p of b.queue) {
      if (picked < capAll) {
        const arr = selectedBySG.get(b.sgNumber) || [];
        arr.push({ ...p, sg: b.sgNumber });
        selectedBySG.set(b.sgNumber, arr);
        picked++;
      } else {
        waiting.push({ ...p, sgNumber: b.sgNumber });
      }
    }
  }

  const capacities = Array.from({ length: maxCount }, () => maxPlayers);
  const placed = distributeBySG(selectedBySG, maxCount, capacities);

  const mapNames = maps.map(m => m.nameOrig);
  const groups = placed.map((arr, i) => ({
    groupId: i + 1,
    players: arr,
    maps: shuffle(mapNames).slice(0, C),
  }));

  await deleteAllGameGroups(chatId);
  await deleteGroupMapResultsForChat(chatId);
  for (const g of groups) {
    // eslint-disable-next-line no-await-in-loop
    await upsertGameGroup(chatId, g.groupId, { players: g.players, maps: g.maps, createdAt: new Date() });
  }
  await setWaitingPlayers(chatId, waiting);

  return { groups, waiting };
}

// -------- Custom groups (произвольные группы) --------

// Counters for Custom Group IDs
// REPLACE the entire getNextCustomGroupId function with this robust version
async function getNextCustomGroupId(chatId) {
  let res;
  try {
    res = await colCounters.findOneAndUpdate(
      { chatId, key: 'customGroupId' },
      { $inc: { value: 1 } },
      { upsert: true, returnDocument: 'after' }
    );
  } catch (_) {
    // ignore, fallback below
  }

  let id = res?.value?.value;

  // Fallback: compute next id by max existing custom groupId (numeric only)
  if (!Number.isInteger(id) || id <= 0) {
    const last = await colCustomGroups
      .find({ chatId, groupId: { $type: 'int' } })
      .sort({ groupId: -1 })
      .limit(1)
      .toArray();

    if (last.length && Number.isFinite(Number(last[0].groupId))) {
      id = Number(last[0].groupId) + 1;
    } else {
      // Try also doubles (in case groupId stored as double)
      const lastDouble = await colCustomGroups
        .find({ chatId, groupId: { $type: 'double' } })
        .sort({ groupId: -1 })
        .limit(1)
        .toArray();
      id = lastDouble.length && Number.isFinite(Number(lastDouble[0].groupId))
        ? Number(lastDouble[0].groupId) + 1
        : 1;
    }

    // Persist corrected counter for future calls
    await colCounters.updateOne(
      { chatId, key: 'customGroupId' },
      { $set: { chatId, key: 'customGroupId', value: id } },
      { upsert: true }
    );
  }

  return id;
}


async function resetCustomGroupIdCounter(chatId) {
  await colCounters.deleteOne({ chatId, key: 'customGroupId' });
}



// Storage
async function listCustomGroups(chatId) {
  return colCustomGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}
async function getCustomGroup(chatId, groupId) {
  return colCustomGroups.findOne({ chatId, groupId: Number(groupId) });
}
async function upsertCustomGroup(chatId, groupId, data) {
  await colCustomGroups.updateOne(
    { chatId, groupId: Number(groupId) },
    { $set: { chatId, groupId: Number(groupId), ...data } },
    { upsert: true }
  );
}
async function deleteCustomGroup(chatId, groupId) {
  await colCustomGroups.deleteOne({ chatId, groupId: Number(groupId) });
}
async function deleteAllCustomGroups(chatId) {
  await colCustomGroups.deleteMany({ chatId });
  await resetCustomGroupIdCounter(chatId);
}

// Points per custom group
async function getCustomPoints(chatId, groupId) {
  const doc = await colCustomPoints.findOne({ chatId, groupId: Number(groupId) });
  return doc?.points || []; // [{nameNorm,nameOrig,pts}]
}
async function setCustomPoints(chatId, groupId, points) {
  await colCustomPoints.updateOne(
    { chatId, groupId: Number(groupId) },
    { $set: { chatId, groupId: Number(groupId), points, updatedAt: new Date() } },
    { upsert: true }
  );
}
function customPointsToMap(arr = []) {
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}

// ДОБАВИТЬ НОВУЮ функцию показа скриншотов для custom-группы
async function showCustomGroupScreenshots(ctx, chatId, groupId) {
  const g = await getCustomGroup(chatId, groupId);
  if (!g) {
    await ctx.reply(`Custom ${groupId} not found.`);
    return;
  }
  const runId = getGroupRunIdFromDoc(g);
  const doc = await colScreenshots.findOne({ chatId, scope: 'custom', groupId: Number(groupId), groupRunId: runId });
  const files = doc?.files || [];
  if (!files.length) {
    await ctx.reply('Скриншоты не найдены для текущей версии custom-группы.');
    return;
  }

  const photos = files.filter(f => f.type === 'photo');
  const docs = files.filter(f => f.type === 'document');

  // Фото пачками по 10; если 1 — отправим одиночным фото.
  for (let i = 0; i < photos.length;) {
    const chunk = photos.slice(i, i + 10);
    if (chunk.length === 1) {
      const p = chunk[0];
      const media = p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) };
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithPhoto(media);
    } else {
      const mediaGroup = chunk.map(p => ({
        type: 'photo',
        media: p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) },
      }));
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithMediaGroup(mediaGroup);
    }
    i += chunk.length;
  }

  // Документы по одному
  for (const d of docs) {
    const media = d.tgFileId ? d.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, d.relPath)) };
    // eslint-disable-next-line no-await-in-loop
    await ctx.replyWithDocument(media);
  }
}


// Formatter (1 колонка, как /g в столбик, сортировка по points если есть)
// REPLACE the entire formatCustomGroupsList function with this version (prevents showing "NaN")
function formatCustomGroupsList(groups = [], pointsByGroup = new Map()) {
  if (!groups.length) return 'Custom groups: (none)';

  const lines = [];

  function sortPlayers(players = [], ptsMap = new Map()) {
    if (!players.length) return [];
    const hasPts = players.some(p => ptsMap.has(p.nameNorm));
    if (hasPts) {
      return players.slice().sort((a, b) => {
        const aHas = ptsMap.has(a.nameNorm);
        const bHas = ptsMap.has(b.nameNorm);
        if (aHas && bHas) {
          const da = ptsMap.get(a.nameNorm);
          const db = ptsMap.get(b.nameNorm);
          if (da !== db) return da - db;
          return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
        }
        if (aHas !== bHas) return aHas ? -1 : 1;
        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      });
    }
    return players.slice().sort((a, b) =>
      (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' })
    );
  }

  for (const g of groups) {
    const displayId = Number.isFinite(Number(g.groupId)) ? Number(g.groupId) : '?';
    const title = `Custom ${displayId}`;
    const nameLine = `Name: ${g.name ? g.name : '(not set)'}`;

    const ptsMap = pointsByGroup.get(g.groupId) || new Map();
    const players = sortPlayers(g.players || [], ptsMap);
    const playerLines = [];
    if (!players.length) {
      playerLines.push('(empty)');
    } else {
      for (const p of players) {
        const pts = ptsMap.get(p.nameNorm);
        playerLines.push(pts != null ? `${p.nameOrig}[${pts}]` : p.nameOrig);
      }
    }

    const maps = Array.isArray(g.maps) ? g.maps : [];
    const mapsLines = maps.length
      ? ['Maps:'].concat(maps.map(m => `- ${m}`))
      : ['Maps: (none)'];

    lines.push(title);
    lines.push(nameLine);
    for (const pl of playerLines) lines.push(pl);
    for (const ml of mapsLines) lines.push(ml);
    lines.push('');
  }

  return lines.join('\n').trim();
}

// ПОЛНАЯ ЗАМЕНА функции customHandler(ctx)
async function customHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // /c — список всех custom-групп (публично)
  if (!tokens.length) {
    const [groups, ptsDocs] = await Promise.all([
      listCustomGroups(chatId),
      colCustomPoints.find({ chatId }).toArray(),
    ]);
    const ptsByGroup = new Map();
    for (const d of ptsDocs) ptsByGroup.set(d.groupId, customPointsToMap(d.points || []));
    await replyPre(ctx, formatCustomGroupsList(groups, ptsByGroup));
    return;
  }

  // /c add <name> — создать новую группу (админ)
  if (tokens[0].toLowerCase() === 'add') {
    if (!(await requireAdminGuard(ctx))) return;
    const name = tokens.slice(1).join(' ').trim();
    if (!name) { await ctx.reply('Использование: /c add <название группы>'); return; }
    const id = await getNextCustomGroupId(chatId);
    await upsertCustomGroup(chatId, id, { name, players: [], maps: [], createdAt: new Date() });
    await ctx.reply(`Создана custom-группа №${id}.`);
    return;
  }

  // /c del <N> — удалить группу (админ)
  if (tokens[0].toLowerCase() === 'del') {
    if (!(await requireAdminGuard(ctx))) return;
    const N = Number(tokens[1]);
    if (!Number.isInteger(N) || N <= 0) { await ctx.reply('Использование: /c del <N>'); return; }
    const g = await getCustomGroup(chatId, N);
    if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
    await deleteCustomGroup(chatId, N);
    await colCustomPoints.deleteOne({ chatId, groupId: N }).catch(() => { });
    await ctx.reply(`Custom ${N} удалена.`);
    return;
  }

  // /c delall — удалить все (админ)
  if (tokens[0].toLowerCase() === 'delall') {
    if (!(await requireAdminGuard(ctx))) return;
    await deleteAllCustomGroups(chatId);
    await colCustomPoints.deleteMany({ chatId });
    await ctx.reply('Все custom-группы удалены.');
    return;
  }

  // /c <N> ... — операции с конкретной группой
  const N = Number(tokens[0]);
  if (Number.isInteger(N) && N > 0) {
    // /c N — показать (публично)
    if (tokens.length === 1) {
      const [g, ptsArr] = await Promise.all([getCustomGroup(chatId, N), getCustomPoints(chatId, N)]);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const txt = formatCustomGroupsList([g], new Map([[N, customPointsToMap(ptsArr)]]));
      await replyPre(ctx, txt);
      return;
    }

    const action = tokens[1].toLowerCase();
    const tail = tokens.slice(2).join(' ');

    // /c N addp name1,name2,...
    if (action === 'addp') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tail));
      if (!list.length) { await ctx.reply('Укажите игроков через запятую. Пример: /c 1 addp ly,test1,test2'); return; }
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const present = new Set((g.players || []).map(p => p.nameNorm));
      const added = [];
      const skipped = [];
      const nextPlayers = (g.players || []).slice();
      for (const p of list) {
        if (present.has(p.nameNorm)) { skipped.push(p.nameOrig); continue; }
        nextPlayers.push({ nameOrig: p.nameOrig, nameNorm: p.nameNorm });
        present.add(p.nameNorm);
        added.push(p.nameOrig);
      }
      await upsertCustomGroup(chatId, N, { ...g, players: nextPlayers });
      // показать обновлённую инфу
      const ptsArr = await getCustomPoints(chatId, N);
      await replyPre(ctx, formatCustomGroupsList([{ ...g, players: nextPlayers }], new Map([[N, customPointsToMap(ptsArr)]])));
      return;
    }

    // /c N delp name1,name2,...
    if (action === 'delp') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tail));
      if (!list.length) { await ctx.reply('Укажите игрока(ов). Пример: /c 1 delp ly'); return; }
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const toDel = new Set(list.map(p => p.nameNorm));
      const before = g.players || [];
      const after = before.filter(p => !toDel.has(p.nameNorm));
      await upsertCustomGroup(chatId, N, { ...g, players: after });
      await ctx.reply(before.length !== after.length
        ? `Deleted from Custom ${N}: ${list.map(p => p.nameOrig).join(', ')}`
        : 'Никого не удалено (нет совпадений).');
      return;
    }

    // /c N maps map1,map2,...
    if (action === 'maps') {
      if (!(await requireAdminGuard(ctx))) return;
      const maps = cleanListParam(tail);
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      await upsertCustomGroup(chatId, N, { ...g, maps });
      const ptsArr = await getCustomPoints(chatId, N);
      const updated = await getCustomGroup(chatId, N);
      await replyPre(ctx, formatCustomGroupsList([updated], new Map([[N, customPointsToMap(ptsArr)]])));
      return;
    }

    // /c N mix — перемешать карты
    if (action === 'mix') {
      if (!(await requireAdminGuard(ctx))) return;
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const maps = Array.isArray(g.maps) ? g.maps.slice() : [];
      if (!maps.length) { await ctx.reply('Нет карт для перемешивания.'); return; }
      const mixed = shuffle(maps);
      await upsertCustomGroup(chatId, N, { ...g, maps: mixed });
      const ptsArr = await getCustomPoints(chatId, N);
      const updated = await getCustomGroup(chatId, N);
      await replyPre(ctx, formatCustomGroupsList([updated], new Map([[N, customPointsToMap(ptsArr)]])));
      return;
    }

    // /c N points — показать points (публично)
    if (action === 'points' && !tail.trim()) {
      const ptsArr = await getCustomPoints(chatId, N);
      if (!ptsArr.length) { await ctx.reply('Custom points: (none)'); return; }
      const sorted = ptsArr
        .slice()
        .sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
      const lines = ['Custom points:'].concat(sorted.map(p => `${p.nameOrig}[${p.pts}]`));
      await replyChunked(ctx, lines.join('\n'));
      return;
    }

    // /c N points name1[p],name2[p],... — задать points (админ) + отсортировать игроков по возрастанию points
    if (action === 'points') {
      if (!(await requireAdminGuard(ctx))) return;
      const parsed = parsePointsList(tokens.slice(2).join(' ').trim());
      if (parsed.error) { await ctx.reply(parsed.error); return; }
      if (!parsed.length) { await ctx.reply('Укажите игроков и очки. Пример: /c 1 points ly[10],test1[20]'); return; }

      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const present = new Map((g.players || []).map(p => [p.nameNorm, p.nameOrig]));
      const missing = parsed.filter(p => !present.has(p.nameNorm)).map(p => p.nameOrig);
      if (missing.length) { await ctx.reply(`Не найдены в Custom ${N}: ${missing.join(', ')}`); return; }

      const toSave = parsed.map(p => ({ nameNorm: p.nameNorm, nameOrig: present.get(p.nameNorm), pts: p.pts }));
      await setCustomPoints(chatId, N, toSave);

      // Отсортируем игроков группы по возрастанию points
      const ptsMap = customPointsToMap(toSave);
      const sortedPlayers = (g.players || []).slice().sort((a, b) => {
        const aHas = ptsMap.has(a.nameNorm);
        const bHas = ptsMap.has(b.nameNorm);
        if (aHas && bHas) {
          const da = ptsMap.get(a.nameNorm);
          const db = ptsMap.get(b.nameNorm);
          if (da !== db) return da - db;
          return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
        }
        if (aHas !== bHas) return aHas ? -1 : 1;
        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      });
      await upsertCustomGroup(chatId, N, { ...g, players: sortedPlayers });

      const updated = await getCustomGroup(chatId, N);
      await replyPre(ctx, formatCustomGroupsList([updated], new Map([[N, ptsMap]])));
      return;
    }

    // --- НОВОЕ: /c N time [value] ---
    if (action === 'time') {
      const value = tokens.slice(2).join(' ').trim();
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }

      if (!value) {
        await ctx.reply(`Custom ${N} time: ${g.time ? g.time : '(not set)'}`);
      } else {
        if (!(await requireAdminGuard(ctx))) return;
        await upsertCustomGroup(chatId, N, { ...g, time: value });
        await ctx.reply(`Custom ${N} time is set: ${value}`);
      }
      return;
    }

    // --- НОВОЕ: /c N demos [add <url1,url2,...>] ---
    if (action === 'demos') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const list = cleanListParam(tokens.slice(3).join(' '));
        const g = await getCustomGroup(chatId, N);
        if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
        await upsertCustomGroup(chatId, N, { ...g, demos: list });
        await replyChunked(ctx, list.length ? `Custom ${N} demos are set:\n${list.join('\n')}` : `Custom ${N} demos cleared.`);
        return;
      }
      const g = await getCustomGroup(chatId, N);
      if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (!ds.length) { await ctx.reply(`Custom ${N} demos: (none)`); return; }
      await replyChunked(ctx, `Custom ${N} demos:\n${ds.join('\n')}`);
      return;
    }

    // --- НОВОЕ: /c N screenshots [add|delall] ---
    if (action === 'screenshots') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;

        const g = await getCustomGroup(chatId, N);
        if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);

        const key = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(key, {
          chatId,
          userId: ctx.from.id,
          mode: 'custom',
          groupId: N,
          runId,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
        });

        await ctx.reply(
          `Пришлите один или несколько скриншотов для Custom ${N} (изображения JPG/PNG/WEBP/GIF).\n` +
          `Когда закончите — отправьте /done. Для отмены — /cancel.`
        );
        return;
      }
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getCustomGroup(chatId, N);
        if (!g) { await ctx.reply(`Custom ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);
        const res = await deleteScreenshotsForGroup(chatId, 'custom', N, runId);
        await ctx.reply(`Deleted for Custom ${N}: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
        return;
      }
      await showCustomGroupScreenshots(ctx, chatId, N);
      return;
    }

    await ctx.reply('Неизвестная опция /c. Используйте: addp | delp | maps | mix | points | time | demos | screenshots или без параметров для просмотра.');
    return;
  }

  await ctx.reply('Неизвестная опция /custom. Используйте /c, /c add, /c del, /c delall, /c N ...');
}


bot.command(['custom', 'c'], customHandler);


// USERS (глобальные профили игроков)
async function usersHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }

  if (!ctx.from) {
    await ctx.reply('Не удалось определить пользователя.');
    return;
  }

  const chat = ctx.chat || {};
  const isPrivate = chat.type === 'private';
  const userId = ctx.from.id;
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);
  const sub = (tokens[0] || '').toLowerCase();

  // /users all — только для админов/владельцев
  if (sub === 'all') {
    if (!(await requireAdminGuard(ctx, { ignoreLock: true }))) return;

    const users = await colUsers.find({}).sort({ nickNorm: 1, telegramId: 1 }).toArray();
    await replyPre(ctx, formatUsersListForDisplay(users));
    return;
  }

  // /users add — регистрация (только в личке с ботом)
  if (sub === 'add') {
    if (!isPrivate) {
      await ctx.reply('Для регистрации профиля напишите боту в личку и выполните там команду /u add.');
      return;
    }

    const existing = await findUserByTelegramId(userId);
    if (existing) {
      await ctx.reply('Вы уже зарегистрированы.\n\nПосмотреть профиль: /u\nИзменить профиль: /u edit.');
      return;
    }

    const uKey = userProfileKey(userId);
    userProfileSessions.set(uKey, {
      mode: 'add',
      step: 'nick',
      nick: null,
      nickNorm: null,
      bio: null,
      startedAt: Date.now(),
    });

    await ctx.reply('Регистрация профиля игрока.\n\nВведите желаемый ник (игровое имя).\n\nОтмена — /cancel.');
    return;
  }

  // /users edit — редактирование (только в личке)
  if (sub === 'edit') {
    if (!isPrivate) {
      await ctx.reply('Для редактирования профиля напишите боту в личку и выполните там команду /u edit.');
      return;
    }

    const existing = await findUserByTelegramId(userId);
    if (!existing) {
      await ctx.reply('Вы ещё не зарегистрированы. Сначала выполните /u add.');
      return;
    }

    const uKey = userProfileKey(userId);
    userProfileSessions.set(uKey, {
      mode: 'edit',
      step: 'nick',
      nick: existing.nick || '',
      nickNorm: existing.nickNorm || (existing.nick ? norm(existing.nick) : null),
      bio: existing.bio || '',
      startedAt: Date.now(),
    });

    const currentNick = existing.nick || '(не задан)';
    const text =
      'Редактирование профиля игрока.\n\n' +
      `Текущий ник: ${currentNick}\n\n` +
      'Отправьте новый ник.\nЕсли хотите оставить как есть — просто повторите текущий ник.\n\n' +
      'Отмена — /cancel.';

    try {
      // если ник без спецсимволов — можно в Markdown, иначе обычный текст
      await ctx.reply(text);
    } catch (_) {
      await ctx.reply(text);
    }
    return;
  }

  // /users или /u без параметров — показать профиль текущего пользователя
  const profile = await findUserByTelegramId(userId);
  if (!profile) {
    if (isPrivate) {
      await ctx.reply(
        'Профиль игрока для вас пока не создан.\n\n' +
        'Чтобы зарегистрироваться, выполните /u add и следуйте инструкциям.'
      );
    } else {
      await ctx.reply(
        'Профиль игрока для вас пока не создан.\n\n' +
        'Чтобы зарегистрироваться, напишите боту в личку и выполните там команду /u add.'
      );
    }
    return;
  }

  await replyPre(ctx, formatUserProfileForDisplay(profile));
}

bot.command(['users', 'u'], usersHandler);

// TEAMS (глобальные игровые команды)
async function teamsHandler(ctx) {
  if (!requireGroupContext(ctx)) return;

  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from?.id;
  if (!userId) {
    await ctx.reply('Не удалось определить пользователя.');
    return;
  }

  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);
  const sub = (tokens[0] || '').toLowerCase();

  // Помощник: загрузить текущего пользователя из colUsers
  const me = await findUserByTelegramId(userId);

  // /teams all — только для админов, игнорирует lock турнира
  if (sub === 'all') {
    if (!(await requireAdminGuard(ctx, { ignoreLock: true }))) return;

    const teams = await colTeams.find({}).sort({ nameNorm: 1 }).toArray();
    await replyPre(ctx, formatTeamsListForDisplay(teams));
    return;
  }

  // /teams add — админы или зарегистрированные users
  if (sub === 'add') {
    let canCreate = false;
    if (me) canCreate = true;
    else if (await isChatAdminOrOwner(ctx, chatId)) canCreate = true;

    if (!canCreate) {
      await ctx.reply(
        'Для создания игровой команды нужно быть зарегистрированным игроком (/u add)\n' +
        'или администратором турнира.'
      );
      return;
    }

    const tKey = teamSessionKey(userId);
    teamSessions.set(tKey, {
      mode: 'add',
      step: 'name',
      teamId: null,
      name: '',
      nameNorm: '',
      description: '',
      memberIds: [],
      memberNicks: [],
      startedAt: Date.now(),
    });

    await ctx.reply(
      'Создание новой игровой команды.\n\n' +
      'Шаг 1. Отправьте название команды.\n\n' +
      'Отмена — /cancel.'
    );
    return;
  }

  // /teams edit — редактирование команды, где текущий пользователь включён в состав
  if (sub === 'edit') {
    if (!me) {
      await ctx.reply(
        'Вы ещё не зарегистрированы как игрок.\n' +
        'Сначала выполните /u add для регистрации профиля.'
      );
      return;
    }

    const teams = await colTeams
      .find({ memberIds: me._id })
      .sort({ nameNorm: 1 })
      .toArray();

    if (!teams.length) {
      await ctx.reply(
        'Вы пока не входите ни в одну игровую команду.\n' +
        'Можно создать новую команду через /teams add.'
      );
      return;
    }

    if (teams.length === 1) {
      const team = teams[0];
      const tKey = teamSessionKey(userId);
      teamSessions.set(tKey, {
        mode: 'edit',
        step: 'name',
        teamId: String(team._id),
        name: team.name || '',
        nameNorm: team.nameNorm || (team.name ? norm(team.name) : ''),
        description: team.description || '',
        memberIds: team.memberIds || [],
        memberNicks: team.memberNicks || [],
        startedAt: Date.now(),
      });

      await replyPre(ctx, formatTeamForDisplay(team));
      await ctx.reply(
        '\nРедактирование этой команды.\n\n' +
        'Сначала отправьте новое название команды.\n' +
        'Отмена — /cancel.'
      );
      return;
    }

    // Несколько команд — даём выбор по номеру
    const tsKey = teamSelectKey(userId);
    teamSelectSessions.set(tsKey, {
      teams,
      startedAt: Date.now(),
    });

    const listText = formatTeamsListForDisplay(teams);
    await replyPre(ctx,
      listText +
      '\n\nУкажите номер команды, которую хотите отредактировать, или /cancel для отмены.'
    );
    return;
  }

  // /teams или /tm без параметров — показать команды, где участвует текущий пользователь
  if (!me) {
    await ctx.reply(
      'Вы ещё не зарегистрированы как игрок.\n' +
      'Сначала выполните /u add для регистрации профиля.'
    );
    return;
  }

  const myTeams = await colTeams
    .find({ memberIds: me._id })
    .sort({ nameNorm: 1 })
    .toArray();

  if (!myTeams.length) {
    await ctx.reply(
      'Вы пока не входите ни в одну игровую команду.\n' +
      'Можно создать новую команду через /teams add.'
    );
    return;
  }

  await replyPre(ctx, formatTeamsListForDisplay(myTeams));
}

bot.command(['teams', 'tm'], teamsHandler);

async function teamAdminHandler(ctx) {
  if (!requireGroupContext(ctx)) return;

  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from?.id;
  if (!userId) {
    await ctx.reply('Не удалось определить пользователя.');
    return;
  }

  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  const sub = (tokens[0] || '').toLowerCase();
  if (sub !== 'editadm') {
    await ctx.reply('Использование: /team editadm <teamId>');
    return;
  }

  const teamId = tokens[1];
  if (!teamId) {
    await ctx.reply('Укажите ID команды. Пример: /team editadm 691904031ce2010bfc82e308');
    return;
  }

  if (!(await requireAdminGuard(ctx, { ignoreLock: true }))) return;

  const team = await findTeamById(teamId);
  if (!team) {
    await ctx.reply('Команда с таким ID не найдена.');
    return;
  }

  const tKey = teamSessionKey(userId);
  teamSessions.set(tKey, {
    mode: 'edit',
    step: 'name',
    teamId: String(team._id),
    name: team.name || '',
    nameNorm: team.nameNorm || (team.name ? norm(team.name) : ''),
    description: team.description || '',
    memberIds: team.memberIds || [],
    memberNicks: team.memberNicks || [],
    startedAt: Date.now(),
  });

  await replyPre(ctx, formatTeamForDisplay(team));
  await ctx.reply(
    '\nАдмин-редактирование команды.\n\n' +
    'Сначала отправьте новое название команды.\n' +
    'Отмена — /cancel.'
  );
}

bot.command('team', teamAdminHandler);

// Command handlers
bot.use(async (ctx, next) => {
  // rate limit
  if (!rateLimit(ctx)) {
    try {
      await ctx.reply('Слишком много команд. Пожалуйста, подождите секунду.');
    } catch (e) { }
    return;
  }

  // record seen user info (helps mapping usernames)
  if (ctx.from) {
    try { await recordUser(ctx, ctx.from); } catch (e) { }
  }


  const msg = ctx.message || ctx.update?.message || ctx.update?.edited_message;

  // НОВОЕ: универсальная защита для всех команд с "delall" — требует явного подтверждения "confirm"
  // Срабатывает для любых команд, где встречается отдельный токен "delall" (или "dellall") без "confirm"
  if (msg && typeof msg.text === 'string') {
    const text = msg.text.trim();
    if (text.startsWith('/')) {
      const hasDelAll = /\bdelall\b/i.test(text) || /\bdellall\b/i.test(text);
      const hasConfirm = /\bconfirm\b/i.test(text);
      if (hasDelAll && !hasConfirm) {
        try {
          await ctx.reply(
            '⚠️ Опасная операция удаления.\n' +
            'Для подтверждения повторите команду, добавив "confirm" в конец.\n' +
            'Примеры:\n' +
            '- /sg delall confirm\n' +
            '- /m delall confirm\n' +
            '- /groups delall confirm\n' +
            '- /groups screenshots delall confirm\n' +
            '- /groups news delall confirm\n' +
            '- /finals delall confirm\n' +
            '- /finals screenshots delall confirm\n' +
            '- /finals news delall confirm\n' +
            '- /superfinal delall confirm\n' +
            '- /superfinal screenshots delall confirm\n' +
            '- /superfinal news delall confirm\n' +
            '- /custom delall confirm\n' +
            '- /achievements delall confirm\n' +
            '- /tournament delall confirm\n' +
            '- /delall confirm'
          );
        } catch (_) { }
        return; // блокируем выполнение команды без подтверждения
      }
    }
  }

  // handle chat migration
  if (msg && msg.migrate_to_chat_id) {
    const oldId = msg.chat.id;
    const newId = msg.migrate_to_chat_id;
    try {
      await migrateChat(oldId, newId);
    } catch (e) {
      console.error('Migration error', e);
    }
  }

  return next();
});

// ПОЛНАЯ ЗАМЕНА обработчика текстов — добавлена поддержка сессий редактирования ачивок (name/desc)
bot.on('text', async (ctx, next) => {
  if (!requireGroupContext(ctx)) return next();

  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from.id;

  // 0.05) --- Сессии мастера регистрации /register (игрок/команда с автоподачей заявки) ---
  {
    const sKey = signupWizardKey(chatId, userId);
    const sSess = signupWizardSessions.get(sKey);
    if (sSess) {
      const txt = (ctx.message.text || '').trim();
      if (!txt || txt.startsWith('/')) {
        // Команды (/cancel и др.) обрабатываются отдельными хэндлерами
        return next();
      }

      try {
        // Шаг 1: ник игрока
        if (sSess.step === 'player_nick') {
          const nick = txt;
          if (nick.length < 2 || nick.length > 50) {
            await ctx.reply('Ник должен содержать от 2 до 50 символов. Попробуйте ещё раз.');
            return;
          }

          const exists = await findUserByNick(nick, null);
          if (exists) {
            await ctx.reply('Такой ник уже используется другим пользователем. Выберите другой ник.');
            return;
          }

          sSess.playerNick = nick;
          sSess.playerNickNorm = norm(nick);
          sSess.step = 'player_bio';
          signupWizardSessions.set(sKey, sSess);

          await ctx.reply(
            'Теперь отправьте краткое описание игрока (био). Можно пропустить, отправив один дефис "-".\n\n' +
            'Отмена — /cancel.'
          );
          return;
        }

        // Шаг 2: описание игрока
        if (sSess.step === 'player_bio') {
          let bio = txt;
          if (bio === '-') bio = '';
          if (bio.length > 1000) {
            await ctx.reply('Описание слишком длинное (максимум 1000 символов). Сократите текст и отправьте снова.');
            return;
          }

          const now = new Date();
          const from = ctx.from || {};
          const base = {
            username: from.username || null,
            firstName: from.first_name || null,
            lastName: from.last_name || null,
            updatedAt: now,
          };

          // защита от двойной регистрации: вдруг пользователь успел сделать /u add
          const already = await findUserByTelegramId(userId);
          if (already && !sSess.playerId) {
            sSess.playerId = already._id;
            sSess.playerNick = already.nick;
            sSess.playerNickNorm = already.nickNorm;
            sSess.playerBio = already.bio || '';
          } else if (!already) {
            const doc = {
              telegramId: Number(userId),
              nick: sSess.playerNick,
              nickNorm: sSess.playerNickNorm,
              bio,
              createdAt: now,
              ...base,
            };

            const res = await colUsers.insertOne(doc);
            sSess.playerId = res.insertedId;
            sSess.playerNick = doc.nick;
            sSess.playerNickNorm = doc.nickNorm;
            sSess.playerBio = doc.bio;
          } else {
            // already есть и playerId уже был записан — ничего не делаем
          }

          // FFA / 1v1 — после игрока сразу подаём заявку
          if (sSess.tournamentType === 'FFA' || sSess.tournamentType === '1v1') {
            const userDoc = await colUsers.findOne({ _id: sSess.playerId });
            if (!userDoc) {
              signupWizardSessions.delete(sKey);
              await ctx.reply('Не удалось найти профиль игрока после регистрации. Попробуйте ещё раз /register.');
              return;
            }

            signupWizardSessions.delete(sKey);
            await createPlayerSignupAndReply(ctx, chatId, { tournamentType: sSess.tournamentType }, userDoc);
            return;
          }

          // TDM — после регистрации игрока переходим к созданию команды
          if (sSess.tournamentType === 'TDM') {
            sSess.step = 'team_name';
            signupWizardSessions.set(sKey, sSess);

            await ctx.reply(
              'Профиль игрока зарегистрирован.\n\n' +
              'Теперь создадим игровую команду.\n\n' +
              'Шаг 1. Отправьте название команды.\n\n' +
              'Отмена — /cancel.'
            );
            return;
          }

          // fallback
          signupWizardSessions.delete(sKey);
          await ctx.reply('Регистрация завершена, но тип турнира не распознан. Обратитесь к администратору.');
          return;
        }

        // Шаг 3: название команды (TDM)
        if (sSess.step === 'team_name') {
          const name = txt;
          if (name.length < 2 || name.length > 100) {
            await ctx.reply('Название команды должно содержать от 2 до 100 символов. Попробуйте ещё раз.');
            return;
          }

          const nameNorm = norm(name);
          const exists = await colTeams.findOne({ nameNorm });
          if (exists) {
            await ctx.reply('Команда с таким названием уже существует. Введите другое название.');
            return;
          }

          sSess.teamName = name;
          sSess.teamNameNorm = nameNorm;
          sSess.step = 'team_desc';
          signupWizardSessions.set(sKey, sSess);

          await ctx.reply(
            'Шаг 2. Отправьте краткое описание команды. Можно пропустить, отправив один дефис "-".\n\n' +
            'Отмена — /cancel.'
          );
          return;
        }

        // Шаг 4: описание команды (TDM), создаём команду и заявку
        if (sSess.step === 'team_desc') {
          let desc = txt;
          if (desc === '-') desc = '';
          if (desc.length > 2000) {
            await ctx.reply('Описание слишком длинное (максимум 2000 символов). Сократите текст и отправьте снова.');
            return;
          }

          const now = new Date();
          const from = ctx.from || {};

          // подстрахуемся — найдём игрока
          const userDoc = await colUsers.findOne({ _id: sSess.playerId }) ||
            await findUserByTelegramId(userId);
          if (!userDoc) {
            signupWizardSessions.delete(sKey);
            await ctx.reply('Не удалось найти профиль игрока при создании команды. Попробуйте ещё раз /register.');
            return;
          }

          const doc = {
            name: sSess.teamName,
            nameNorm: sSess.teamNameNorm,
            description: desc || '',
            memberIds: [userDoc._id],
            memberNicks: [userDoc.nick || ''],
            createdByTelegramId: userId,
            createdByUsername: from.username || null,
            createdAt: now,
            updatedAt: now,
          };

          const res = await colTeams.insertOne(doc);
          const savedTeam = { ...doc, _id: res.insertedId };

          // Завершили мастер — удаляем сессию
          signupWizardSessions.delete(sKey);

          // Создаём заявку от команды
          await createTeamSignupAndReply(ctx, chatId, { tournamentType: 'TDM' }, savedTeam);
          return;
        }

        // fallback
        signupWizardSessions.delete(sKey);
        await ctx.reply('Состояние мастера регистрации не распознано. Попробуйте начать заново через /register.');
        return;
      } catch (e) {
        console.error('signupWizardSessions error', e);
        signupWizardSessions.delete(sKey);
        await ctx.reply('Ошибка обработки регистрации. Попробуйте ещё раз /register.');
        return;
      }
    }
  }

  // 0) --- NEW: сбор текста для активной feedback-сессии (add/edit) ---
  // используем общую in-memory Map feedbackSessions с ключом `${chatId}:${userId}`
  // --- Feedback session handling ---
  const fKey = fbKey(chatId, userId);
  const fSess = feedbackSessions.get(fKey);
  if (fSess) {
    const txt = (ctx.message.text || '').trim();
    if (!txt || txt.startsWith('/')) {
      return next(); // **Важно**: пропускаем команды (/done, /cancel) к их обработчикам
    }
    (fSess.buffer ||= []).push(txt);
    return; // для обычного текста остаёмся в режиме ввода фидбэка и не передаём дальше
  }

  // 0.1) --- NEW: сессии профиля пользователя (только в личке) ---
  if (ctx.chat?.type === 'private') {
    const uKey = userProfileKey(userId);
    const uSess = userProfileSessions.get(uKey);
    if (uSess) {
      const txt = (ctx.message.text || '').trim();
      if (!txt || txt.startsWith('/')) {
        // команды (/cancel, /done и т.п.) обрабатываются отдельными хэндлерами
        return next();
      }

      try {
        if (uSess.step === 'nick') {
          const nick = txt;
          if (nick.length < 2 || nick.length > 50) {
            await ctx.reply('Ник должен содержать от 2 до 50 символов. Попробуйте ещё раз.');
            return;
          }

          const exists = await findUserByNick(nick, uSess.mode === 'edit' ? userId : null);
          if (exists) {
            await ctx.reply('Такой ник уже используется другим пользователем. Выберите другой ник.');
            return;
          }

          uSess.nick = nick;
          uSess.nickNorm = norm(nick);
          uSess.step = 'bio';
          userProfileSessions.set(uKey, uSess);

          await ctx.reply('Ок. Теперь отправьте короткое описание о себе (одно или несколько предложений).\n\nОтмена — /cancel.');
          return;
        }

        if (uSess.step === 'bio') {
          const bio = txt;
          if (bio.length > 1000) {
            await ctx.reply('Описание слишком длинное (максимум 1000 символов). Сократите текст и отправьте снова.');
            return;
          }

          const now = new Date();
          const from = ctx.from || {};
          const base = {
            username: from.username || null,
            firstName: from.first_name || null,
            lastName: from.last_name || null,
            updatedAt: now,
          };

          if (uSess.mode === 'add') {
            // защита от двойной регистрации
            const already = await findUserByTelegramId(userId);
            if (already) {
              userProfileSessions.delete(uKey);
              await ctx.reply('Вы уже зарегистрированы. Посмотреть профиль: /u . Изменить профиль: /u edit.');
              return;
            }

            const doc = {
              telegramId: Number(userId),
              nick: uSess.nick,
              nickNorm: uSess.nickNorm,
              bio,
              createdAt: now,
              ...base,
            };

            await colUsers.insertOne(doc);
            userProfileSessions.delete(uKey);

            await ctx.reply('Профиль игрока создан.');
            await replyPre(ctx, formatUserProfileForDisplay(doc));
            return;
          }

          if (uSess.mode === 'edit') {
            const upd = await colUsers.updateOne(
              { telegramId: Number(userId) },
              {
                $set: {
                  nick: uSess.nick,
                  nickNorm: uSess.nickNorm,
                  bio,
                  ...base,
                },
              },
            );

            userProfileSessions.delete(uKey);

            if (!upd.matchedCount) {
              await ctx.reply('Профиль не найден. Попробуйте сначала выполнить /u add.');
              return;
            }

            const fresh = await findUserByTelegramId(userId);
            await ctx.reply('Профиль игрока обновлён.');
            if (fresh) {
              await replyPre(ctx, formatUserProfileForDisplay(fresh));
            }
            return;
          }

          // неизвестный режим — сбрасываем
          userProfileSessions.delete(uKey);
          await ctx.reply('Неизвестный режим редактирования профиля. Попробуйте /u или /u edit ещё раз.');
          return;
        }
      } catch (e) {
        console.error('userProfileSessions error', e);
        userProfileSessions.delete(uKey);
        await ctx.reply('Ошибка сохранения профиля. Попробуйте ещё раз позже.');
        return;
      }
    }
  }

  // 0.2) --- Выбор команды для /teams edit (если у пользователя несколько команд) ---
  {
    const tsKey = teamSelectKey(userId);
    const tsSess = teamSelectSessions.get(tsKey);
    if (tsSess) {
      const txt = (ctx.message.text || '').trim();
      if (!txt || txt.startsWith('/')) {
        // Команды (/cancel, /done) обрабатываются отдельно
        return next();
      }

      const n = Number(txt);
      if (!Number.isInteger(n) || n < 1 || n > tsSess.teams.length) {
        await ctx.reply(`Введите номер команды от 1 до ${tsSess.teams.length}, либо /cancel для отмены.`);
        return;
      }

      const team = tsSess.teams[n - 1];
      teamSelectSessions.delete(tsKey);

      const tKey = teamSessionKey(userId);
      teamSessions.set(tKey, {
        mode: 'edit',
        step: 'name',
        teamId: String(team._id),
        name: team.name || '',
        description: team.description || '',
        memberNicks: team.memberNicks || [],
        memberIds: team.memberIds || [],
        startedAt: Date.now(),
      });

      await ctx.reply(
        'Редактирование команды.\n\n' +
        `Текущее название: ${team.name || '(не задано)'}\n\n` +
        'Отправьте новое название команды.\n' +
        'Отмена — /cancel.'
      );
      return;
    }
  }

  // 0.25) --- Выбор команды для /signup add (TDM, если у пользователя несколько команд) ---
  {
    const stKey = signupTeamSelectKey(chatId, userId);
    const stSess = signupTeamSelectSessions.get(stKey);
    if (stSess) {
      const txt = (ctx.message.text || '').trim();
      if (!txt || txt.startsWith('/')) {
        // /cancel и прочие команды пойдут дальше по пайплайну
        return next();
      }

      const n = Number(txt);
      if (!Number.isInteger(n) || n < 1 || n > stSess.teams.length) {
        await ctx.reply(`Введите номер команды от 1 до ${stSess.teams.length}, либо /cancel для отмены.`);
        return;
      }

      const team = stSess.teams[n - 1];

      try {
        // На всякий случай — проверим, что тип турнира всё ещё TDM
        const reg = await getRegistrationSettings(chatId);
        if (reg.tournamentType !== 'TDM') {
          signupTeamSelectSessions.delete(stKey);
          await ctx.reply('Тип турнира был изменён. Повторите /signup add.');
          return;
        }

        // Проверяем, не успела ли команда уже подать заявку
        const existing = await colSignups.findOne({
          chatId,
          kind: 'team',
          teamId: team._id,
        });
        if (existing) {
          signupTeamSelectSessions.delete(stKey);
          await ctx.reply(
            'Команда уже подала заявку на этот турнир.\n\n' +
            `ID заявки: ${existing.signupId}`
          );
          return;
        }

        const now2 = new Date();
        const signupId = generateSignupId();
        await colSignups.insertOne({
          chatId,
          signupId,
          kind: 'team',
          teamId: team._id,
          teamName: team.name,
          teamNameNorm: team.nameNorm,
          teamMembers: team.memberNicks || [],
          confirmed: false,
          createdByTelegramId: userId,
          createdByUsername: ctx.from?.username || null,
          createdAt: now2,
          updatedAt: now2,
        });

        signupTeamSelectSessions.delete(stKey);

        await ctx.reply(
          `Заявка команды "${team.name}" на участие в турнире принята.\n` +
          `ID заявки: ${signupId}`
        );
      } catch (e) {
        console.error('signupTeamSelectSessions error', e);
        signupTeamSelectSessions.delete(stKey);
        await ctx.reply('Не удалось сохранить заявку. Попробуйте ещё раз /signup add.');
      }
      return;
    }
  }

  // 0.3) --- Сессии создания/редактирования команды (name → description → members → /done) ---
  {
    const tKey = teamSessionKey(userId);
    const tSess = teamSessions.get(tKey);
    if (tSess) {
      const txt = (ctx.message.text || '').trim();
      if (!txt || txt.startsWith('/')) {
        // /cancel или /done пойдут в соответствующие хэндлеры
        return next();
      }

      // Шаг 1: название
      if (tSess.step === 'name') {
        if (txt.length < 2 || txt.length > 100) {
          await ctx.reply('Название команды должно содержать от 2 до 100 символов. Попробуйте ещё раз.');
          return;
        }

        const nameNorm = norm(txt);
        const exists = await colTeams.findOne({
          nameNorm,
          ...(tSess.mode === 'edit'
            ? { _id: { $ne: new ObjectId(tSess.teamId) } }
            : {}),
        });

        if (exists) {
          await ctx.reply('Команда с таким названием уже существует. Введите другое название.');
          return;
        }

        tSess.name = txt;
        tSess.nameNorm = nameNorm;
        tSess.step = 'description';
        teamSessions.set(tKey, tSess);

        await ctx.reply('Ок. Теперь отправьте описание команды.\n\nОтмена — /cancel.');
        return;
      }

      // Шаг 2: описание
      if (tSess.step === 'description') {
        if (txt.length > 2000) {
          await ctx.reply('Описание слишком длинное (максимум 2000 символов). Сократите текст и отправьте снова.');
          return;
        }

        tSess.description = txt;
        tSess.step = 'members';
        teamSessions.set(tKey, tSess);

        await ctx.reply(
          'Теперь отправьте список ников участников команды через запятую.\n' +
          'Например: ly, aid, slonik\n\n' +
          'Все ники должны быть зарегистрированными пользователями (/u add).\n' +
          'Отмена — /cancel.'
        );
        return;
      }

      // Шаг 3: список участников
      if (tSess.step === 'members') {
        const raw = txt.split(',').map(s => s.trim()).filter(Boolean);
        if (!raw.length) {
          await ctx.reply('Список участников пуст. Укажите хотя бы один ник.');
          return;
        }

        // удаляем дубликаты по нормализованному нику
        const seen = new Set();
        const nicks = [];
        for (const r of raw) {
          const nn = norm(r);
          if (!nn) continue;
          if (seen.has(nn)) continue;
          seen.add(nn);
          nicks.push({ nick: r, nickNorm: nn });
        }

        if (!nicks.length) {
          await ctx.reply('Не удалось разобрать список ников. Попробуйте ещё раз.');
          return;
        }

        // Ищем всех пользователей
        const nickNorms = nicks.map(x => x.nickNorm);
        const users = await colUsers.find({ nickNorm: { $in: nickNorms } }).toArray();

        if (users.length !== nicks.length) {
          // Найдём, каких ников не хватает
          const found = new Set(users.map(u => u.nickNorm));
          const missing = nicks.filter(x => !found.has(x.nickNorm)).map(x => x.nick);
          await ctx.reply(
            'Не все указанные ники найдены среди зарегистрированных пользователей.\n' +
            'Следующие ники отсутствуют в базе:\n' +
            missing.join(', ') + '\n\n' +
            'Попросите этих пользователей зарегистрироваться через /u add,\n' +
            'или скорректируйте список и отправьте его ещё раз.'
          );
          return;
        }

        tSess.memberIds = users.map(u => u._id);
        tSess.memberNicks = users.map(u => u.nick);
        tSess.step = 'confirm';
        teamSessions.set(tKey, tSess);

        const previewLines = [];
        previewLines.push('Проверьте данные команды:');
        previewLines.push('');
        previewLines.push(`Название: ${tSess.name}`);
        previewLines.push('');
        previewLines.push('Описание:');
        previewLines.push(tSess.description || '(без описания)');
        previewLines.push('');
        previewLines.push('Состав:');
        previewLines.push(tSess.memberNicks.join(', '));
        previewLines.push('');
        previewLines.push('Если все данные верны — отправьте /done для сохранения.');
        previewLines.push('Для отмены используйте /cancel.');

        await replyPre(ctx, previewLines.join('\n'));
        return;
      }

      // Если step неизвестен — сбрасываем
      teamSessions.delete(tKey);
      await ctx.reply('Неизвестный шаг редактирования команды. Попробуйте ещё раз с /teams add или /teams edit.');
      return;
    }
  }

  const key = achvKey(chatId, userId);

  // 1) Черновик новой ачивки (после /done — ждём ТЕКСТ описания)
  const draft = achvDrafts.get(key);
  if (draft) {
    const txt = (ctx.message.text || '').trim();
    if (!txt || txt.startsWith('/')) return next(); // команды не перехватываем
    try {
      const idx = await addAchievement(chatId, {
        name: draft.name,
        desc: txt,
        image: draft.image,
        createdBy: { id: userId, username: ctx.from?.username || null },
        type: 'achievement', // по умолчанию
      });
      achvDrafts.delete(key);
      await ctx.reply(`Ачивка сохранена с номером ${idx}.`);
    } catch (e) {
      console.error('addAchievement error', e);
      await ctx.reply('Ошибка сохранения ачивки. Попробуйте ещё раз.');
    }
    return; // не зовём next — чтобы текст не ушёл в другие обработчики
  }

  // 2) Сессия редактирования (name/desc), в т.ч. цепочка 'all'
  const edit = achvEditSessions.get(key);
  if (edit) {
    const txt = (ctx.message.text || '').trim();
    if (!txt || txt.startsWith('/')) return next(); // команды обрабатываются отдельными хэндлерами

    try {
      const ach = await getAchievement(chatId, edit.idx);
      if (!ach) {
        achvEditSessions.delete(key);
        await ctx.reply(`Achievement ${edit.idx} not found.`);
        return;
      }

      if (edit.mode === 'name') {
        await colAchievements.updateOne(
          { chatId, idx: Number(edit.idx) },
          { $set: { name: txt, updatedAt: new Date() } }
        );
        await ctx.reply(`Название ачивки ${edit.idx} обновлено: ${txt}`);
        achvEditSessions.delete(key);

        if (edit.chain) {
          // стартуем этап загрузки логотипа
          const sKey = ssKey(chatId, userId);
          screenshotSessions.set(sKey, {
            chatId,
            userId,
            mode: 'achv_logo',
            achvIdx: Number(edit.idx),
            chain: true,
            startedAt: Date.now(),
            expiresAt: Date.now() + 10 * 60 * 1000,
            count: 0,
            tempImage: null,
          });
          await ctx.reply(
            `Шаг 2/3. Пришлите новую картинку для ачивки ${edit.idx} (JPG/PNG/WEBP/GIF). ` +
            `Когда закончите — отправьте /done. Для отмены — /cancel.`
          );
        }
        return;
      }

      if (edit.mode === 'desc') {
        await colAchievements.updateOne(
          { chatId, idx: Number(edit.idx) },
          { $set: { desc: txt, updatedAt: new Date() } }
        );
        achvEditSessions.delete(key);
        await ctx.reply(`Описание ачивки ${edit.idx} обновлено.`);
        return;
      }

      // fallback
      achvEditSessions.delete(key);
    } catch (e) {
      console.error('achv edit text handler error', e);
      await ctx.reply('Ошибка. Попробуйте ещё раз.');
    }
    return;
  }

  // 3) --- NEW: ввод текста внутри конструктора новости (/t news2 add -> /text) ---
  purgeExpiredNews2Sessions();
  const nkey = n2Key(chatId, userId);
  const n2 = news2Sessions.get(nkey);
  if (n2 && n2.waiting === 'text') {
    const txt = (ctx.message.text || '').trim();
    if (!txt || txt.startsWith('/')) return next(); // команды пропускаем дальше
    n2.blocks.push({ type: 'text', text: txt, createdAt: new Date() });
    n2.waiting = null;
    n2.expiresAt = Date.now() + 30 * 60 * 1000;
    news2Sessions.set(nkey, n2);
    await ctx.reply('Текст добавлен.');
    await promptNews2Next(ctx);
    return; // текст обработан
  }

  // иначе — дальше по остальным обработчикам
  return next();
});


bot.catch(async (err, ctx) => {
  console.error('Telegraf error for update', ctx.update, err);
  try {
    await ctx.reply('Internal error. Please try again.');
  } catch (_) { }
});

bot.start(async ctx => {
  await ctx.reply(
    'Привет! Я бот для управления скилл-группами, картами и формирования игровых/финальных групп.\n' +
    'Используйте: /help'
  );
});

// /whoami
bot.command('whoami', async ctx => {
  const u = ctx.from;
  await ctx.reply(`Your ID: ${u.id}\nUsername: ${u.username ? '@' + u.username : '(none)'}`);
});

// /chatid — показать ID текущего чата и ID темы (если есть)
bot.command(['chatid', 'cid'], async ctx => {
  const chat = ctx.chat || {};
  const msg = ctx.message || {};
  const threadId = typeof msg.message_thread_id === 'number' ? msg.message_thread_id : null;

  const lines = [];
  lines.push(`Chat ID: ${chat.id ?? '(unknown)'}`);
  lines.push(`Type: ${chat.type ?? '(unknown)'}`);
  if (chat.title) lines.push(`Title: ${chat.title}`);
  if (chat.username) lines.push(`Username: @${chat.username}`);
  if (threadId !== null) lines.push(`Topic ID: ${threadId}`); // для форумных тем в супергруппах

  await ctx.reply(lines.join('\n'));
});


// HELP
// ПОЛНАЯ ЗАМЕНА helpText() — добавлены команды по type и edit для ачивок
// ПОЛНАЯ ЗАМЕНА helpText() — добавлены /t newschannel и пояснения
// ПОЛНАЯ ЗАМЕНА helpText() — обновлено описание: /t newschannel makenews публикует ВСЕ новости всех стадий,
// а добавление новостей в /g, /f, /s тоже дублируется в канал при наличии привязки
// HELP
// ПОЛНАЯ ЗАМЕНА helpText() — добавлены команды по персональной статистике и /setid
async function helpText() {
  return [
    'Команды (публичные):',
    '/help (/h) — это меню',
    '/info (/i) — сводная информация (турнир, SG, карты, рейтинги, игровые/финальные/суперфинальные группы — вывод в 2 колонки)',
    '/tournament (/t) info — сводка турнира',
    '/custom (/c) — список всех произвольных групп; /c N — показать группу N',
    '',
    '--------------------------',
    'Администраторы и роли:',
    '/admin (/a) — список админов',
    '/admin add @nick | user_id — добавить админа',
    '/admin del @nick | user_id — удалить админа',
    '/admin delall — удалить всех админов (владельцев удалить нельзя)',
    '/admin role — список ролей и назначенных пользователей (по текущему чату)',
    '/admin role add <rolename> @nick | user_id — назначить роль пользователю в текущем чате',
    '/admin role del <rolename> @nick | user_id — удалить роль у пользователя в текущем чате',
    '  Поддерживаемые роли:',
    '  - Achievements — даёт доступ ко всем НЕпубличным командам /achievements (как у админа чата)',
    '  - News — даёт доступ к добавлению и редактированию новостей в текущем чате (турнирные, групповые, финальные, суперфинальные, /news del|edit).',
    '    Полное удаление всех новостей (команды с delall) остаётся только для админов.',
    '',
    '--------------------------',
    'Все новости по турниру:',
    '/news — показать все новости по текущему чату (всех разделов) с ID',
    '/news del <id> — удалить новость по ID (админы чата или роль News)',
    '/news edit <id> <текст> — изменить новость по ID (админы чата или роль News)',
    '',
    '--------------------------',
    '0) Турнир:',
    '/tournament (/t) name [value] — показать/задать название',
    '/tournament (/t) site [url] — показать/задать сайт',
    '/tournament (/t) desc [text] — показать/задать описание',
    '/tournament (/t) logo — показать текущий логотип турнира',
    '/tournament (/t) logo add — загрузить логотип (JPG/PNG/WEBP/GIF, завершить: /done, отменить: /cancel) (только админы)',
    '/tournament (/t) news — показать новости турнира (с ID)',
    '/tournament (/t) news add <text> — добавить новость (только админы; при привязанном канале пост автоматически публикуется в канале)',
    '/tournament (/t) news edit <id> <text> — изменить новость по ID (только админы)',
    '/tournament (/t) news2 add — добавить новость одним сообщением: картинка + подпись. (только админы)',
    '/tournament (/t) news <id> — показать новость по ID',
    '/tournament (/t) news delall — удалить все новости турнира (только админы)',
    '/tournament (/t) newschannel — показать текущий привязанный канал новостей (публично)',
    '/tournament (/t) newschannel add @tgchannel — привязать канал новостей (бот должен быть админом в канале) (только админы)',
    '/tournament (/t) newschannel makenews — опубликовать ВСЕ новости из БД (турнир, группы, финалы, суперфиналы) в привязанный канал (только админы)',
    '/tournament (/t) servers — показать список серверов',
    '/tournament (/t) servers add <server1,server2,...> — задать список серверов (перезапись, только админы)',
    '/tournament (/t) pack [url] — показать/задать ссылку на турнирный пак (только админы)',
    '/tournament (/t) streams — показать список стримов',
    '/tournament (/t) streams <url1,url2,...> — задать список стримов (перезапись, только админы)',
    '/tournament (/t) demos — сводный список ссылок на демо по всем стадиям (группы, финалы, суперфиналы)',
    '/tournament (/t) delall — сброс настроек турнира (name/site/desc/logo/servers/pack/streams/channel) и удаление новостей турнира (только админы)',
    // Новые команды персональной статистики
    '/tournament (/t) stats_url [url] — показать/задать URL персональной статистики турнира (по умолчанию пусто)',
    '/tournament (/t) stats_enabled [true|false] — включить/выключить персональную статистику (по умолчанию: false)',
    '',
    '--------------------------',
    '1) Скилл-группы:',
    '/skillgroup (/sg) — список всех скилл-групп с игроками',
    '/skillgroup (/sg) N add name1,name2,... — добавить игроков (только админы)',
    '/skillgroup (/sg) N del — удалить всех игроков из SG N (только админы)',
    '/skillgroup (/sg) N del name1,name2,... — удалить указанных игроков (только админы)',
    '/skillgroup (/sg) delall — удалить все скилл-группы (только админы)',
    '',
    '2) Карты:',
    '/map (/m) — список карт',
    '/map (/m) add map1,map2,... — добавить карты (только админы)',
    '/map (/m) del map1,map2,... — удалить карты (только админы)',
    '/map (/m) delall — удалить все карты (только админы)',
    '',
    '3) Игровые группы:',
    '/groups (/g) — показать все группы (и Waiting при algo=2/3) — в один столбец',
    '/groups (/g) players — список всех игроков',
    '/groups (/g) N — показать группу N',
    '/groups (/g) move <from> <to> <player> — перенос игрока (только админы)',
    '/groups (/g) del N — удалить группу N (только админы)',
    '/groups (/g) delall — удалить все группы (только админы)',
    '/groups (/g) N addp name1,name2,... — добавить игроков (только админы)',
    '/groups (/g) N delp name1,name2,... — удалить игроков (только админы)',
    '/groups (/g) N result name1,name2,... — записать места (только админы)',
    '/groups (/g) N mapres — показать результаты игр на картах для группы N (публично)',
    '/groups (/g) N mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...] — записать/перезаписать результат карты (только админы; карта и игроки должны принадлежать группе)',
    '/groups (/g) N mapres delall — удалить все результаты карт для группы N (только админы)',
    '/groups (/g) points — показать очки группового этапа',
    '/groups (/g) points name1[p],name2[p],... — задать очки (только админы)',
    '/groups (/g) rating — показать рейтинг',
    '/groups (/g) rating name1,name2,... — задать рейтинг (перезапись, только админы)',
    '/groups (/g) algo <1|2|3> — выбрать алгоритм (только админы)',
    '/groups (/g) maxplayers <N> — максимальный размер группы (только админы)',
    '/groups (/g) minplayers <N> — min (algo=2) (только админы)',
    '/groups (/g) recplayers <N> — рекомендуемое (algo=2) (только админы)',
    '/groups (/g) maxcount <N> — количество групп (algo=3) (только админы)',
    '/groups (/g) make <C> — сформировать группы, по C карт (только админы)',
    '/groups (/g) N time [value] — показать/задать строку времени (МСК). Без value — только показать (публично), с value — задать (только админы)',
    '',
    'Скриншоты (game groups):',
    '/groups (/g) screenshots — показать скриншоты всех групп текущего запуска',
    '/groups (/g) screenshots delall — удалить ВСЕ скриншоты (только админы)',
    '/groups (/g) N screenshots — показать скриншоты группы N',
    '/groups (/g) N screenshots add — начать приём скриншотов (JPG/PNG/WEBP/GIF, завершение: /done, отмена: /cancel) (только админы)',
    '',
    'Новости (game groups):',
    '/groups (/g) news — показать новости текущего запуска (с ID)',
    '/groups (/g) news add <text> — добавить новость (только админы; при привязанном канале пост автоматически публикуется в канале)',
    '/groups (/g) news edit <id> <текст> — изменить новость по ID (только админы)',
    '/groups (/g) news delall — удалить все новости (только админы)',
    '',
    'Демо (game groups):',
    '/groups (/g) demos — показать все ссылки демо по всем группам',
    '/groups (/g) N demos — показать ссылки для группы N',
    '/groups (/g) N demos add <url1,url2,...> — задать список ссылок (перезапись, только админы)',
    '',
    '4) Финалы:',
    '/finals (/f) — список финальных групп (в столбик)',
    '/finals (/f) players — список игроков финалов',
    '/finals (/f) N — показать финальную группу N',
    '/finals (/f) move <from> <to> <player> — перенос игрока (только админы)',
    '/finals (/f) delall — удалить финальные группы (только админы)',
    '/finals (/f) N mapres — показать результаты игр на картах для финала N (публично)',
    '/finals (/f) N mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...] — записать/перезаписать результат карты (только админы; карта и игроки должны принадлежать финалу)',
    '/finals (/f) N mapres delall — удалить все результаты карт для финала N (только админы)',
    '/finals (/f) points — показать очки финалов',
    '/finals (/f) points name1[p],name2[p],... — задать очки финалов (только админы)',
    '/finals (/f) rating — показать рейтинг финалов',
    '/finals (/f) rating name1,name2,... — задать рейтинг финалов (перезапись, только админы)',
    '/finals (/f) news — показать новости текущего финала (с ID)',
    '/finals (/f) news add <text> — добавить новость (только админы; при привязанном канале пост автоматически публикуется в канале)',
    '/finals (/f) news edit <id> <текст> — изменить новость по ID (только админы)',
    '/finals (/f) news delall — удалить все новости финалов (только админы)',
    '/finals (/f) N time [value] — показать/задать строку времени (МСК). Без value — показать (публично), с value — задать (только админы)',
    '/finals (/f) N screenshots — показать скриншоты финала N',
    '/finals (/f) N screenshots add — приём скриншотов (только админы)',
    '/finals (/f) N screenshots delall — удалить скриншоты финала N (только админы)',
    '/finals (/f) screenshots — показать скриншоты всех финалов',
    '/finals (/f) screenshots delall — удалить ВСЕ скриншоты финалов',
    '/finals (/f) make <C> — сформировать финалы (algo: /finals algo) (только админы)',
    '/finals (/f) algo <1|2> — выбрать алгоритм (только админы)',
    '/finals (/f) maxplayers <N> — максимальный размер финала (только админы)',
    '/finals (/f) totalplayers <N|all|auto|0> — ограничение общего числа участников (algo=2 игнорирует) (только админы)',
    '',
    'Демо (finals):',
    '/finals (/f) demos — показать все ссылки демо по всем финалам',
    '/finals (/f) N demos — показать ссылки для финала N',
    '/finals (/f) N demos add <url1,url2,...> — задать список ссылок (перезапись, только админы)',
    '',
    '5) Суперфиналы:',
    '/superfinal (/s) — список суперфинальных групп',
    '/superfinal (/s) N — показать суперфинальную группу N',
    '/superfinal (/s) N time [value] — показать/задать строку времени (МСК). Без value — показать (публично), с value — задать (только админы)',
    '/superfinal (/s) players — список игроков суперфиналов',
    '/superfinal (/s) make <C> — сформировать суперфиналы (algo: /superfinal algo) (только админы)',
    '/superfinal (/s) algo <1|2> — выбрать алгоритм (только админы)',
    '/superfinal (/s) maxplayers <N> — максимальный размер суперфинала (только админы)',
    '/superfinal (/s) totalplayers <N|all|auto|0> — ограничение общего числа участников (algo=2 игнорирует) (только админы)',
    '/superfinal (/s) N mapres — показать результаты игр на картах для суперфинала N (публично)',
    '/superfinal (/s) N mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...] — записать/перезаписать результат карты (только админы; карта и игроки должны принадлежать суперфиналу)',
    '/superfinal (/s) N mapres delall — удалить все результаты карт для суперфинала N (только админы)',
    '/superfinal (/s) points — показать очки суперфинала',
    '/superfinal (/s) points name1[p],name2[p],... — задать очки суперфинала (только админы)',
    '/superfinal (/s) rating — показать рейтинг суперфинала',
    '/superfinal (/s) rating name1,name2,... — задать рейтинг суперфинала (только админы)',
    '/superfinal (/s) news — показать новости текущего суперфинала (с ID)',
    '/superfinal (/s) news add <text> — добавить новость (только админы; при привязанном канале пост автоматически публикуется в канале)',
    '/superfinal (/s) news edit <id> <текст> — изменить новость по ID (только админы)',
    '/superfinal (/s) news delall — удалить все новости суперфинала (только админы)',
    '/superfinal (/s) N screenshots — показать скриншоты суперфинала N',
    '/superfinal (/s) N screenshots add — приём скриншотов (только админы)',
    '/superfinal (/s) N screenshots delall — удалить скриншоты суперфинала N (только админы)',
    '/superfinal (/s) screenshots — показать скриншоты всех суперфиналов',
    '/superfinal (/s) screenshots delall — удалить ВСЕ скриншоты суперфиналов',
    '/superfinal (/s) demos — показать все ссылки демо по всем суперфиналам',
    '/superfinal (/s) N demos — показать ссылки для суперфинала N',
    '/superfinal (/s) N demos add <url1,url2,...> — задать список ссылок (перезапись, только админы)',
    '/superfinal (/s) delall — удалить все суперфинальные группы (только админы)',
    '',
    '6) Произвольные группы (custom):',
    '/custom (/c) — список всех custom-групп (публично)',
    '/custom (/c) N — показать custom-группу N (публично)',
    '/custom (/c) add <name> — создать новую custom-группу (только админы)',
    '/custom (/c) del <N> — удалить custom-группу N (только админы)',
    '/custom (/c) delall — удалить все custom-группы (только админы)',
    '/custom (/c) N addp name1,name2,... — добавить игроков (только админы)',
    '/custom (/c) N delp name1,name2,... — удалить игроков (только админы)',
    '/custom (/c) N maps map1,map2,... — задать список карт (только админы)',
    '/custom (/c) N mix — перемешать порядок карт (только админы)',
    '/custom (/c) N points — показать текущие очки (публично)',
    '/custom (/c) N points name1[p],name2[p],... — задать очки и отсортировать игроков по возрастанию очков (только админы)',
    '/custom (/c) N time [value] — показать/задать время для custom-группы',
    '/custom (/c) N demos — показать ссылки демо; /custom N demos add <urls> — задать',
    '/custom (/c) N screenshots — показать скриншоты; /custom N screenshots add | delall',
    '',
    '7) Ачивки:',
    '/achievements (/ac) — показать все ачивки (публично)',
    '/achievements (/ac) N — показать ачивку N (публично)',
    '/achievements (/ac) add <name> — добавить ачивку (админы/роль Achievements; далее загрузка картинки, затем описание; тип по умолчанию: achievement)',
    '/achievements (/ac) N type <achievement|perc> — установить тип ачивки (админы/роль Achievements)',
    '/achievements (/ac) N edit <name|logo|desc|all> — редактировать ачивку (админы/роль Achievements)',
    '/achievements (/ac) N addp <player> — назначить владельца ачивки N (админы/роль Achievements)',
    '/achievements (/ac) N del — удалить ачивку N с перенумерацией (админы/роль Achievements)',
    '/achievements (/ac) delall — удалить все ачивки (админы/роль Achievements)',
    '',
    '8) Отзывы',
    '/feedback (/fb) add <текст> — оставить отзыв (можно в несколько сообщений, завершить /done)',
    '/feedback (/fb) edit <текст> — отредактировать ранее оставленный отзыв (если был)',
    '/feedback (/fb) del — удалить свой отзыв (если был)',
    '',
    '--------------------------',
    '',
    '9) Регистрация пользователей и команд',
    'Пользователи:',
    '/users (/u) — ваш профиль игрока (ник + описание)',
    '/users add - регистрация игрока (глобальная для всех турниров)',
    '/users edit - редактирование игрока ',
    '/users all - просмотр всех зарегистрированных игроков (для админа)',
    '',
    'Игровые команды:',
    '/teams (/tm) — список игровых команд, в которых вы состоите',
    '/teams add — создать новую игровую команду',
    '/teams edit — отредактировать одну из ваших команд',
    // Для админов:
    '/teams all — список всех команд (админы)',
    '/team editadm <teamId> — админ-редактирование команды',
    '',
    '--------------------------',
    '10) Регистрация на турнир',
    '/signup (/sup) — подать / отозвать заявку на участие в турнире и посмотреть статус',

    '/registration (/reg) — сводка по настройкам регистрации и списку заявок',
    '----------',
    'Административные команды регистрации на турнир:',
    '/registration (/reg) enabled true|false — открыть/закрыть регистрацию',
    '/reg maxplayers <N> — установить лимит игроков регистрации',
    '/reg deadline [YYYY-MM-DD HH:mm] — показать/установить дедлайн регистрации',
    '/reg addp <nick> — добавить игрока в список регистрации',
    '/reg addt <team> — добавить команду в список регистрации (TDM)',
    '/reg delp <id>, /reg delt <id> — удалить заявку',
    '/reg players [<id> status true|false] — список/изменение статуса заявок игроков',
    '/reg teams [<id> status true|false] — список/изменение статуса заявок команд',
    '/t type [FFA|1v1|TDM] — посмотреть/установить тип турнира',
    '----------',
    '',
    'Управление приёмом изображений:',
    '/done — завершить текущую сессию приёма изображений',
    '/cancel — отменить текущую сессию (временные файлы ачивок удаляются)',
    '',
    'Диагностика:',
    '/whoami — показать ваш ID и username',
    '/chatid — показать ID текущего чата и ID темы (если есть)',
    '/setid — установить/посмотреть/сбросить целевой chatId (переключение контекста данных). Примеры: /setid -1001234567890, /setid clear',
    '',
    'Сброс:',
    '/delall — удалить все SG, игровые группы, карты, финалы, суперфиналы, РЕЙТИНГИ, ОЧКИ, ЛОГОТИП, СКРИНШОТЫ и НОВОСТИ (только текущего чата) (только админы)',
    '',
    'Примечания:',
    '- Поле time хранится как текст (например: 2025-10-20 23:00) и трактуется как московское время.',
    '- Публично доступны команды просмотра; любые операции добавления/удаления/редактирования — только для админов, или для обладателей соответствующих ролей (Achievements — только для /achievements).',
    '',
    'Разработка / поддержка: ly (@AlexCpto), @QuakeJourney, 2025.',
  ].join('\n');
}


bot.command(['help', 'h'], async ctx => {
  await replyChunked(ctx, await helpText());
});

// INFO
bot.command(['info', 'i'], async ctx => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const [
    settings, sgs, maps, ggs, fgs, sfgs, waiting, rating, finalRating, groupPtsArr, finalPtsArr, regSettings,
  ] = await Promise.all([
    getChatSettings(chatId),
    listSkillGroups(chatId),
    listMaps(chatId),
    listGameGroups(chatId),
    listFinalGroups(chatId),
    listSuperFinalGroups(chatId),
    getWaitingPlayers(chatId),
    getRating(chatId),
    getFinalRating(chatId),
    getGroupPoints(chatId),
    getFinalPoints(chatId),
    getRegistrationSettings(chatId),
  ]);

  const gpMap = groupPointsToMap(groupPtsArr);
  const fpMap = finalPointsToMap(finalPtsArr);

  const lines = [];

  // Tournament block
  lines.push('Tournament:');
  lines.push(`- name: ${settings.tournamentName || '(not set)'}`);
  lines.push(`- site: ${settings.tournamentSite || '(not set)'}`);
  lines.push(`- desc: ${settings.tournamentDesc || '(not set)'}`);
  lines.push(`- type: ${regSettings.tournamentType || '(not set)'}`);
  lines.push('');

  // Settings block
  lines.push('Settings:');
  lines.push(`- groups algo: ${settings.groupsAlgo}`);
  lines.push(`- maxPlayers: ${settings.maxPlayers}`);
  lines.push(`- minPlayers (algo2): ${settings.minPlayers2}`);
  lines.push(`- recPlayers (algo2): ${settings.recPlayers2}`);
  lines.push(`- maxCount (algo3): ${settings.maxCount3 ?? '(not set)'}`);
  lines.push(`- finals algo: ${settings.finalsAlgo}`);
  lines.push(`- finals maxPlayers: ${settings.finalMaxPlayers}`);
  lines.push(`- finals totalPlayers: ${settings.finalTotalPlayers ?? '(all available)'}${settings.finalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
  lines.push(`- superfinal algo: ${settings.superfinalsAlgo}`);
  lines.push(`- superfinal maxPlayers: ${settings.superfinalMaxPlayers}`);
  lines.push(`- superfinal totalPlayers: ${settings.superfinalTotalPlayers ?? '(all available)'}${settings.superfinalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
  lines.push('');

  // Skill-groups
  lines.push('Skill-groups:');
  lines.push(formatSkillGroupsList(sgs));
  lines.push('');

  // Maps
  lines.push(formatMapsList(maps));
  lines.push('');

  // Ratings
  lines.push(formatRatingList(rating));
  lines.push('');
  lines.push(formatFinalRatingList(finalRating));
  lines.push('');

  // Game groups (2 columns)
  lines.push(formatGameGroupsList(ggs, waiting, gpMap));
  lines.push('');

  // Final groups (2 columns, с очками)
  if (fgs.length) {
    lines.push(formatFinalGroupsList(fgs, fpMap));
    lines.push('');
  }

  // Superfinal groups (2 columns)
  if (sfgs.length) {
    lines.push(formatSuperFinalGroupsList(sfgs));
  }

  await replyPre(ctx, lines.join('\n'));
});


// ADMIN
// ПОЛНАЯ ЗАМЕНА adminHandler(ctx) — chatId = getEffectiveChatId(ctx)
async function adminHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в группе.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const [sub, ...rest] = args.split(' ').filter(Boolean);

  if (!sub) {
    const admins = await getChatAdmins(chatId);
    await ctx.reply(formatAdminsList(admins));
    return;
  }

  // Блок ролей
  // Блок ролей
  if (sub.toLowerCase() === 'role') {
    if (!(await requireAdminGuard(ctx))) return;

    const action = (rest[0] || '').toLowerCase();

    // /admin role — список ролей по чату
    if (!action) {
      const docs = await colRoles.find({ chatId }).toArray();
      const table = formatRolesTable(docs);
      await replyChunked(ctx, table);
      return;
    }

    // /admin role add <rolename> @username|user_id
    if (action === 'add') {
      const roleName = (rest[1] || '').trim();
      if (!roleName) {
        await ctx.reply(
          'Укажите роль. Пример:\n' +
          '/admin role add Achievements @username\n' +
          '/admin role add News @username'
        );
        return;
      }
      if (!isSupportedRoleName(roleName)) {
        await ctx.reply('Неизвестная роль. Доступные роли: Achievements, News');
        return;
      }

      const tail = rest.slice(2).join(' ');
      const ref = extractUserRef(ctx, tail);
      if (!ref) {
        await ctx.reply('Укажите пользователя: /admin role add <rolename> @username ИЛИ числовой id, ИЛИ используйте как ответ на сообщение.');
        return;
      }

      // Попробуем обогатить userId из user_index по username, если id не передан
      let userId = ref.userId || null;
      let username = ref.username || null;
      if (!userId && username) {
        const doc = await colUserIndex.findOne({ username });
        if (doc) userId = Number(doc.userId);
      }
      if (!userId) {
        await ctx.reply('Не удалось определить user_id. Сначала отправьте команду как ответ на сообщение от этого пользователя либо используйте числовой id.');
        return;
      }

      const normRole = normalizeRoleName(roleName);
      await addUserRole(chatId, { userId, username }, normRole);
      const label = username ? `@${username}` : `#${userId}`;
      await ctx.reply(`Роль ${normRole} назначена пользователю ${label}.`);
      return;
    }

    // /admin role del <rolename> @username|user_id
    if (action === 'del') {
      const roleName = (rest[1] || '').trim();
      if (!roleName) {
        await ctx.reply(
          'Укажите роль. Пример:\n' +
          '/admin role del Achievements @username\n' +
          '/admin role del News @username'
        );
        return;
      }
      if (!isSupportedRoleName(roleName)) {
        await ctx.reply('Неизвестная роль. Доступные роли: Achievements, News');
        return;
      }

      const tail = rest.slice(2).join(' ');
      const ref = extractUserRef(ctx, tail);
      if (!ref) {
        await ctx.reply('Укажите пользователя: /admin role del <rolename> @username ИЛИ числовой id, ИЛИ используйте как ответ на сообщение.');
        return;
      }

      let userId = ref.userId || null;
      let username = ref.username || null;
      if (!userId && username) {
        const doc = await colUserIndex.findOne({ username });
        if (doc) userId = Number(doc.userId);
      }
      if (!userId) {
        await ctx.reply('Не удалось определить user_id. Сначала отправьте команду как ответ на сообщение от этого пользователя либо используйте числовой id.');
        return;
      }

      const normRole = normalizeRoleName(roleName);
      await removeUserRole(chatId, { userId, username }, normRole);
      const label = username ? `@${username}` : `#${userId}`;
      await ctx.reply(`Роль ${normRole} удалена у пользователя ${label}.`);
      return;
    }

    await ctx.reply('Неизвестная опция. Используйте: /admin role, /admin role add, /admin role del');
    return;
  }

  // Остальные админ-операции (добавление/удаление админов)
  if (!(await requireAdminGuard(ctx))) return;

  if (sub.toLowerCase() === 'add') {
    const tail = rest.join(' ');
    const ref = extractUserRef(ctx, tail);
    if (!ref) {
      await ctx.reply('Укажите пользователя: /admin add @username ИЛИ числовой id, ИЛИ используйте как ответ на сообщение.');
      return;
    }
    // Try to enrich username by userIndex if missing
    if (!ref.userId && ref.username) {
      const doc = await colUserIndex.findOne({ username: ref.username });
      if (doc) ref.userId = Number(doc.userId);
    }
    const admins = await getChatAdmins(chatId);
    // Prevent duplicate
    if (admins.some(a =>
      (ref.userId && a.userId === ref.userId) ||
      (ref.username && a.username && norm(a.username) === norm(ref.username))
    )) {
      await ctx.reply('Этот админ уже добавлен.');
      return;
    }
    admins.push({ userId: ref.userId || null, username: ref.username || null });
    await setChatAdmins(chatId, admins);
    const label = ref.username ? `@${ref.username}` : `#${ref.userId}`;
    await ctx.reply(`admin ${label} is added.`);
    return;
  }

  if (sub.toLowerCase() === 'del') {
    const tail = rest.join(' ');
    const ref = extractUserRef(ctx, tail);
    if (!ref) {
      await ctx.reply('Укажите пользователя: /admin del @username или user_id.');
      return;
    }
    const admins = await getChatAdmins(chatId);
    const next = admins.filter(a => {
      if (ref.userId && a.userId) return Number(a.userId) !== Number(ref.userId);
      if (ref.username && a.username) return norm(a.username) !== norm(ref.username);
      // if we can't compare, keep
      return true;
    });
    if (next.length === admins.length) {
      await ctx.reply('Такого админа не найдено.');
      return;
    }
    await setChatAdmins(chatId, next);
    const label = ref.username ? `@${ref.username}` : `#${ref.userId}`;
    await ctx.reply(`admin ${label} is removed.`);
    return;
  }

  if (sub.toLowerCase() === 'delall') {
    await setChatAdmins(chatId, []);
    await ctx.reply('All admins removed (owners are still owners).');
    return;
  }

  await ctx.reply('Неизвестная опция /admin. Используйте: /admin, /admin add, /admin del, /admin delall, /admin role, /admin role add, /admin role del');
}


bot.command(['admin', 'a'], adminHandler);

// SKILLGROUPS
// ПОЛНАЯ ЗАМЕНА skillGroupHandler(ctx) — chatId = getEffectiveChatId(ctx)
async function skillGroupHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в группе.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // No args -> list all
  if (!tokens.length) {
    const sgs = await listSkillGroups(chatId);
    await replyChunked(ctx, formatSkillGroupsList(sgs));
    return;
  }

  if (!(await requireAdminGuard(ctx))) return;

  // delall | dellall
  if (tokens[0].toLowerCase() === 'delall' || tokens[0].toLowerCase() === 'dellall') {
    await delAllSkillGroups(chatId);
    await ctx.reply('All skill-groups removed.');
    return;
  }

  const N = Number(tokens[0]);
  if (!Number.isInteger(N) || N <= 0) {
    await ctx.reply('Укажите номер скилл-группы N (положительное целое). Пример: /sg 1 add name1,name2');
    return;
  }

  const action = (tokens[1] || '').toLowerCase();
  const tail = tokens.slice(2).join(' ');

  if (action === 'add') {
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) {
      await ctx.reply('Укажите игроков через запятую. Пример: /sg 4 add David,pp,aid');
      return;
    }

    // Validate duplicates across all SG
    const existingSGs = await listSkillGroups(chatId);
    const taken = new Set();
    for (const sg of existingSGs) {
      for (const p of sg.players || []) taken.add(p.nameNorm);
    }

    const sgDoc = (await getSkillGroup(chatId, N)) || { players: [] };
    const current = new Set((sgDoc.players || []).map(p => p.nameNorm));

    const added = [];
    const skipped = [];
    for (const player of list) {
      if (taken.has(player.nameNorm) || current.has(player.nameNorm)) {
        skipped.push(player.nameOrig);
      } else {
        sgDoc.players.push(player);
        added.push(player.nameOrig);
        taken.add(player.nameNorm);
        current.add(player.nameNorm);
      }
    }
    await upsertSkillGroup(chatId, N, sgDoc.players);

    let msg = '';
    if (added.length) msg += `Added to SG ${N}: ${added.join(', ')}\n`;
    if (skipped.length) msg += `Skipped (already present in some SG): ${skipped.join(', ')}`;
    if (!msg) msg = 'Нечего добавлять.';
    await replyChunked(ctx, msg.trim());
    return;
  }

  if (action === 'del') {
    if (!tail) {
      // delete all in SG N
      await delSkillGroup(chatId, N);
      await ctx.reply(`Skill-group ${N} cleared.`);
      return;
    }
    // partial delete
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) {
      await ctx.reply('Укажите игроков для удаления через запятую.');
      return;
    }
    const sgDoc = await getSkillGroup(chatId, N);
    if (!sgDoc) {
      await ctx.reply(`Skill-group ${N} не существует.`);
      return;
    }
    const setDel = new Set(list.map(p => p.nameNorm));
    const presentSet = new Set((sgDoc.players || []).map(p => p.nameNorm));
    const actuallyDel = [];
    const notFound = [];
    for (const p of list) {
      if (presentSet.has(p.nameNorm)) actuallyDel.push(p.nameNorm);
      else notFound.push(p.nameOrig);
    }
    const nextPlayers = (sgDoc.players || []).filter(p => !setDel.has(p.nameNorm));
    await upsertSkillGroup(chatId, N, nextPlayers);
    let msg = '';
    if (actuallyDel.length) msg += `Deleted from SG ${N}: ${list.filter(p => actuallyDel.includes(p.nameNorm)).map(p => p.nameOrig).join(', ')}\n`;
    if (notFound.length) msg += `Not found in SG ${N}: ${notFound.join(', ')}`;
    await replyChunked(ctx, msg.trim());
    return;
  }

  if (!action) {
    // list SG N
    const sgDoc = await getSkillGroup(chatId, N);
    if (!sgDoc) {
      await ctx.reply(`Skill-group ${N} players: (none)`);
      return;
    }
    const names = (sgDoc.players || []).map(p => p.nameOrig).join(', ') || '(empty)';
    await ctx.reply(`Skill-group ${N} players: ${names}`);
    return;
  }

  await ctx.reply('Неизвестная опция. Используйте: add | del | (пусто для списка)');
}


bot.command(['skillgroup', 'sg'], skillGroupHandler);

// MAPS
// ПОЛНАЯ ЗАМЕНА mapsHandler(ctx) — chatId = getEffectiveChatId(ctx)
async function mapsHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в группе.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const [action, ...rest] = args.split(' ').filter(Boolean);

  if (!action) {
    const maps = await listMaps(chatId);
    await ctx.reply(formatMapsList(maps));
    return;
  }

  if (!(await requireAdminGuard(ctx))) return;

  if (action.toLowerCase() === 'add') {
    const list = dedupByNorm(cleanListParam(rest.join(' ')));
    if (!list.length) {
      await ctx.reply('Укажите карты через запятую. Пример: /m add q2dm1,q2dm2');
      return;
    }
    const { added, skippedExists } = await addMaps(chatId, list);
    let msg = '';
    if (added.length) msg += `Added maps: ${added.join(', ')}\n`;
    if (skippedExists.length) msg += `Skipped (already exist): ${skippedExists.join(', ')}`;
    await replyChunked(ctx, msg.trim());
    return;
  }

  if (action.toLowerCase() === 'del') {
    const list = dedupByNorm(cleanListParam(rest.join(' ')));
    if (!list.length) {
      await ctx.reply('Укажите карты через запятую. Пример: /m del q2dm1,q2dm2');
      return;
    }
    const { removed, notFound } = await removeMaps(chatId, list);
    let msg = '';
    if (removed.length) msg += `Removed maps: ${removed.join(', ')}\n`;
    if (notFound.length) msg += `Not found: ${notFound.join(', ')}`;
    await replyChunked(ctx, msg.trim());
    return;
  }

  if (action.toLowerCase() === 'delall') {
    await delAllMaps(chatId);
    await ctx.reply('All maps removed.');
    return;
  }

  await ctx.reply('Неизвестная опция. Используйте: add | del | delall | (пусто для списка)');
}


bot.command(['map', 'm'], mapsHandler);

// REPLACE the entire tournamentHandler(ctx) function with the version below
// ПОЛНАЯ ЗАМЕНА tournamentHandler(ctx) — добавлена ветка /t newschannel и авто-постинг новостей в канал
// ПОЛНАЯ ЗАМЕНА tournamentHandler(ctx) — /t newschannel makenews теперь публикует ВСЕ новости (tournament/group/final/superfinal)
// и news add больше не дублирует вручную (это делает addNews)
// REPLACE the entire tournamentHandler(ctx) function with the version below
// ПОЛНАЯ ЗАМЕНА tournamentHandler(ctx) — добавлены ветки /t stats_url и /t stats_enabled
// и показ их значений в /t info

async function tournamentHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  const token0 = tokens[0]?.toLowerCase();

  // /t type — показать или задать тип турнира (FFA / 1v1 / TDM)
  if (token0 === 'type') {
    const reg = await getRegistrationSettings(chatId);

    // /t type  — только показать (публично)
    if (tokens.length === 1) {
      await ctx.reply(
        `Текущий тип турнира: ${reg.tournamentType || '(not set)'}\n\n` +
        'Варианты: FFA, 1v1, TDM.\n' +
        'Админы могут задать тип: /t type FFA'
      );
      return;
    }

    // /t type <value> — задать (только админы)
    if (!(await requireAdminGuard(ctx))) return;

    const rawType = tokens[1].toUpperCase();
    const allowed = ['FFA', '1V1', 'TDM'];
    if (!allowed.includes(rawType)) {
      await ctx.reply('Неверный тип турнира. Допустимые значения: FFA, 1v1, TDM.');
      return;
    }

    const normalized = rawType === '1V1' ? '1v1' : rawType;

    // --- НОВОЕ: блокировка смены типа, если уже есть конфликтующие заявки ---

    const currentType = reg.tournamentType || null;
    if (currentType && normalized !== currentType) {
      const currentIsTeam = currentType === 'TDM';   // true если текущий тип = команды
      const newIsTeam = normalized === 'TDM';        // true если новый тип = команды

      // Разрешаем смену ТОЛЬКО внутри одной "семьи":
      // - FFA ↔ 1v1 (оба игроки)
      // - TDM ↔ TDM (само на себя)
      // НЕЛЬЗЯ менять "игроки" <-> "команды", если есть заявки.
      if (currentIsTeam !== newIsTeam) {
        // Если переходим в TDM — конфликтуют заявки игроков (kind: 'player')
        // Если переходим из TDM в FFA/1v1 — конфликтуют заявки команд (kind: 'team')
        const conflictKind = newIsTeam ? 'player' : 'team';

        const conflict = await colSignups.findOne({ chatId, kind: conflictKind });
        if (conflict) {
          await ctx.reply(
            'Нельзя сменить тип турнира между режимами "игроки" (FFA/1v1) и "команды" (TDM),\n' +
            'пока существуют заявки на участие для этого турнира.\n' +
            'Сначала очистите заявки через /reg delp / /reg delt (или очистите коллекцию signups).'
          );
          return;
        }
      }
    }

    await updateRegistrationSettings(chatId, { tournamentType: normalized });
    await ctx.reply(`Тип турнира установлен: ${normalized}`);
    return;
  }


  // /tournament info — агрегированная информация
  if (!tokens.length || tokens[0].toLowerCase() === 'info') {
    const [
      settings, sgs, ggs, fgs, sfgs, waiting, groupPtsArr, finalPtsArr,
      cgs, cptsDocs,
      achs, regSettings,
    ] = await Promise.all([
      getChatSettings(chatId),
      listSkillGroups(chatId),
      listGameGroups(chatId),
      listFinalGroups(chatId),
      listSuperFinalGroups(chatId),
      getWaitingPlayers(chatId),
      getGroupPoints(chatId),
      getFinalPoints(chatId),
      listCustomGroups(chatId),
      colCustomPoints.find({ chatId }).toArray(),
      listAchievements(chatId),
      getRegistrationSettings(chatId),
    ]);

    const gpMap = groupPointsToMap(groupPtsArr);
    const fpMap = finalPointsToMap(finalPtsArr);
    const customPtsByGroup = new Map();
    for (const d of cptsDocs) customPtsByGroup.set(d.groupId, customPointsToMap(d.points || []));

    const lines = [];
    lines.push('Tournament:');
    lines.push(`- name: ${settings.tournamentName || '(not set)'}`);
    lines.push(`- site: ${settings.tournamentSite || '(not set)'}`);
    lines.push(`- desc: ${settings.tournamentDesc || '(not set)'}`);
    lines.push(`- type: ${regSettings.tournamentType || '(not set)'}`);
    lines.push(`- news channel: ${settings.tournamentNewsChannel || '(not set)'}`);
    // Новые строки персональной статистики
    lines.push(`- player stats enabled: ${settings.tournamentStatsEnabled ? 'true' : 'false'}`);
    lines.push(`- player stats url: ${settings.tournamentStatsUrl || '(not set)'}\n`);

    // Новые поля турнира
    lines.push(`- servers:`);
    if (settings.tournamentServers?.length) {
      for (const s of settings.tournamentServers) lines.push(`  * ${s}`);
    } else {
      lines.push('  (none)');
    }
    lines.push(`- pack: ${settings.tournamentPack || '(not set)'}`);
    lines.push(`- streams:`);
    if (settings.tournamentStreams?.length) {
      for (const s of settings.tournamentStreams) lines.push(`  * ${s}`);
    } else {
      lines.push('  (none)');
    }

    lines.push('');
    lines.push('Settings:');
    lines.push(`- groups algo: ${settings.groupsAlgo}`);
    lines.push(`- maxPlayers: ${settings.maxPlayers}`);
    lines.push(`- minPlayers (algo2): ${settings.minPlayers2}`);
    lines.push(`- recPlayers (algo2): ${settings.recPlayers2}`);
    lines.push(`- maxCount (algo3): ${settings.maxCount3 ?? '(not set)'}`);
    lines.push(`- finals algo: ${settings.finalsAlgo}`);
    lines.push(`- finals maxPlayers: ${settings.finalMaxPlayers}`);
    lines.push(`- finals totalPlayers: ${settings.finalTotalPlayers ?? '(all available)'}${settings.finalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
    lines.push(`- superfinal algo: ${settings.superfinalsAlgo}`);
    lines.push(`- superfinal maxPlayers: ${settings.superfinalMaxPlayers}`);
    lines.push(`- superfinal totalPlayers: ${settings.superfinalTotalPlayers ?? '(all available)'}${settings.superfinalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
    lines.push('');
    lines.push('Skill-groups:');
    lines.push(formatSkillGroupsList(sgs));
    lines.push('');

    // Achievements
    lines.push('Achievements:');
    if (!achs.length) {
      lines.push('(none)');
    } else {
      for (const a of achs) {
        const typeStr = a.type ? String(a.type) : 'achievement';
        const owner = a.player?.nameOrig ? ` — ${a.player.nameOrig}` : '';
        lines.push(`${a.idx}) [${typeStr}] ${a.name}${owner}`);
      }
    }
    lines.push('');

    // Custom groups
    lines.push('Custom groups:');
    lines.push(formatCustomGroupsList(cgs, customPtsByGroup));
    lines.push('');

    lines.push(formatGameGroupsList(ggs, waiting, gpMap));
    lines.push('');
    lines.push(formatFinalGroupsList(fgs, fpMap));
    lines.push('');
    lines.push(formatSuperFinalGroupsList(sfgs));

    await replyPre(ctx, lines.join('\n'));
    return;
  }

  const sub = tokens[0].toLowerCase();
  const tail = tokens.slice(1).join(' ').trim();

  function stripQuotes(s = '') {
    return s.replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1').trim();
  }

  function parseBoolStrict(s = '') {
    const v = String(s).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on', 'enable', 'enabled'].includes(v)) return { ok: true, value: true };
    if (['0', 'false', 'no', 'off', 'disable', 'disabled'].includes(v)) return { ok: true, value: false };
    return { ok: false };
  }

  if (sub === 'name') {
    if (!tail) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament name: ${s.tournamentName || '(not set)'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const value = stripQuotes(tail);
    await setChatSettings(chatId, { tournamentName: value || null });
    await ctx.reply(`Tournament name is set: ${value || '(cleared)'}`);
    return;
  }

  if (sub === 'site') {
    if (!tail) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament site: ${s.tournamentSite || '(not set)'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const value = stripQuotes(tail);
    await setChatSettings(chatId, { tournamentSite: value || null });
    await ctx.reply(`Tournament site is set: ${value || '(cleared)'}`);
    return;
  }

  if (sub === 'desc') {
    if (!tail) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament desc: ${s.tournamentDesc || '(not set)'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const value = stripQuotes(tail);
    await setChatSettings(chatId, { tournamentDesc: value || null });
    await ctx.reply(`Tournament desc is set: ${value || '(cleared)'}`);
    return;
  }

  if (sub === 'logo') {
    const op = (tokens[1] || '').toLowerCase();
    if (op === 'add') {
      if (!(await requireAdminGuard(ctx))) return;

      const key = ssKey(chatId, ctx.from.id);
      screenshotSessions.set(key, {
        chatId,
        userId: ctx.from.id,
        mode: 'tlogo',
        groupId: 0,
        runId: 'tlogo',
        startedAt: Date.now(),
        expiresAt: Date.now() + 10 * 60 * 1000,
        count: 0,
      });

      await ctx.reply(
        'Пришлите изображение для логотипа турнира (JPG/PNG/WEBP/GIF). ' +
        'Можно отправить одно или несколько — сохранится последний как текущий логотип. ' +
        'Завершить: /done. Отмена: /cancel.'
      );
      return;
    }
    await showTournamentLogo(ctx, chatId);
    return;
  }

  // Управление новостным каналом турнира
  if (sub === 'newschannel') {
    const op = (tokens[1] || '').toLowerCase();

    // /t newschannel — показать текущие настройки (публично)
    if (!op) {
      const s = await getChatSettings(chatId);
      await ctx.reply(
        `Tournament news channel: ${s.tournamentNewsChannel || '(not set)'}\n` +
        `Задать: /t newschannel add @tgchannel\n` +
        `Опубликовать все прошлые новости: /t newschannel makenews`
      );
      return;
    }

    // /t newschannel add @tgchannel — только админы
    if (op === 'add') {
      if (!(await requireAdminGuard(ctx))) return;
      const ref = tokens[2] || '';
      if (!ref) {
        await ctx.reply('Использование: /t newschannel add @tgchannel');
        return;
      }
      const ok = /^@[\w\d_]{5,}$/.test(ref) || /^-?\d+$/.test(ref);
      if (!ok) {
        await ctx.reply('Укажите канал в виде @channel_name или числового ID (например, -1001234567890).');
        return;
      }
      await setChatSettings(chatId, { tournamentNewsChannel: ref });
      await ctx.reply(`Привязан новостной канал: ${ref}\nУбедитесь, что бот добавлен администратором в этот канал.`);
      return;
    }

    // /t newschannel makenews — только админы (постит ВСЕ новости всех стадий)
    if (op === 'makenews') {
      if (!(await requireAdminGuard(ctx))) return;
      const target = await getNewsChannelTarget(chatId);
      if (!target) {
        await ctx.reply('Канал не задан. Укажите командой: /t newschannel add @tgchannel');
        return;
      }
      const res = await postAllNewsToChannel(chatId);
      if (res.error) {
        await ctx.reply(`Ошибка: ${res.error}`);
        return;
      }
      await ctx.reply(`Опубликовано новостей: ${res.posted || 0}.`);
      return;
    }

    await ctx.reply('Неизвестная опция. Используйте: /t newschannel, /t newschannel add @tgchannel, /t newschannel makenews');
    return;
  }

  // Новые команды: персональная статистика турнира
  if (sub === 'stats_url') {
    if (!tail) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament player stats URL: ${s.tournamentStatsUrl || '(not set)'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const value = stripQuotes(tail);
    // Поддержим очистку по ключевым словам
    const cleared = ['clear', 'none', 'null', 'off'].includes(value.toLowerCase());
    await setChatSettings(chatId, { tournamentStatsUrl: cleared ? null : value });
    await ctx.reply(`Tournament player stats URL is set: ${cleared ? '(cleared)' : value}`);
    return;
  }

  if (sub === 'stats_enabled') {
    const valRaw = tail.trim();
    if (!valRaw) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament player stats enabled: ${s.tournamentStatsEnabled ? 'true' : 'false'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const parsed = parseBoolStrict(valRaw);
    if (!parsed.ok) {
      await ctx.reply('Использование: /t stats_enabled <true|false>');
      return;
    }
    await setChatSettings(chatId, { tournamentStatsEnabled: parsed.value });
    await ctx.reply(`Tournament player stats enabled set to: ${parsed.value ? 'true' : 'false'}`);
    return;
  }

  if (sub === 'news') {
    const op = (tokens[1] || '').toLowerCase();

    if (op === 'add') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const text = tokens.slice(2).join(' ').trim();
      if (!text) { await ctx.reply('Использование: /tournament news add <текст новости>'); return; }
      await addNews(chatId, 'tournament', text, null, { id: ctx.from.id, username: ctx.from.username });
      await ctx.reply('Новость турнира добавлена.');
      return;
    }

    // --- ПОЛНАЯ ЗАМЕНА ветки edit ---
    if (op === 'edit') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;

      const idStr = tokens[1];
      const newText = tokens.slice(2).join(' ').trim();
      if (!idStr || !newText) {
        await ctx.reply('Использование: /news edit <id> <новый текст>');
        return;
      }

      const chatId = getEffectiveChatId(ctx);
      let _id;
      try { _id = new ObjectId(idStr); } catch (_) {
        await ctx.reply('Некорректный ID новости.');
        return;
      }

      const doc = await colNews.findOne({ _id, chatId });
      if (!doc) {
        await ctx.reply('Новость с таким ID не найдена в текущем чате.');
        return;
      }

      await colNews.updateOne(
        { _id, chatId },
        { $set: { text: newText, updatedAt: new Date() } }
      );

      // Если у новости есть картинка — предложим её заменить
      if (doc.news_img_file_name) {
        const sKey = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(sKey, {
          chatId,
          userId: ctx.from.id,
          mode: 'news_edit_img',
          newsId: String(_id),
          oldImgFileName: doc.news_img_file_name,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
        });

        await ctx.reply(
          'Текст новости обновлён.\n' +
          'Хотите заменить картинку? Пришлите новую (JPG/PNG/WEBP/GIF) одним сообщением.\n' +
          'Чтобы оставить старую — отправьте /skip. Отмена — /cancel.'
        );
      } else {
        await ctx.reply('Текст новости обновлён. У этой новости нет картинки.');
      }
      return;
    }
    // --- конец ветки edit ---

    if (op === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const n = await delAllNews(chatId, 'tournament');
      await ctx.reply(`Удалено новостей турнира: ${n}.`);
      return;
    }

    const news = await listNews(chatId, 'tournament', null);
    await replyChunked(ctx, formatNewsList(news));
    return;
  }

  // --- NEW: упрощённый news2 ---
  if (sub === 'news2') {
    const op = (tokens[1] || '').toLowerCase();
    if (op === 'add') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const userId = ctx.from.id;

      const keyS = ssKey(chatId, userId);
      screenshotSessions.set(keyS, {
        chatId,
        userId,
        mode: 'news2_one',       // <--- упрощённый режим «одно сообщение: фото+подпись»
        groupId: 0,
        runId: 'news2_one',
        startedAt: Date.now(),
        expiresAt: Date.now() + 10 * 60 * 1000,
        count: 0,
      });

      await ctx.reply(
        'Отправьте ОДНО сообщение: картинка (фото или документ-изображение) + подпись (текст).\n' +
        'Форматы: JPG / PNG / WEBP / GIF.\n' +
        'Новость сохранится сразу после получения. Отмена — /cancel.'
      );
      return;
    }
    await ctx.reply('Использование: /t news2 add');
    return;
  }


  // servers / pack / streams / demos — без изменений
  if (sub === 'servers') {
    const op = (tokens[1] || '').toLowerCase();
    if (op === 'add') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tokens.slice(2).join(' '))).map(x => x.nameOrig);
      await setChatSettings(chatId, { tournamentServers: list });
      await replyChunked(ctx, list.length ? `Tournament servers are set:\n${list.join('\n')}` : 'Tournament servers cleared.');
      return;
    }
    const s = await getChatSettings(chatId);
    if (!s.tournamentServers?.length) { await ctx.reply('Tournament servers: (none)'); return; }
    await replyChunked(ctx, `Tournament servers:\n${s.tournamentServers.join('\n')}`);
    return;
  }

  if (sub === 'pack') {
    const url = tail.trim();
    if (!url) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Tournament pack: ${s.tournamentPack || '(not set)'}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    await setChatSettings(chatId, { tournamentPack: url });
    await ctx.reply(`Tournament pack is set: ${url}`);
    return;
  }

  if (sub === 'streams') {
    if (tail) {
      if (!(await requireAdminGuard(ctx))) return;
      const list = cleanListParam(tail);
      await setChatSettings(chatId, { tournamentStreams: list });
      await replyChunked(ctx, list.length ? `Tournament streams are set:\n${list.join('\n')}` : 'Tournament streams cleared.');
      return;
    }
    const s = await getChatSettings(chatId);
    if (!s.tournamentStreams?.length) { await ctx.reply('Tournament streams: (none)'); return; }
    await replyChunked(ctx, `Tournament streams:\n${s.tournamentStreams.join('\n')}`);
    return;
  }

  if (sub === 'demos') {
    const [ggs, fgs, sfgs] = await Promise.all([
      listGameGroups(chatId),
      listFinalGroups(chatId),
      listSuperFinalGroups(chatId),
    ]);
    const lines = [];
    let added = 0;

    for (const g of ggs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Group ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }
    for (const g of fgs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Final ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }
    for (const g of sfgs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Superfinal ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }

    if (!added) {
      await ctx.reply('Tournament demos: (none)');
    } else {
      await replyChunked(ctx, lines.join('\n').trim());
    }
    return;
  }

  if (sub === 'delall') {
    if (!(await requireAdminGuard(ctx))) return;
    const existing = await getTournamentLogo(chatId);
    await setChatSettings(chatId, {
      tournamentName: null, tournamentSite: null, tournamentDesc: null, tournamentLogo: null,
      tournamentServers: [], tournamentPack: null, tournamentStreams: [], tournamentNewsChannel: null,
      tournamentStatsUrl: null, tournamentStatsEnabled: false,
    });
    if (existing?.relPath) {
      try { await fs.promises.unlink(path.join(SCREENSHOTS_DIR, existing.relPath)); } catch (_) { }
    }
    const n = await delAllNews(chatId, 'tournament');
    await ctx.reply(`Tournament settings and logo cleared. Deleted tournament news: ${n}.`);
    return;
  }

  await ctx.reply('Неизвестная опция /tournament. Используйте: name, site, desc, logo, news, info, servers, pack, streams, demos, newschannel, stats_url, stats_enabled или delall.');
}


// ДОБАВИТЬ ГЛОБАЛЬНО (рядом с другими runtime-состояниями)
/**
 * Переключение контекста чата: userId -> targetChatId
 * Если задан, все операции будут работать с данным chatId (права/данные — из него),
 * а ответы бот по-прежнему отправляет в текущий чат.
 */
const userTargetChat = new Map(); // key = Number(userId) -> Number(chatId)

// Ключ переопределения контекста: <sourceChatId>:<userId>
function makeOverrideKey(ctx) {
  const srcChatId = Number(ctx.chat?.id);
  const userId = Number(ctx.from?.id);
  if (!Number.isInteger(srcChatId) || !Number.isInteger(userId)) return null;
  return `${srcChatId}:${userId}`;
}

/** Возвращает целевой chatId для операций (переключённый через /setid) или текущий ctx.chat.id */
function getEffectiveChatId(ctx) {
  const key = makeOverrideKey(ctx);
  if (key) {
    const override = userTargetChat.get(key);
    if (Number.isInteger(override)) return override;
  }
  return ctx.chat?.id;
}


// --------- Screenshots helpers and runtime state ---------

// In-memory сессии приёма скриншотов: key = `${chatId}:${userId}`
const screenshotSessions = new Map();
function ssKey(chatId, userId) { return `${chatId}:${userId}`; }
function purgeExpiredSessions() {
  const now = Date.now();
  for (const [k, s] of screenshotSessions.entries()) {
    if (s.expiresAt && s.expiresAt <= now) screenshotSessions.delete(k);
  }
}

// Формируем стабильный runId на основе createdAt группы
function formatRunId(date) {
  const d = date instanceof Date ? date : new Date(date);
  return d.toISOString().replace(/[:.]/g, '-');
}
function getGroupRunIdFromDoc(g) {
  const dt = g?.createdAt ? new Date(g.createdAt) : new Date();
  return formatRunId(dt);
}

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

function guessExt(mime, filePath, origName) {
  // сначала по пути TG (там обычно есть расширение)
  const src = filePath || origName || '';
  const extByPath = path.extname(src).replace('.', '').toLowerCase();
  if (extByPath) return extByPath;
  // по mime
  const m = (mime || '').toLowerCase();
  if (m === 'image/jpeg' || m === 'image/jpg') return 'jpg';
  if (m === 'image/png') return 'png';
  if (m === 'image/webp') return 'webp';
  if (m === 'image/gif') return 'gif';
  return 'jpg';
}

function downloadToFile(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    let bytes = 0;
    https.get(url, res => {
      if (res.statusCode !== 200) {
        file.destroy();
        fs.promises.unlink(destPath).catch(() => { });
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      res.on('data', chunk => { bytes += chunk.length; });
      res.pipe(file);
      file.on('finish', () => file.close(() => resolve(bytes)));
    }).on('error', err => {
      file.destroy();
      fs.promises.unlink(destPath).catch(() => { });
      reject(err);
    });
  });
}

async function saveTelegramFileToDisk(bot, fileId, opts) {
  // opts: { baseDirAbs, relDir, fileUniqueId, mime, origName }
  const fileInfo = await bot.telegram.getFile(fileId); // { file_path, file_size }
  const ext = guessExt(opts.mime, fileInfo.file_path, opts.origName);
  const filename = `${Date.now()}__${opts.fileUniqueId}.${ext}`;
  const relPath = path.join(opts.relDir, filename);
  const absDir = path.join(opts.baseDirAbs, opts.relDir);
  await ensureDir(absDir);
  const absPath = path.join(absDir, filename);
  const url = `https://api.telegram.org/file/bot${BOT_TOKEN}/${fileInfo.file_path}`;
  const size = await downloadToFile(url, absPath);
  return { relPath, absPath, size, ext };
}

async function addScreenshotMeta({ chatId, scope, groupId, groupRunId, file }) {
  // scope: 'group' | 'superfinal'
  await colScreenshots.updateOne(
    { chatId, scope, groupId: Number(groupId), groupRunId },
    {
      $setOnInsert: { chatId, scope, groupId: Number(groupId), groupRunId, createdAt: new Date() },
      $set: { updatedAt: new Date() },
      $push: { files: { ...file, addedAt: new Date() } }
    },
    { upsert: true }
  );
}


async function showGroupScreenshots(ctx, chatId, groupId) {
  const g = await getGameGroup(chatId, groupId);
  if (!g) {
    await ctx.reply(`Group ${groupId} not found.`);
  } else {
    const runId = getGroupRunIdFromDoc(g);
    const doc = await colScreenshots.findOne({ chatId, scope: 'group', groupId: Number(groupId), groupRunId: runId });
    const files = doc?.files || [];
    if (!files.length) {
      await ctx.reply('Скриншоты не найдены для текущей версии групп.');
      return;
    }

    // Фото можно отправлять альбомами (2..10), документы — по одному.
    const photos = files.filter(f => f.type === 'photo');
    const docs = files.filter(f => f.type === 'document');

    // Фото пачками по 10; если 1 — отправим одиночным фото.
    for (let i = 0; i < photos.length;) {
      const chunk = photos.slice(i, i + 10);
      if (chunk.length === 1) {
        const p = chunk[0];
        const media = p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) };
        // eslint-disable-next-line no-await-in-loop
        await ctx.replyWithPhoto(media);
      } else {
        const mediaGroup = chunk.map(p => ({
          type: 'photo',
          media: p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) },
        }));
        // eslint-disable-next-line no-await-in-loop
        await ctx.replyWithMediaGroup(mediaGroup);
      }
      i += chunk.length;
    }

    // Документы по одному
    for (const d of docs) {
      const media = d.tgFileId ? d.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, d.relPath)) };
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithDocument(media);
    }
  }
}

// --------- Tournament logo helpers ---------

async function getTournamentLogo(chatId) {
  const doc = await colChats.findOne({ chatId }, { projection: { tournamentLogo: 1 } });
  return doc?.tournamentLogo || null; // { relPath, mime, size, tgFileId, tgUniqueId, uploaderId, uploaderUsername, updatedAt }
}

async function setTournamentLogo(chatId, logo) {
  await colChats.updateOne(
    { chatId },
    { $set: { chatId, tournamentLogo: { ...logo, updatedAt: new Date() } } },
    { upsert: true }
  );
}

async function showTournamentLogo(ctx, chatId) {
  const logo = await getTournamentLogo(chatId);
  if (!logo?.relPath && !logo?.tgFileId) {
    await ctx.reply('Логотип турнира не задан. Используйте /tournament logo add (только для админов).');
    return;
  }
  const media = logo.tgFileId
    ? logo.tgFileId
    : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, logo.relPath)) };
  await ctx.replyWithPhoto(media);
}


async function showSuperFinalScreenshots(ctx, chatId, groupId) {
  const g = await getSuperFinalGroup(chatId, groupId);
  if (!g) {
    await ctx.reply(`Superfinal ${groupId} not found.`);
    return;
  }
  const runId = getGroupRunIdFromDoc(g);
  const doc = await colScreenshots.findOne({ chatId, scope: 'superfinal', groupId: Number(groupId), groupRunId: runId });
  const files = doc?.files || [];
  if (!files.length) {
    await ctx.reply('Скриншоты не найдены для текущего суперфинала.');
    return;
  }

  const photos = files.filter(f => f.type === 'photo');
  const docs = files.filter(f => f.type === 'document');

  for (let i = 0; i < photos.length;) {
    const chunk = photos.slice(i, i + 10);
    if (chunk.length === 1) {
      const p = chunk[0];
      const media = p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) };
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithPhoto(media);
    } else {
      const mediaGroup = chunk.map(p => ({
        type: 'photo',
        media: p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) },
      }));
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithMediaGroup(mediaGroup);
    }
    i += chunk.length;
  }

  for (const d of docs) {
    const media = d.tgFileId ? d.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, d.relPath)) };
    // eslint-disable-next-line no-await-in-loop
    await ctx.replyWithDocument(media);
  }
}

async function showFinalScreenshots(ctx, chatId, groupId) {
  const g = await getFinalGroup(chatId, groupId);
  if (!g) {
    await ctx.reply(`Final ${groupId} not found.`);
    return;
  }
  const runId = getGroupRunIdFromDoc(g);
  const doc = await colScreenshots.findOne({ chatId, scope: 'final', groupId: Number(groupId), groupRunId: runId });
  const files = doc?.files || [];
  if (!files.length) {
    await ctx.reply('Скриншоты не найдены для текущего финала.');
    return;
  }

  const photos = files.filter(f => f.type === 'photo');
  const docs = files.filter(f => f.type === 'document');

  // Фото пачками по 10
  for (let i = 0; i < photos.length;) {
    const chunk = photos.slice(i, i + 10);
    if (chunk.length === 1) {
      const p = chunk[0];
      const media = p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) };
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithPhoto(media);
    } else {
      const mediaGroup = chunk.map(p => ({
        type: 'photo',
        media: p.tgFileId ? p.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, p.relPath)) },
      }));
      // eslint-disable-next-line no-await-in-loop
      await ctx.replyWithMediaGroup(mediaGroup);
    }
    i += chunk.length;
  }

  // Документы по одному
  for (const d of docs) {
    const media = d.tgFileId ? d.tgFileId : { source: fs.createReadStream(path.join(SCREENSHOTS_DIR, d.relPath)) };
    // eslint-disable-next-line no-await-in-loop
    await ctx.replyWithDocument(media);
  }
}


// Приём изображений в активной сессии
// ПОЛНАЯ ЗАМЕНА функции handleIncomingScreenshot на версию с поддержкой scope 'custom'
// ПОЛНАЯ ЗАМЕНА функции handleIncomingScreenshot — добавлена поддержка режима 'achv_logo'
// ПОЛНАЯ ЗАМЕНА функции handleIncomingScreenshot
async function handleIncomingScreenshot(ctx) {
  purgeExpiredSessions();
  purgeExpiredNews2Sessions();
  const chatId = getEffectiveChatId(ctx); // <-- было ctx.chat?.id
  const userId = ctx.from?.id;
  if (!chatId || !userId) return;

  const key = ssKey(chatId, userId);
  const sess = screenshotSessions.get(key);
  if (!sess) return; // нет активной сессии — игнор

  const now = Date.now();
  if (sess.expiresAt && sess.expiresAt < now) {
    screenshotSessions.delete(key);
    await ctx.reply('Сессия приёма изображений истекла. Запустите снова команду добавления.');
    return;
  }

  // Поддерживаем фото и документ-изображение
  let kind = null;
  let fileId = null;
  let fileUniqueId = null;
  let width, height, size, mime, origName;

  if (ctx.message.photo && ctx.message.photo.length) {
    const ph = ctx.message.photo.slice().sort((a, b) => (b.file_size || 0) - (a.file_size || 0))[0];
    kind = 'photo';
    fileId = ph.file_id;
    fileUniqueId = ph.file_unique_id;
    width = ph.width;
    height = ph.height;
    size = ph.file_size;
    mime = 'image/jpeg'; // Telegram не отдаёт mime для photo
  } else if (ctx.message.document) {
    const doc = ctx.message.document;
    const m = (doc.mime_type || '').toLowerCase();
    const name = doc.file_name || '';
    const allowedByMime = m.startsWith('image/');
    const allowedByExt = ['.jpg', '.jpeg', '.png', '.webp', '.gif'].includes(path.extname(name || '').toLowerCase());
    if (!allowedByMime && !allowedByExt) {
      await ctx.reply('Этот файл не похож на изображение. Поддерживаются: JPG, PNG, WEBP, GIF.');
      return;
    }
    kind = 'document';
    fileId = doc.file_id;
    fileUniqueId = doc.file_unique_id;
    size = doc.file_size;
    mime = doc.mime_type || undefined;
    origName = doc.file_name || undefined;
  } else {
    return; // другие типы сообщений не обрабатываем
  }

  // Куда сохраняем и какой scope
  let relDir;
  let scope;
  if (sess.mode === 'superfinal') {
    scope = 'superfinal';
    relDir = path.join(String(chatId), 'superfinals', sess.runId, `superfinal_${sess.groupId}`);
  } else if (sess.mode === 'final') {
    scope = 'final';
    relDir = path.join(String(chatId), 'finals', sess.runId, `final_${sess.groupId}`);
  } else if (sess.mode === 'tlogo') {
    scope = 'tlogo';
    relDir = path.join(String(chatId), 'tournament', 'logo');
  } else if (sess.mode === 'achv' || sess.mode === 'achv_logo') {
    scope = 'achv';
    relDir = path.join(String(chatId), 'achievements');
  } else if (sess.mode === 'custom') {
    scope = 'custom';
    relDir = path.join(String(chatId), 'custom', sess.runId, `custom_${sess.groupId}`);
  } else if (sess.mode === 'news2_one' || sess.mode === 'news_edit_img') {
    scope = 'news'; relDir = path.join(String(chatId), 'news');
  } else {
    scope = 'group';
    relDir = path.join(String(chatId), 'groups', sess.runId, `group_${sess.groupId}`);
  }

  // Сохранить файл на диск
  const saved = await saveTelegramFileToDisk(bot, fileId, {
    baseDirAbs: SCREENSHOTS_DIR,
    relDir,
    fileUniqueId,
    mime,
    origName,
  });

  // <<< ВАЖНО: имя файла нужно во всех режимах ниже
  const fileNameOnly = path.basename(saved.relPath);

  // Обработка по scope
  if (scope === 'tlogo') {
    // Турнирный логотип — один «текущий» на чат; старый файл удаляем
    const prev = await getTournamentLogo(chatId);
    await setTournamentLogo(chatId, {
      relPath: saved.relPath,
      mime,
      size: size || saved.size,
      tgFileId: fileId,
      tgUniqueId: fileUniqueId,
      uploaderId: userId,
      uploaderUsername: ctx.from?.username || null,
    });
    if (prev?.relPath && prev.relPath !== saved.relPath) {
      fs.promises.unlink(path.join(SCREENSHOTS_DIR, prev.relPath)).catch(() => { });
    }

    // Продлеваем сессию
    sess.count = (sess.count || 0) + 1;
    sess.expiresAt = Date.now() + 10 * 60 * 1000;
    screenshotSessions.set(key, sess);

    try { await ctx.reply('✅ Сохранено'); } catch (_) { }
    return;
  }

  if (sess.mode === 'achv' || sess.mode === 'achv_logo') {
    // В режимах ачивок ничего в БД не пишем — только временно храним последний файл
    if (sess.tempImage?.relPath && sess.tempImage.relPath !== saved.relPath) {
      try { await fs.promises.unlink(path.join(SCREENSHOTS_DIR, sess.tempImage.relPath)); } catch (_) { }
    }
    sess.tempImage = {
      relPath: saved.relPath,
      mime,
      size: size || saved.size,
      tgFileId: fileId,
      tgUniqueId: fileUniqueId,
      uploaderId: userId,
      uploaderUsername: ctx.from?.username || null,
    };
    // Продлеваем сессию и подтверждаем
    sess.count = (sess.count || 0) + 1;
    sess.expiresAt = Date.now() + 10 * 60 * 1000;
    screenshotSessions.set(key, sess);
    try { await ctx.reply('✅ Сохранено'); } catch (_) { }
    return;
  }

  if (sess.mode === 'news2_one') {
    // Одно сообщение: берем подпись из caption
    const caption = (ctx.message.caption || '').trim();
    const fileNameOnly = path.basename(saved.relPath);

    const doc = {
      chatId,
      scope: 'tournament',
      text: caption,
      news_img_file_name: fileNameOnly, // <--- ИМЯ ФАЙЛА
      author: { id: userId, username: ctx.from?.username || null },
      createdAt: new Date(),
    };

    try {
      const ins = await colNews.insertOne(doc);
      doc._id = ins.insertedId;

      // Автопост в привязанный канал: фото + подпись
      try { await postOneNewsToChannel(chatId, 'tournament', doc); } catch (e) { console.error('postOneNewsToChannel news2_one', e); }

      screenshotSessions.delete(key);
      await ctx.reply(`Новость сохранена. ID: ${ins.insertedId}`);
    } catch (e) {
      console.error('news2_one insert error', e);
      await ctx.reply('Ошибка сохранения новости.');
    }
    return;
  }

  // --- НОВОЕ: замена картинки у существующей новости после /news edit ---
  if (sess.mode === 'news_edit_img') {
    let _id;
    try { _id = new ObjectId(sess.newsId); } catch (_) { _id = null; }
    if (!_id) {
      screenshotSessions.delete(key);
      await ctx.reply('Не удалось распознать ID новости. Сессия завершена.');
      return;
    }

    // Обновляем новость на новый файл
    await colNews.updateOne(
      { _id, chatId },
      { $set: { news_img_file_name: fileNameOnly, updatedAt: new Date() } }
    );

    // Удалим старый файл, если отличается
    if (sess.oldImgFileName && sess.oldImgFileName !== fileNameOnly) {
      const oldAbs = path.join(SCREENSHOTS_DIR, String(chatId), 'news', sess.oldImgFileName);
      fs.promises.unlink(oldAbs).catch(() => { });
    }

    screenshotSessions.delete(key);
    await ctx.reply('Картинка новости заменена.');
    return;
  }

  // Основные сценарии: group | final | superfinal | custom — пишем метаданные в БД
  await addScreenshotMeta({
    chatId,
    scope, // 'group' | 'final' | 'superfinal' | 'custom'
    groupId: sess.groupId,
    groupRunId: sess.runId,
    file: {
      type: kind,
      tgFileId: fileId,
      tgUniqueId: fileUniqueId,
      mime: mime || null,
      size: size || saved.size || null,
      width: width || null,
      height: height || null,
      relPath: saved.relPath,
      uploaderId: userId,
      uploaderUsername: ctx.from?.username || null,
    }
  });

  // Обновляем состояние сессии и отвечаем галочкой
  sess.count = (sess.count || 0) + 1;
  sess.expiresAt = Date.now() + 10 * 60 * 1000;
  screenshotSessions.set(key, sess);

  try { await ctx.reply('✅ Сохранено'); } catch (_) { }
}


// --------- Screenshots: deletion and bulk show ---------

async function deleteScreenshotFiles(files = []) {
  for (const f of files) {
    const abs = path.join(SCREENSHOTS_DIR, f.relPath || '');
    try { await fs.promises.unlink(abs); } catch (_) { /* ignore */ }
  }
}

async function deleteScreenshotsForGroup(chatId, scope, groupId, groupRunId) {
  const q = { chatId, scope, groupId: Number(groupId), groupRunId };
  const doc = await colScreenshots.findOne(q);
  if (!doc) return { deletedFiles: 0, deletedDocs: 0 };
  const files = doc.files || [];
  await deleteScreenshotFiles(files);
  await colScreenshots.deleteOne(q);
  return { deletedFiles: files.length, deletedDocs: 1 };
}

async function deleteAllScreenshotsForScope(chatId, scope) {
  const cur = colScreenshots.find({ chatId, scope });
  const docs = await cur.toArray();
  let total = 0;
  for (const d of docs) {
    // eslint-disable-next-line no-await-in-loop
    await deleteScreenshotFiles(d.files || []);
    total += (d.files || []).length;
  }
  const res = await colScreenshots.deleteMany({ chatId, scope });
  return { deletedFiles: total, deletedDocs: res.deletedCount || 0 };
}

async function deleteAllScreenshotsForChat(chatId) {
  const cur = colScreenshots.find({ chatId });
  const docs = await cur.toArray();
  let total = 0;
  for (const d of docs) {
    // eslint-disable-next-line no-await-in-loop
    await deleteScreenshotFiles(d.files || []);
    total += (d.files || []).length;
  }
  const res = await colScreenshots.deleteMany({ chatId });
  return { deletedFiles: total, deletedDocs: res.deletedCount || 0 };
}

// Показать скриншоты всех групп текущего запуска в заданном scope
async function showAllScopeScreenshots(ctx, chatId, scope) {
  let items = [];
  if (scope === 'group') items = await listGameGroups(chatId);
  else if (scope === 'final') items = await listFinalGroups(chatId);
  else if (scope === 'superfinal') items = await listSuperFinalGroups(chatId);

  if (!items.length) {
    await ctx.reply(scope === 'group'
      ? 'Нет игровых групп.'
      : scope === 'final'
        ? 'Нет финальных групп.'
        : 'Нет суперфинальных групп.'
    );
    return;
  }
  // Для каждого элемента показываем его скриншоты (текущий runId берётся из createdAt)
  for (const g of items) {
    // eslint-disable-next-line no-await-in-loop
    await ctx.reply(`${scope === 'group' ? 'Group' : scope === 'final' ? 'Final' : 'Superfinal'} ${g.groupId} screenshots:`);
    if (scope === 'group') {
      // eslint-disable-next-line no-await-in-loop
      await showGroupScreenshots(ctx, chatId, g.groupId);
    } else if (scope === 'final') {
      // eslint-disable-next-line no-await-in-loop
      await showFinalScreenshots(ctx, chatId, g.groupId);
    } else {
      // eslint-disable-next-line no-await-in-loop
      await showSuperFinalScreenshots(ctx, chatId, g.groupId);
    }
  }
}


// --------- News (новости) ---------

async function editNewsById(chatId, scope, id, newText) {
  let _id;
  try {
    _id = new ObjectId(String(id));
  } catch (_) {
    return { error: 'Некорректный ID новости.' };
  }
  const res = await colNews.updateOne(
    { _id, chatId, scope },
    { $set: { text: newText, updatedAt: new Date() } }
  );
  if (!res.matchedCount) {
    return { error: 'Новость с таким ID не найдена в текущем чате/разделе.' };
  }
  return { ok: true };
}

async function editNewsAnyScope(chatId, id, newText) {
  let _id;
  try {
    _id = new ObjectId(String(id));
  } catch (_) {
    return { error: 'Некорректный ID новости.' };
  }
  const res = await colNews.updateOne(
    { _id, chatId },
    { $set: { text: newText, updatedAt: new Date() } }
  );
  if (!res.matchedCount) {
    return { error: 'Новость с таким ID не найдена для этого чата.' };
  }
  return { ok: true };
}

async function deleteNewsById(chatId, id) {
  let _id;
  try {
    _id = new ObjectId(String(id));
  } catch (_) {
    return { error: 'Некорректный ID новости.' };
  }
  const res = await colNews.deleteOne({ _id, chatId });
  if (!res.deletedCount) {
    return { error: 'Новость с таким ID не найдена для этого чата.' };
  }
  return { ok: true };
}


// colNews: { chatId, scope: 'group'|'final'|'superfinal'|'tournament', groupRunId?: string, text, createdAt, authorId, authorUsername }
// ПОЛНАЯ ЗАМЕНА функции addNews — теперь после сохранения новость автоматически отправляется в канал (если он задан)
async function addNews(chatId, scope, text, groupRunId = null, author = {}) {
  const doc = {
    chatId,
    scope,                    // 'group'|'final'|'superfinal'|'tournament'
    groupRunId: groupRunId || null,
    text,
    createdAt: new Date(),
    authorId: author.id || null,
    authorUsername: author.username || null,
  };
  const res = await colNews.insertOne(doc);
  // Подготовим документ с _id для отправки в канал
  const newsDoc = { _id: res.insertedId, ...doc };

  // Пытаемся продублировать в канал (если он привязан)
  try {
    await postOneNewsToChannel(chatId, scope, newsDoc);
  } catch (e) {
    console.error('addNews->postOneNewsToChannel error:', e);
  }
}


async function listNews(chatId, scope, groupRunId = null) {
  const q = { chatId, scope };
  if (groupRunId) q.groupRunId = groupRunId;
  return colNews.find(q).sort({ createdAt: -1 }).toArray();
}

async function delAllNews(chatId, scope) {
  const res = await colNews.deleteMany({ chatId, scope });
  return res.deletedCount || 0;
}

// Возвращает последний runId по scope (по максимальному createdAt у групп соответствующего scope)
async function findLatestRunIdForScope(chatId, scope) {
  let col;
  if (scope === 'group') col = colGameGroups;
  else if (scope === 'final') col = colFinalGroups;
  else if (scope === 'superfinal') col = colSuperFinalGroups;
  else return null;
  const last = await col.find({ chatId }).sort({ createdAt: -1 }).limit(1).toArray();
  if (!last.length) return null;
  return getGroupRunIdFromDoc(last[0]);
}

function formatNewsList(news = []) {
  if (!news.length) return 'Новости: (пусто)';
  const lines = news.map(n => {
    const idStr = n._id ? String(n._id) : '?';
    const ts = n.createdAt ? new Date(n.createdAt).toLocaleString() : '';
    const who = n.authorUsername ? `@${n.authorUsername}` : (n.authorId ? `#${n.authorId}` : '');
    return `- [${idStr}] [${ts}] ${n.text}${who ? ` (${who})` : ''}`;
  });
  return `Новости:\n${lines.join('\n')}`;
}



bot.command(['tournament', 't'], tournamentHandler);

// GROUPS

async function groupsHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в группе.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // No args -> list all groups (+ waiting), в столбик
  if (!tokens.length) {
    const [ggs, waiting, ptsArr] = await Promise.all([
      listGameGroups(chatId),
      getWaitingPlayers(chatId),
      getGroupPoints(chatId),
    ]);
    await replyPre(ctx, formatGameGroupsList(ggs, waiting, groupPointsToMap(ptsArr), { twoCols: false }));
    return;
  }

  const cmd = tokens[0].toLowerCase();

  // /groups demos — список ссылок по всем группам
  if (cmd === 'demos') {
    const ggs = await listGameGroups(chatId);
    const lines = [];
    let added = 0;
    for (const g of ggs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Group ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }
    if (!added) {
      await ctx.reply('Group demos: (none)');
    } else {
      await replyChunked(ctx, lines.join('\n').trim());
    }
    return;
  }

  // /groups screenshots [delall]
  if (cmd === 'screenshots') {
    const sub = (tokens[1] || '').toLowerCase();
    if (sub === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const res = await deleteAllScreenshotsForScope(chatId, 'group');
      await ctx.reply(`Screenshots deleted for groups: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
      return;
    }
    await showAllScopeScreenshots(ctx, chatId, 'group');
    return;
  }

  // /groups news [add <text> | delall]

  if (cmd === 'news') {
    const sub = (tokens[1] || '').toLowerCase();

    if (sub === 'add') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const text = tokens.slice(2).join(' ').trim();
      if (!text) { await ctx.reply('Использование: /groups news add <текст новости>'); return; }
      const runId = await findLatestRunIdForScope(chatId, 'group');
      if (!runId) { await ctx.reply('Нет текущих игровых групп. Сначала сформируйте /groups make.'); return; }
      await addNews(chatId, 'group', text, runId, { id: ctx.from.id, username: ctx.from.username });
      await ctx.reply('Новость добавлена.');
      return;
    }

    if (sub === 'edit') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const idStr = tokens[2];
      const text = tokens.slice(3).join(' ').trim();
      if (!idStr || !text) { await ctx.reply('Использование: /groups news edit <id> <текст новости>'); return; }
      const res = await editNewsById(chatId, 'group', idStr, text);
      if (res.error) { await ctx.reply(res.error); return; }
      await ctx.reply('Новость обновлена.');
      return;
    }

    if (sub === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const n = await colNews.deleteMany({ chatId, scope: 'group' });
      await ctx.reply(`Удалено новостей (groups): ${n.deletedCount || 0}.`);
      return;
    }

    const runId = await findLatestRunIdForScope(chatId, 'group');
    if (!runId) { await ctx.reply('Нет текущих игровых групп.'); return; }
    const news = await listNews(chatId, 'group', runId);
    await replyChunked(ctx, formatNewsList(news));
    return;
  }

  // /groups move ...
  if (cmd === 'move') {
    if (!(await requireAdminGuard(ctx))) return;
    const n1 = Number(tokens[1]);
    const n2 = Number(tokens[2]);
    const tail = tokens.slice(3).join(' ');
    if (!Number.isInteger(n1) || n1 <= 0 || !Number.isInteger(n2) || n2 <= 0 || !tail) {
      await ctx.reply('Использование: /groups move <from> <to> <player>. Пример: /groups move 1 2 ly');
      return;
    }
    if (n1 === n2) {
      await ctx.reply('Группы совпадают.');
      return;
    }
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) {
      await ctx.reply('Укажите игрока. Пример: /groups move 1 2 ly');
      return;
    }
    const target = list[0];

    const fromG = await getGameGroup(chatId, n1);
    if (!fromG) { await ctx.reply(`Group ${n1} not found.`); return; }
    const toG = await getGameGroup(chatId, n2);
    if (!toG) { await ctx.reply(`Group ${n2} not found.`); return; }

    const idx = (fromG.players || []).findIndex(p => p.nameNorm === target.nameNorm);
    if (idx === -1) {
      await ctx.reply(`${target.nameOrig} не найден(а) в группе ${n1}.`);
      return;
    }

    const settings = await getChatSettings(chatId);
    const cap = settings.maxPlayers || DEFAULT_MAX_PLAYERS;
    const toPlayers = toG.players || [];
    if (toPlayers.length >= cap) {
      await ctx.reply(`Группа ${n2} заполнена (capacity ${cap}).`);
      return;
    }

    if (toPlayers.some(p => p.nameNorm === target.nameNorm)) {
      await ctx.reply(`${target.nameOrig} уже в группе ${n2}.`);
      return;
    }

    const moving = { ...fromG.players[idx] };
    delete moving.pos;

    const nextFrom = (fromG.players || []).slice();
    nextFrom.splice(idx, 1);
    const nextTo = toPlayers.concat([moving]);

    await upsertGameGroup(chatId, n1, { ...fromG, players: nextFrom });
    await upsertGameGroup(chatId, n2, { ...toG, players: nextTo });

    await ctx.reply(`Moved ${moving.nameOrig} from group ${n1} to group ${n2}.`);
    return;
  }

  // /groups rating ...
  if (cmd === 'rating') {
    const tail = tokens.slice(1).join(' ').trim();
    if (!tail) {
      const rating = await getRating(chatId);
      await replyChunked(ctx, formatRatingList(rating));
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) {
      await ctx.reply('Укажите игроков через запятую. Пример: /groups rating David,pp,aid,proto');
      return;
    }
    await setRating(chatId, list);
    const rating = await getRating(chatId);
    await replyChunked(ctx, 'Rating is set (previous rating cleared).\n' + formatRatingList(rating));
    return;
  }

  // /groups points ...
  if (cmd === 'points') {
    const tail = tokens.slice(1).join(' ').trim();

    if (!tail) {
      const ptsArr = await getGroupPoints(chatId);
      if (!ptsArr.length) {
        await ctx.reply('Group points: (none)');
        return;
      }
      const sorted = ptsArr
        .slice()
        .sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
      const lines = ['Group points:'].concat(sorted.map(p => `${p.nameOrig}[${p.pts}]`));
      await replyChunked(ctx, lines.join('\n'));
      return;
    }

    if (!(await requireAdminGuard(ctx))) return;
    const parsed = parsePointsList(tail);
    if (parsed.error) {
      await ctx.reply(parsed.error);
      return;
    }
    if (!parsed.length) {
      await ctx.reply('Укажите игроков и очки. Пример: /groups points David[10],aid[12]');
      return;
    }

    const ggs = await listGameGroups(chatId);
    const present = new Map();
    for (const g of ggs) for (const p of (g.players || [])) present.set(p.nameNorm, p.nameOrig);
    const missing = parsed.filter(p => !present.has(p.nameNorm)).map(p => p.nameOrig);
    if (missing.length) {
      await ctx.reply(`Не найдены в игровых группах: ${missing.join(', ')}`);
      return;
    }

    const toSave = parsed.map(p => ({ nameNorm: p.nameNorm, nameOrig: present.get(p.nameNorm), pts: p.pts }));
    await setGroupPoints(chatId, toSave);
    await ctx.reply('Group points are set.');
    return;
  }

  // /groups players
  if (cmd === 'players') {
    const [ggs, ptsArr] = await Promise.all([listGameGroups(chatId), getGroupPoints(chatId)]);
    if (!ggs.length) {
      await ctx.reply('Players (all game groups): (none)');
      return;
    }
    const ptsMap = groupPointsToMap(ptsArr);

    const seen = new Set();
    const all = [];
    for (const g of ggs) {
      for (const p of (g.players || [])) {
        if (!seen.has(p.nameNorm)) {
          seen.add(p.nameNorm);
          all.push({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: p.sg, pos: p.pos });
        }
      }
    }

    const anyPts = all.some(p => ptsMap.has(p.nameNorm));
    let names;
    if (anyPts) {
      all.sort((a, b) => {
        const aa = ptsMap.has(a.nameNorm) ? ptsMap.get(a.nameNorm) : Number.POSITIVE_INFINITY;
        const bb = ptsMap.has(b.nameNorm) ? ptsMap.get(b.nameNorm) : Number.POSITIVE_INFINITY;
        if (aa !== bb) return aa - bb;
        return a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' });
      });
      names = all.map(p => p.nameOrig);
    } else {
      const ordered = sortPlayersForDisplay(all);
      names = ordered.map(p => p.nameOrig);
    }

    await replyChunked(ctx, `Players (all game groups): ${names.join(', ')}`);
    return;
  }

  // Настройки и операции по группам
  if (['algo', 'minplayers', 'recplayers', 'maxcount', 'maxplayers', 'make', 'del', 'delall'].includes(cmd)) {
    if (cmd === 'algo') {
      if (!(await requireAdminGuard(ctx))) return;
      const v = Number(tokens[1]);
      if (![1, 2, 3].includes(v)) { await ctx.reply('Использование: /groups algo <1|2|3>'); return; }
      await setChatSettings(chatId, { groupsAlgo: v });
      await ctx.reply(`groups algo set to ${v}.`);
      return;
    }

    if (cmd === 'maxplayers') {
      if (!(await requireAdminGuard(ctx))) return;
      const v = Number(tokens[1]);
      if (!Number.isInteger(v) || v <= 0) { await ctx.reply('Использование: /groups maxplayers <целое > 0>'); return; }
      await setChatSettings(chatId, { maxPlayers: v });
      await ctx.reply(`groups maxPlayers set to ${v}.`);
      return;
    }

    if (cmd === 'minplayers') {
      if (!(await requireAdminGuard(ctx))) return;
      const v = Number(tokens[1]);
      if (!Number.isInteger(v) || v <= 0) { await ctx.reply('Использование: /groups minplayers <целое > 0>'); return; }
      await setChatSettings(chatId, { minPlayers2: v });
      await ctx.reply(`algo2 minPlayers set to ${v}.`);
      return;
    }

    if (cmd === 'recplayers') {
      if (!(await requireAdminGuard(ctx))) return;
      const v = Number(tokens[1]);
      if (!Number.isInteger(v) || v <= 0) { await ctx.reply('Использование: /groups recplayers <целое > 0>'); return; }
      await setChatSettings(chatId, { recPlayers2: v });
      await ctx.reply(`algo2 recPlayers set to ${v}.`);
      return;
    }

    if (cmd === 'maxcount') {
      if (!(await requireAdminGuard(ctx))) return;
      const v = Number(tokens[1]);
      if (!Number.isInteger(v) || v <= 0) { await ctx.reply('Использование: /groups maxcount <целое > 0>'); return; }
      await setChatSettings(chatId, { maxCount3: v });
      await ctx.reply(`algo3 maxCount set to ${v}.`);
      return;
    }

    if (cmd === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      await deleteAllGameGroups(chatId);
      await deleteGroupMapResultsForChat(chatId);
      await clearWaitingPlayers(chatId);
      await ctx.reply('All game groups are deleted.');
      return;
    }

    if (cmd === 'del') {
      if (!(await requireAdminGuard(ctx))) return;
      const N = Number(tokens[1]);
      if (!Number.isInteger(N) || N <= 0) { await ctx.reply('Использование: /groups del <N>'); return; }
      const g = await getGameGroup(chatId, N);
      if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
      await deleteGameGroup(chatId, N);
      await deleteGroupMapResultsForGroup(chatId, N);
      await ctx.reply(`Group ${N} deleted.`);
      return;
    }

    if (cmd === 'make') {
      if (!(await requireAdminGuard(ctx))) return;
      const C = Number(tokens[1]);
      if (!Number.isInteger(C) || C <= 0) { await ctx.reply('Использование: /groups make <C>. Пример: /groups make 2'); return; }
      const res = await makeGameGroups(chatId, C);
      if (res?.error) {
        await ctx.reply(`Ошибка: ${res.error}`);
        return;
      }
      const [ptsArr] = await Promise.all([getGroupPoints(chatId)]);
      const txt = formatGameGroupsList(res.groups || [], res.waiting || [], groupPointsToMap(ptsArr), { twoCols: false });
      await replyPre(ctx, txt);
      return;
    }
  }

  // /groups N ...
  const N = Number(tokens[0]);
  if (Number.isInteger(N) && N > 0) {
    if (tokens.length === 1) {
      const [g, ptsArr] = await Promise.all([getGameGroup(chatId, N), getGroupPoints(chatId)]);
      if (!g) {
        await ctx.reply(`Group ${N} not found.`);
        return;
      }
      const txt = formatGameGroupsList([g], [], groupPointsToMap(ptsArr), { twoCols: false });
      await replyPre(ctx, txt);
      return;
    }

    const action = tokens[1].toLowerCase();
    const tail = tokens.slice(2).join(' ');

    // --- NEW: результаты игр на картах для групп ---
    if (action === 'mapres') {
      // /g N mapres       -> показать результаты (публично)
      // /g N mapres delall -> удалить все результаты (админ)
      // /g N mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> <players...>  -> записать (админ)

      // Только номер группы проверяем сразу
      const g = await getGameGroup(chatId, N);
      if (!g) {
        await ctx.reply(`Group ${N} not found.`);
        return;
      }

      // Без доп. параметров: показать (публично)
      if (!tokens[2]) {
        const results = await listGroupMapResults(chatId, N);
        const text = formatMapResultsTable('Group', N, results);
        await replyPre(ctx, text);
        return;
      }

      const sub = (tokens[2] || '').toLowerCase();

      // delall
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const existed = await listGroupMapResults(chatId, N);
        if (!existed.length) {
          await ctx.reply('Результаты по картам для этой группы не найдены.');
          return;
        }
        await deleteGroupMapResultsForGroup(chatId, N);
        await ctx.reply(`Все результаты по картам для Group ${N} удалены.`);
        return;
      }

      // Запись результата: требуется админ
      if (!(await requireAdminGuard(ctx))) return;

      // Ожидаем: map date time playtime players...
      // tokens: [N, 'mapres', map, 'YYYY-MM-DD', 'HH:MM', 'MM:SS', players...]
      if (tokens.length < 7) {
        await ctx.reply(
          'Некорректный формат.\n' +
          'Использование:\n' +
          '/g <N> mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...]'
        );
        return;
      }

      const mapInput = tokens[2];
      const datePart = tokens[3];
      const timePart = tokens[4];
      const playtime = tokens[5];
      const playersStr = tokens.slice(6).join(' ').trim();

      const dtStr = `${datePart} ${timePart}`;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart) || !/^\d{2}:\d{2}$/.test(timePart)) {
        await ctx.reply('Дата/время матча должны быть в формате YYYY-MM-DD HH:MM (например, 2025-10-20 23:09).');
        return;
      }
      if (!/^\d{1,2}:\d{2}$/.test(playtime)) {
        await ctx.reply('Время игры должно быть в формате MM:SS (например, 6:50).');
        return;
      }

      const mapsArr = Array.isArray(g.maps) ? g.maps : [];
      const foundMapOrig = mapsArr.find(m => norm(m) === norm(mapInput));
      if (!foundMapOrig) {
        await ctx.reply(`Карта "${mapInput}" не назначена для Group ${N}.`);
        return;
      }

      const parsed = parseMapResultPlayers(playersStr);
      if (parsed.error) {
        await ctx.reply(parsed.error);
        return;
      }
      const players = parsed.players || [];

      if (!players.length) {
        await ctx.reply('Список игроков пуст.');
        return;
      }

      const groupPlayersMap = new Map((g.players || []).map(p => [p.nameNorm, p.nameOrig]));
      const missing = players.filter(p => !groupPlayersMap.has(p.nameNorm)).map(p => p.nameOrig);

      if (missing.length) {
        await ctx.reply(
          'Ошибка: следующие игроки отсутствуют в данной группе и результат не будет сохранён:\n' +
          missing.join(', ')
        );
        return;
      }

      // Нормализуем оригинальные имена по группе
      const storedPlayers = players.map(p => ({
        ...p,
        nameOrig: groupPlayersMap.get(p.nameNorm) || p.nameOrig,
      }));

      const matchDateTimeIso = toMoscowIso(datePart, timePart);
      const matchTs = toUnixTsFromMoscow(datePart, timePart);
      if (matchTs == null) {
        await ctx.reply('Не удалось распарсить дату/время (MSK). Проверьте формат YYYY-MM-DD HH:MM.');
        return;
      }

      await upsertGroupMapResult(chatId, N, foundMapOrig, {
        matchDateTime: dtStr,
        matchDateTimeIso,
        matchTs,
        matchPlaytime: playtime,
        players: storedPlayers,
      });

      await ctx.reply(
        `Результат по карте "${foundMapOrig}" для Group ${N} записан/обновлён.\n` +
        'Проверить: /g ' + N + ' mapres'
      );
      return;
    }


    // /groups N demos [add]
    if (action === 'demos') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const list = cleanListParam(tokens.slice(3).join(' '));
        const g = await getGameGroup(chatId, N);
        if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
        await upsertGameGroup(chatId, N, { ...g, demos: list });
        await replyChunked(ctx, list.length ? `Group ${N} demos are set:\n${list.join('\n')}` : `Group ${N} demos cleared.`);
        return;
      }
      const g = await getGameGroup(chatId, N);
      if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (!ds.length) { await ctx.reply(`Group ${N} demos: (none)`); return; }
      await replyChunked(ctx, `Group ${N} demos:\n${ds.join('\n')}`);
      return;
    }

    // /groups N screenshots [add|delall]
    if (action === 'screenshots') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;

        const g = await getGameGroup(chatId, N);
        if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);

        const key = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(key, {
          chatId,
          userId: ctx.from.id,
          mode: 'group',
          groupId: N,
          runId,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
        });

        await ctx.reply(
          `Пришлите один или несколько скриншотов для Group ${N} (изображения JPG/PNG/WEBP/GIF).\n` +
          `Когда закончите — отправьте /done. Для отмены — /cancel.`
        );
        return;
      }
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getGameGroup(chatId, N);
        if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);
        const res = await deleteScreenshotsForGroup(chatId, 'group', N, runId);
        await ctx.reply(`Deleted for Group ${N}: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
        return;
      }
      await showGroupScreenshots(ctx, chatId, N);
      return;
    }

    // /groups N result ...
    if (action === 'result') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tail));
      if (!list.length) {
        await ctx.reply('Укажите игроков через запятую в порядке занятых мест. Пример: /groups 1 result pp,dante,ly');
        return;
      }
      const g = await getGameGroup(chatId, N);
      if (!g) {
        await ctx.reply(`Group ${N} not found.`);
        return;
      }
      const sgs = await listSkillGroups(chatId);
      const sgMap = buildSGMap(sgs);
      const finalPlayers = list.map((p, i) => ({ ...p, pos: i + 1, sg: sgMap.get(p.nameNorm) || null }));
      await upsertGameGroup(chatId, N, { ...g, players: finalPlayers });
      await replyChunked(ctx, `Results set for group ${N}:\n${playersToStringWithPos(finalPlayers)}`);
      return;
    }

    // /groups N addp name1,name2,...
    if (action === 'addp') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tail));
      if (!list.length) { await ctx.reply('Укажите игроков через запятую.'); return; }

      const [g, sgs, allGroups, settings] = await Promise.all([
        getGameGroup(chatId, N),
        listSkillGroups(chatId),
        listGameGroups(chatId),
        getChatSettings(chatId),
      ]);
      if (!g) { await ctx.reply(`Group ${N} not found.`); return; }
      const cap = settings.maxPlayers || DEFAULT_MAX_PLAYERS;

      const sgMap = buildSGMap(sgs);
      const inThis = new Set((g.players || []).map(p => p.nameNorm));
      const presentEverywhere = new Set();
      for (const ag of allGroups) for (const p of (ag.players || [])) presentEverywhere.add(p.nameNorm);

      const added = [];
      const skippedInGroup = [];
      const skippedOtherGroup = [];
      const skippedCapacity = [];

      let nextPlayers = (g.players || []).slice();
      for (const p of list) {
        if (inThis.has(p.nameNorm)) { skippedInGroup.push(p.nameOrig); continue; }
        if (presentEverywhere.has(p.nameNorm)) { skippedOtherGroup.push(p.nameOrig); continue; }
        if (nextPlayers.length >= cap) { skippedCapacity.push(p.nameOrig); continue; }
        const sgNum = sgMap.get(p.nameNorm) || null;
        nextPlayers.push({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, sg: sgNum });
        inThis.add(p.nameNorm);
        added.push(p.nameOrig);
      }

      await upsertGameGroup(chatId, N, { ...g, players: nextPlayers });
      if (added.length) {
        try { await removeFromWaiting(chatId, added.map(a => norm(a))); } catch (_) { }
      }

      let msg = [];
      if (added.length) msg.push(`Added to Group ${N}: ${added.join(', ')}`);
      if (skippedInGroup.length) msg.push(`Skipped (already in group ${N}): ${skippedInGroup.join(', ')}`);
      if (skippedOtherGroup.length) msg.push(`Skipped (already in some group): ${skippedOtherGroup.join(', ')}`);
      if (skippedCapacity.length) msg.push(`Skipped (capacity ${cap}): ${skippedCapacity.join(', ')}`);
      if (!msg.length) msg = ['Нечего добавлять.'];
      await replyChunked(ctx, msg.join('\n'));
      return;
    }

    // /groups N delp name1,name2,...
    if (action === 'delp') {
      if (!(await requireAdminGuard(ctx))) return;
      const list = dedupByNorm(cleanListParam(tail));
      if (!list.length) { await ctx.reply('Укажите игроков через запятую.'); return; }
      const g = await getGameGroup(chatId, N);
      if (!g) { await ctx.reply(`Group ${N} not found.`); return; }

      const toDel = new Set(list.map(p => p.nameNorm));
      const before = g.players || [];
      const after = before.filter(p => !toDel.has(p.nameNorm));
      const actually = before.length - after.length;

      await upsertGameGroup(chatId, N, { ...g, players: after });
      await ctx.reply(actually ? `Deleted from Group ${N}: ${list.map(p => p.nameOrig).join(', ')}` : `Никого не удалено (нет совпадений).`);
      return;
    }

    // /groups N time [value] — показать/задать строку времени (МСК, хранится как текст)
    if (action === 'time') {
      const value = tokens.slice(2).join(' ').trim();
      const g = await getGameGroup(chatId, N);
      if (!g) { await ctx.reply(`Group ${N} not found.`); return; }

      if (!value) {
        await ctx.reply(`Group ${N} time: ${g.time ? g.time : '(not set)'}`);
      } else {
        if (!(await requireAdminGuard(ctx))) return;
        await upsertGameGroup(chatId, N, { ...g, time: value });
        await ctx.reply(`Group ${N} time is set: ${value}`);
      }
      return;
    }


    await ctx.reply('Неизвестная опция. Используйте: demos | screenshots | result | addp | delp или без параметров.');
    return;
  }

  await ctx.reply('Неизвестная опция /groups. Используйте /help.');
}


bot.command(['groups', 'g'], groupsHandler);

// FINALS

async function getFinalGroup(chatId, groupId) {
  return colFinalGroups.findOne({ chatId, groupId: Number(groupId) });
}

async function finalsHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // /finals — список финалов (в столбик)
  if (!tokens.length) {
    const [finals, finalPtsArr] = await Promise.all([
      listFinalGroups(chatId),
      getFinalPoints(chatId),
    ]);
    await replyPre(ctx, formatFinalGroupsList(finals, finalPointsToMap(finalPtsArr), { twoCols: false }));
    return;
  }

  const sub = tokens[0].toLowerCase();

  // finals demos — список ссылок по всем финалам
  if (sub === 'demos') {
    const fgs = await listFinalGroups(chatId);
    const lines = [];
    let added = 0;
    for (const g of fgs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Final ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }
    if (!added) {
      await ctx.reply('Final demos: (none)');
    } else {
      await replyChunked(ctx, lines.join('\n').trim());
    }
    return;
  }

  // finals make <C>
  if (sub === 'make') {
    if (!(await requireAdminGuard(ctx))) return;
    const C = Number(tokens[1]);
    if (!Number.isInteger(C) || C <= 0) {
      await ctx.reply('Использование: /finals make <C>. Пример: /finals make 2');
      return;
    }
    const res = await makeFinals(chatId, C);
    if (res?.error) { await ctx.reply(`Ошибка: ${res.error}`); return; }
    const finalPtsArr = await getFinalPoints(chatId);
    await replyPre(ctx, formatFinalGroupsList(res.groups || [], finalPointsToMap(finalPtsArr), { twoCols: false }));
    return;
  }

  // finals players
  if (sub === 'players') {
    const [fgs, ptsArr] = await Promise.all([listFinalGroups(chatId), getFinalPoints(chatId)]);
    if (!fgs.length) { await ctx.reply('Players (finals): (none)'); return; }
    const pts = finalPointsToMap(ptsArr);
    const seen = new Set();
    const all = [];
    for (const g of fgs) for (const p of (g.players || [])) if (!seen.has(p.nameNorm)) { seen.add(p.nameNorm); all.push(p); }
    const anyPts = all.some(p => pts.has(p.nameNorm));
    if (anyPts) {
      all.sort((a, b) => {
        const aa = pts.has(a.nameNorm) ? pts.get(a.nameNorm) : Number.POSITIVE_INFINITY;
        const bb = pts.has(b.nameNorm) ? pts.get(b.nameNorm) : Number.POSITIVE_INFINITY;
        if (aa !== bb) return aa - bb;
        return a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' });
      });
    } else {
      all.sort((a, b) => a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
    }
    await replyChunked(ctx, `Players (finals): ${all.map(p => p.nameOrig).join(', ')}`);
    return;
  }

  // finals move <from> <to> <player>
  if (sub === 'move') {
    if (!(await requireAdminGuard(ctx))) return;
    const n1 = Number(tokens[1]);
    const n2 = Number(tokens[2]);
    const tail = tokens.slice(3).join(' ');
    if (!Number.isInteger(n1) || n1 <= 0 || !Number.isInteger(n2) || n2 <= 0 || !tail) {
      await ctx.reply('Использование: /finals move <from> <to> <player>. Пример: /finals move 1 2 ly');
      return;
    }
    if (n1 === n2) { await ctx.reply('Группы совпадают.'); return; }
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) { await ctx.reply('Укажите игрока. Пример: /finals move 1 2 ly'); return; }
    const target = list[0];

    const fromG = await getFinalGroup(chatId, n1);
    if (!fromG) { await ctx.reply(`Final ${n1} not found.`); return; }
    const toG = await getFinalGroup(chatId, n2);
    if (!toG) { await ctx.reply(`Final ${n2} not found.`); return; }

    const idx = (fromG.players || []).findIndex(p => p.nameNorm === target.nameNorm);
    if (idx === -1) { await ctx.reply(`${target.nameOrig} не найден(а) в финале ${n1}.`); return; }

    const settings = await getChatSettings(chatId);
    const cap = settings.finalMaxPlayers || DEFAULT_MAX_PLAYERS;
    const toPlayers = toG.players || [];
    if (toPlayers.length >= cap) { await ctx.reply(`Финал ${n2} заполнен (capacity ${cap}).`); return; }
    if (toPlayers.some(p => p.nameNorm === target.nameNorm)) { await ctx.reply(`${target.nameOrig} уже в финале ${n2}.`); return; }

    const moving = { ...fromG.players[idx] };
    delete moving.pos;
    const nextFrom = (fromG.players || []).slice();
    nextFrom.splice(idx, 1);
    const nextTo = toPlayers.concat([moving]);
    await upsertFinalGroup(chatId, n1, { ...fromG, players: nextFrom });
    await upsertFinalGroup(chatId, n2, { ...toG, players: nextTo });
    await ctx.reply(`Moved ${moving.nameOrig} from final ${n1} to final ${n2}.`);
    return;
  }

  // finals points ...
  if (sub === 'points') {
    const tail = tokens.slice(1).join(' ').trim();

    if (!tail) {
      const ptsArr = await getFinalPoints(chatId);
      if (!ptsArr.length) { await ctx.reply('Final points: (none)'); return; }
      const sorted = ptsArr.slice().sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
      const lines = ['Final points:'].concat(sorted.map(p => `${p.nameOrig}[${p.pts}]`));
      await replyChunked(ctx, lines.join('\n'));
      return;
    }

    if (!(await requireAdminGuard(ctx))) return;
    const parsed = parsePointsList(tail);
    if (parsed.error) { await ctx.reply(parsed.error); return; }
    if (!parsed.length) { await ctx.reply('Укажите игроков и очки. Пример: /finals points David[10],aid[12]'); return; }

    const fgs = await listFinalGroups(chatId);
    const present = new Map();
    for (const g of fgs) for (const p of (g.players || [])) present.set(p.nameNorm, p.nameOrig);
    const missing = parsed.filter(p => !present.has(p.nameNorm)).map(p => p.nameOrig);
    if (missing.length) { await ctx.reply(`Не найдены в финалах: ${missing.join(', ')}`); return; }

    const toSave = parsed.map(p => ({ nameNorm: p.nameNorm, nameOrig: present.get(p.nameNorm), pts: p.pts }));
    await setFinalPoints(chatId, toSave);
    await ctx.reply('Final points are set.');
    return;
  }

  // finals rating ...
  if (sub === 'rating') {
    const tail = tokens.slice(1).join(' ').trim();
    if (!tail) {
      const rating = await getFinalRating(chatId);
      await replyChunked(ctx, formatFinalRatingList(rating));
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const list = dedupByNorm(cleanListParam(tail));
    if (!list.length) {
      await ctx.reply('Укажите игроков через запятую. Пример: /finals rating David,pp,aid');
      return;
    }
    await setFinalRating(chatId, list);
    const rating = await getFinalRating(chatId);
    await replyChunked(ctx, 'Finals rating is set (previous cleared).\n' + formatFinalRatingList(rating));
    return;
  }

  // finals algo <1|2>
  if (sub === 'algo') {
    if (!(await requireAdminGuard(ctx))) return;
    const v = Number(tokens[1]);
    if (![1, 2].includes(v)) {
      await ctx.reply('Использование: /finals algo <1|2>');
      return;
    }
    await setChatSettings(chatId, { finalsAlgo: v });
    await ctx.reply(`Finals algo set to ${v}.`);
    return;
  }

  // finals maxplayers [N]
  if (sub === 'maxplayers') {
    if (!tokens[1]) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Finals maxPlayers: ${s.finalMaxPlayers}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const v = Number(tokens[1]);
    if (!Number.isInteger(v) || v <= 0) {
      await ctx.reply('Использование: /finals maxplayers <целое > 0>');
      return;
    }
    await setChatSettings(chatId, { finalMaxPlayers: v });
    await ctx.reply(`Finals maxPlayers set to ${v}.`);
    return;
  }

  // finals totalplayers [N|all|auto|0]
  if (sub === 'totalplayers') {
    if (!tokens[1]) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Finals totalPlayers: ${s.finalTotalPlayers ?? '(all available)'}${s.finalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const valRaw = String(tokens[1]).toLowerCase();
    if (['all', 'auto', '0'].includes(valRaw)) {
      await setChatSettings(chatId, { finalTotalPlayers: null });
      await ctx.reply('Finals totalPlayers set to: all (auto).');
      return;
    }
    const v = Number(tokens[1]);
    if (!Number.isInteger(v) || v <= 0) {
      await ctx.reply('Использование: /finals totalplayers <целое > 0 | all|auto|0>');
      return;
    }
    await setChatSettings(chatId, { finalTotalPlayers: v });
    await ctx.reply(`Finals totalPlayers set to ${v}.`);
    return;
  }

  // finals screenshots [delall]
  if (sub === 'screenshots') {
    const op = (tokens[1] || '').toLowerCase();
    if (op === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const res = await deleteAllScreenshotsForScope(chatId, 'final');
      await ctx.reply(`Screenshots deleted for finals: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
      return;
    }
    await showAllScopeScreenshots(ctx, chatId, 'final');
    return;
  }

  // finals news [add <text> | delall]
  if (sub === 'news') {
    const op = (tokens[1] || '').toLowerCase();

    if (op === 'add') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const text = tokens.slice(2).join(' ').trim();
      if (!text) { await ctx.reply('Использование: /finals news add <текст новости>'); return; }
      const runId = await findLatestRunIdForScope(chatId, 'final');
      if (!runId) { await ctx.reply('Нет текущих финальных групп. Сначала сформируйте /finals make.'); return; }
      await addNews(chatId, 'final', text, runId, { id: ctx.from.id, username: ctx.from.username });
      await ctx.reply('Новость финалов добавлена.');
      return;
    }

    if (op === 'edit') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const idStr = tokens[2];
      const text = tokens.slice(3).join(' ').trim();
      if (!idStr || !text) { await ctx.reply('Использование: /finals news edit <id> <текст новости>'); return; }
      const res = await editNewsById(chatId, 'final', idStr, text);
      if (res.error) { await ctx.reply(res.error); return; }
      await ctx.reply('Новость финалов обновлена.');
      return;
    }

    if (op === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const n = await delAllNews(chatId, 'final');
      await ctx.reply(`Удалено новостей (finals): ${n}.`);
      return;
    }

    const runId = await findLatestRunIdForScope(chatId, 'final');
    if (!runId) { await ctx.reply('Нет текущих финальных групп.'); return; }
    const news = await listNews(chatId, 'final', runId);
    await replyChunked(ctx, formatNewsList(news));
    return;
  }

  // finals delall — удалить все финальные группы
  if (sub === 'delall') {
    if (!(await requireAdminGuard(ctx))) return;
    await deleteAllFinalGroups(chatId);
    await ctx.reply('All final groups are deleted.');
    return;
  }

  // finals N ...
  const N = Number(tokens[0]);
  if (Number.isInteger(N) && N > 0) {
    if (tokens.length === 1) {
      const [g, finalPtsArr] = await Promise.all([getFinalGroup(chatId, N), getFinalPoints(chatId)]);
      if (!g) { await ctx.reply(`Final ${N} not found.`); return; }
      const txt = formatFinalGroupsList([g], finalPointsToMap(finalPtsArr), { twoCols: false });
      await replyPre(ctx, txt);
      return;
    }
    const action = tokens[1].toLowerCase();

    // --- NEW: результаты игр на картах для финалов ---
    if (action === 'mapres') {
      const g = await getFinalGroup(chatId, N);
      if (!g) {
        await ctx.reply(`Final ${N} not found.`);
        return;
      }

      // /f N mapres — показать (публично)
      if (!tokens[2]) {
        const results = await listFinalMapResults(chatId, N);
        const text = formatMapResultsTable('Final', N, results);
        await replyPre(ctx, text);
        return;
      }

      const sub = (tokens[2] || '').toLowerCase();

      // /f N mapres delall — удалить результаты
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const existed = await listFinalMapResults(chatId, N);
        if (!existed.length) {
          await ctx.reply('Результаты по картам для этого финала не найдены.');
          return;
        }
        await deleteFinalMapResultsForGroup(chatId, N);
        await ctx.reply(`Все результаты по картам для Final ${N} удалены.`);
        return;
      }

      // Запись — только админы
      if (!(await requireAdminGuard(ctx))) return;

      if (tokens.length < 7) {
        await ctx.reply(
          'Некорректный формат.\n' +
          'Использование:\n' +
          '/f <N> mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...]'
        );
        return;
      }

      const mapInput = tokens[2];
      const datePart = tokens[3];
      const timePart = tokens[4];
      const playtime = tokens[5];
      const playersStr = tokens.slice(6).join(' ').trim();

      const dtStr = `${datePart} ${timePart}`;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart) || !/^\d{2}:\d{2}$/.test(timePart)) {
        await ctx.reply('Дата/время матча должны быть в формате YYYY-MM-DD HH:MM.');
        return;
      }
      if (!/^\d{1,2}:\d{2}$/.test(playtime)) {
        await ctx.reply('Время игры должно быть в формате MM:SS.');
        return;
      }

      const mapsArr = Array.isArray(g.maps) ? g.maps : [];
      const foundMapOrig = mapsArr.find(m => norm(m) === norm(mapInput));
      if (!foundMapOrig) {
        await ctx.reply(`Карта "${mapInput}" не назначена для Final ${N}.`);
        return;
      }

      const parsed = parseMapResultPlayers(playersStr);
      if (parsed.error) {
        await ctx.reply(parsed.error);
        return;
      }
      const players = parsed.players || [];
      if (!players.length) {
        await ctx.reply('Список игроков пуст.');
        return;
      }

      const finalPlayersMap = new Map((g.players || []).map(p => [p.nameNorm, p.nameOrig]));
      const missing = players.filter(p => !finalPlayersMap.has(p.nameNorm)).map(p => p.nameOrig);
      if (missing.length) {
        await ctx.reply(
          'Ошибка: следующие игроки отсутствуют в данном финале и результат не будет сохранён:\n' +
          missing.join(', ')
        );
        return;
      }

      const storedPlayers = players.map(p => ({
        ...p,
        nameOrig: finalPlayersMap.get(p.nameNorm) || p.nameOrig,
      }));

      const matchDateTimeIso = toMoscowIso(datePart, timePart);
      const matchTs = toUnixTsFromMoscow(datePart, timePart);
      if (matchTs == null) {
        await ctx.reply('Не удалось распарсить дату/время (MSK). Проверьте формат YYYY-MM-DD HH:MM.');
        return;
      }

      await upsertFinalMapResult(chatId, N, foundMapOrig, {
        matchDateTime: dtStr,
        matchDateTimeIso,
        matchTs,
        matchPlaytime: playtime,
        players: storedPlayers,
      });


      await ctx.reply(
        `Результат по карте "${foundMapOrig}" для Final ${N} записан/обновлён.\n` +
        'Проверить: /f ' + N + ' mapres'
      );
      return;
    }


    // /finals N demos [add]
    if (action === 'demos') {
      const sub2 = (tokens[2] || '').toLowerCase();
      if (sub2 === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const list = cleanListParam(tokens.slice(3).join(' '));
        const g = await getFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Final ${N} not found.`); return; }
        await upsertFinalGroup(chatId, N, { ...g, demos: list });
        await replyChunked(ctx, list.length ? `Final ${N} demos are set:\n${list.join('\n')}` : `Final ${N} demos cleared.`);
        return;
      }
      const g = await getFinalGroup(chatId, N);
      if (!g) { await ctx.reply(`Final ${N} not found.`); return; }
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (!ds.length) { await ctx.reply(`Final ${N} demos: (none)`); return; }
      await replyChunked(ctx, `Final ${N} demos:\n${ds.join('\n')}`);
      return;
    }

    if (action === 'screenshots') {
      const sub2 = (tokens[2] || '').toLowerCase();
      if (sub2 === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Final ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);

        const key = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(key, {
          chatId,
          userId: ctx.from.id,
          mode: 'final',
          groupId: N,
          runId,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
        });

        await ctx.reply(
          `Пришлите один или несколько скриншотов для Final ${N} (изображения JPG/PNG/WEBP/GIF).\n` +
          `Когда закончите — отправьте /done. Для отмены — /cancel.`
        );
        return;
      }
      if (sub2 === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Final ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);
        const res = await deleteScreenshotsForGroup(chatId, 'final', N, runId);
        await ctx.reply(`Deleted for Final ${N}: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
        return;
      }
      await showFinalScreenshots(ctx, chatId, N);
      return;
    }

    // /finals N time [value] — показать/задать строку времени (МСК, хранится как текст)
    if (action === 'time') {
      const value = tokens.slice(2).join(' ').trim();
      const g = await getFinalGroup(chatId, N);
      if (!g) { await ctx.reply(`Final ${N} not found.`); return; }

      if (!value) {
        await ctx.reply(`Final ${N} time: ${g.time ? g.time : '(not set)'}`);
      } else {
        if (!(await requireAdminGuard(ctx))) return;
        await upsertFinalGroup(chatId, N, { ...g, time: value });
        await ctx.reply(`Final ${N} time is set: ${value}`);
      }
      return;
    }



    if (action === 'addp' || action === 'delp') {
      await ctx.reply('Опция временно недоступна.');
      return;
    }

    await ctx.reply('Неизвестная опция. Используйте: demos | make | players | move | points | rating | algo | maxplayers | totalplayers | screenshots | news | delall или без параметров.');
    return;
  }

  await ctx.reply('Неизвестная опция /finals. Используйте /help.');
}


bot.command(['finals', 'f'], finalsHandler);


// ---- Finals stage rating (for superfinal algo=2 or viewing via /finals rating)
async function getFinalRating(chatId) {
  const doc = await colFinalRatings.findOne({ chatId });
  return doc?.players || [];
}
async function setFinalRating(chatId, players) {
  const rated = players.map((p, i) => ({ ...p, rank: i + 1 }));
  await colFinalRatings.updateOne(
    { chatId },
    { $set: { chatId, players: rated, updatedAt: new Date() } },
    { upsert: true }
  );
}
function formatFinalRatingList(players = []) {
  if (!players.length) return 'Finals rating: (none)';
  const sorted = players.slice().sort((a, b) => (a.rank ?? 9999) - (b.rank ?? 9999));
  const lines = sorted.map(p => `${p.rank}) ${p.nameOrig}`);
  return `Finals rating:\n${lines.join('\n')}`;
}

// ---- Superfinal groups storage
async function listSuperFinalGroups(chatId) {
  return colSuperFinalGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}
async function getSuperFinalGroup(chatId, groupId) {
  return colSuperFinalGroups.findOne({ chatId, groupId: Number(groupId) });
}
async function upsertSuperFinalGroup(chatId, groupId, data) {
  await colSuperFinalGroups.updateOne(
    { chatId, groupId: Number(groupId) },
    { $set: { chatId, groupId: Number(groupId), ...data } },
    { upsert: true }
  );
}
async function deleteAllSuperFinalGroups(chatId) {
  await colSuperFinalGroups.deleteMany({ chatId });
}

// ---- Superfinal 2-col formatter (карты вертикально)
function formatSuperFinalGroupsList(groups) {
  if (!groups.length) return 'Superfinal groups: (none)';

  function buildBlock(g) {
    const title = `Superfinal ${g.groupId}`;
    const timeLine = `Time: ${g.time ? g.time : '(not set)'}`;
    const ps = (g.players || []).slice().sort((a, b) =>
      (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' })
    );
    const playerLines = ps.length ? ps.map(p => p.nameOrig) : ['(empty)'];
    const maps = g.maps || [];
    const mapsLines = maps.length ? ['Maps:'].concat(maps.map(m => `- ${m}`)) : ['Maps: (none)'];
    return { title, timeLine, playerLines, mapsLines };
  }

  const blocks = groups.map(buildBlock);
  const lines = [];
  const GAP = '    ';

  for (let i = 0; i < blocks.length; i += 2) {
    const L = blocks[i];
    const R = blocks[i + 1] || { title: '', timeLine: '', playerLines: [], mapsLines: [] };

    const maxPlayers = Math.max(L.playerLines.length, R.playerLines.length);
    const maxMaps = Math.max(L.mapsLines.length, R.mapsLines.length);

    const leftLinesForWidth = [L.title, L.timeLine];
    for (let k = 0; k < maxPlayers; k++) leftLinesForWidth.push(L.playerLines[k] || '');
    for (let k = 0; k < maxMaps; k++) leftLinesForWidth.push(L.mapsLines[k] || '');
    const leftWidth = Math.max(...leftLinesForWidth.map(s => s.length), 0) + GAP.length;

    lines.push((L.title || '').padEnd(leftWidth, ' ') + (R.title || ''));
    lines.push((L.timeLine || '').padEnd(leftWidth, ' ') + (R.timeLine || ''));
    for (let k = 0; k < maxPlayers; k++) {
      const lp = L.playerLines[k] || '';
      const rp = R.playerLines[k] || '';
      lines.push(lp.padEnd(leftWidth, ' ') + rp);
    }
    for (let k = 0; k < maxMaps; k++) {
      const lm = L.mapsLines[k] || '';
      const rm = R.mapsLines[k] || '';
      lines.push(lm.padEnd(leftWidth, ' ') + rm);
    }
    if (i + 2 < blocks.length) lines.push('');
  }

  return lines.join('\n');
}

// ---- Superfinal stage rating (аналогично финальным рейтингам)
async function getSuperFinalRating(chatId) {
  const doc = await colSuperFinalRatings.findOne({ chatId });
  if (doc?.players?.length) {
    return doc.players;
  }
  if (doc?.rating?.length) {
    // Если сохранён старый формат, конвертируем его в новый
    const uniqueList = dedupByNorm(doc.rating);
    const rated = uniqueList.map((p, i) => ({ ...p, rank: i + 1 }));
    // Сохраняем конвертированный формат для будущего
    await colSuperFinalRatings.updateOne(
      { chatId },
      { $set: { chatId, players: rated, updatedAt: new Date() } },
      { upsert: true }
    );
    return rated;
  }
  return [];
}

async function setSuperFinalRating(chatId, players) {
  const rated = players.map((p, i) => ({ ...p, rank: i + 1 }));
  await colSuperFinalRatings.updateOne(
    { chatId },
    { $set: { chatId, players: rated, updatedAt: new Date() } },
    { upsert: true }
  );
}

function formatSuperFinalRatingList(players = []) {
  if (!players.length) return 'Superfinal rating: (none)';
  const sorted = players.slice().sort((a, b) => (a.rank ?? 9999) - (b.rank ?? 9999));
  const lines = sorted.map(p => `${p.rank}) ${p.nameOrig}`);
  return `Superfinal rating:\n${lines.join('\n')}`;
}


async function superfinalHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // Ветка: /superfinal N ... ------------>>>>>>>
  const N = Number(tokens[0]);
  if (Number.isInteger(N) && N > 0) {
    if (tokens.length === 1) {
      const g = await getSuperFinalGroup(chatId, N);
      if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }
      await replyPre(ctx, formatSuperFinalGroupsList([g]));
      return;
    }
    const action = tokens[1].toLowerCase();

    // --- NEW: результаты игр на картах для суперфиналов ---
    if (action === 'mapres') {
      const g = await getSuperFinalGroup(chatId, N);
      if (!g) {
        await ctx.reply(`Superfinal ${N} not found.`);
        return;
      }

      // /s N mapres — показать (публично)
      if (!tokens[2]) {
        const results = await listSuperFinalMapResults(chatId, N);
        const text = formatMapResultsTable('Superfinal', N, results);
        await replyPre(ctx, text);
        return;
      }

      const sub = (tokens[2] || '').toLowerCase();

      // /s N mapres delall
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const existed = await listSuperFinalMapResults(chatId, N);
        if (!existed.length) {
          await ctx.reply('Результаты по картам для этого суперфинала не найдены.');
          return;
        }
        await deleteSuperFinalMapResultsForGroup(chatId, N);
        await ctx.reply(`Все результаты по картам для Superfinal ${N} удалены.`);
        return;
      }

      // Запись — только админы
      if (!(await requireAdminGuard(ctx))) return;

      if (tokens.length < 7) {
        await ctx.reply(
          'Некорректный формат.\n' +
          'Использование:\n' +
          '/s <N> mapres <map> <YYYY-MM-DD> <HH:MM> <MM:SS> player1[frags,kills,eff,fph,dgiv,drec],player2[...]'
        );
        return;
      }

      const mapInput = tokens[2];
      const datePart = tokens[3];
      const timePart = tokens[4];
      const playtime = tokens[5];
      const playersStr = tokens.slice(6).join(' ').trim();

      const dtStr = `${datePart} ${timePart}`;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(datePart) || !/^\d{2}:\d{2}$/.test(timePart)) {
        await ctx.reply('Дата/время матча должны быть в формате YYYY-MM-DD HH:MM.');
        return;
      }
      if (!/^\d{1,2}:\d{2}$/.test(playtime)) {
        await ctx.reply('Время игры должно быть в формате MM:SS.');
        return;
      }

      const mapsArr = Array.isArray(g.maps) ? g.maps : [];
      const foundMapOrig = mapsArr.find(m => norm(m) === norm(mapInput));
      if (!foundMapOrig) {
        await ctx.reply(`Карта "${mapInput}" не назначена для Superfinal ${N}.`);
        return;
      }

      const parsed = parseMapResultPlayers(playersStr);
      if (parsed.error) {
        await ctx.reply(parsed.error);
        return;
      }
      const players = parsed.players || [];
      if (!players.length) {
        await ctx.reply('Список игроков пуст.');
        return;
      }

      const sfPlayersMap = new Map((g.players || []).map(p => [p.nameNorm, p.nameOrig]));
      const missing = players.filter(p => !sfPlayersMap.has(p.nameNorm)).map(p => p.nameOrig);
      if (missing.length) {
        await ctx.reply(
          'Ошибка: следующие игроки отсутствуют в данном суперфинале и результат не будет сохранён:\n' +
          missing.join(', ')
        );
        return;
      }

      const storedPlayers = players.map(p => ({
        ...p,
        nameOrig: sfPlayersMap.get(p.nameNorm) || p.nameOrig,
      }));

      const matchDateTimeIso = toMoscowIso(datePart, timePart);
      const matchTs = toUnixTsFromMoscow(datePart, timePart);
      if (matchTs == null) {
        await ctx.reply('Не удалось распарсить дату/время (MSK). Проверьте формат YYYY-MM-DD HH:MM.');
        return;
      }

      await upsertSuperFinalMapResult(chatId, N, foundMapOrig, {
        matchDateTime: dtStr,
        matchDateTimeIso,
        matchTs,
        matchPlaytime: playtime,
        players: storedPlayers,
      });


      await ctx.reply(
        `Результат по карте "${foundMapOrig}" для Superfinal ${N} записан/обновлён.\n` +
        'Проверить: /s ' + N + ' mapres'
      );
      return;
    }


    // /superfinal N demos [add]
    if (action === 'demos') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const list = cleanListParam(tokens.slice(3).join(' '));
        const g = await getSuperFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }
        await upsertSuperFinalGroup(chatId, N, { ...g, demos: list });
        await replyChunked(ctx, list.length ? `Superfinal ${N} demos are set:\n${list.join('\n')}` : `Superfinal ${N} demos cleared.`);
        return;
      }
      const g = await getSuperFinalGroup(chatId, N);
      if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (!ds.length) { await ctx.reply(`Superfinal ${N} demos: (none)`); return; }
      await replyChunked(ctx, `Superfinal ${N} demos:\n${ds.join('\n')}`);
      return;
    }

    if (action === 'screenshots') {
      const sub = (tokens[2] || '').toLowerCase();
      if (sub === 'add') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getSuperFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);

        const key = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(key, {
          chatId,
          userId: ctx.from.id,
          mode: 'superfinal',
          groupId: N,
          runId,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
        });

        await ctx.reply(
          `Пришлите один или несколько скриншотов для Superfinal ${N} (изображения JPG/PNG/WEBP/GIF).\n` +
          `Когда закончите — отправьте /done. Для отмены — /cancel.`
        );
        return;
      }
      if (sub === 'delall') {
        if (!(await requireAdminGuard(ctx))) return;
        const g = await getSuperFinalGroup(chatId, N);
        if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }
        const runId = getGroupRunIdFromDoc(g);
        const res = await deleteScreenshotsForGroup(chatId, 'superfinal', N, runId);
        await ctx.reply(`Deleted for Superfinal ${N}: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
        return;
      }
      await showSuperFinalScreenshots(ctx, chatId, N);
      return;
    }

    if (action === 'addp' || action === 'delp') {
      await ctx.reply('Опция временно недоступна.');
      return;
    }

    // /superfinal N time [value] — показать/задать строку времени (МСК, хранится как текст)
    if (action === 'time') {
      const value = tokens.slice(2).join(' ').trim();
      const g = await getSuperFinalGroup(chatId, N);
      if (!g) { await ctx.reply(`Superfinal ${N} not found.`); return; }

      if (!value) {
        await ctx.reply(`Superfinal ${N} time: ${g.time ? g.time : '(not set)'}`);
      } else {
        if (!(await requireAdminGuard(ctx))) return;
        await upsertSuperFinalGroup(chatId, N, { ...g, time: value });
        await ctx.reply(`Superfinal ${N} time is set: ${value}`);
      }
      return;
    }

    await ctx.reply('Неизвестная опция. Используйте: demos | screenshots | addp | delp или без параметров для просмотра группы.');
    return;
  }

  // Ветка: /superfinal N ... <<<<<<<<<<<<<<<<<<< КОНЕЦ

  // /superfinal — показать все суперфиналы
  if (!tokens.length) {
    const sfgs = await listSuperFinalGroups(chatId);
    await replyPre(ctx, formatSuperFinalGroupsList(sfgs));
    return;
  }

  const sub = tokens[0].toLowerCase();

  // /superfinal rating или /superfinal rating name1,name2,...
  if (sub === 'rating') {
    const rest = tokens.slice(1).join(' ').trim();
    if (!rest) {
      const rating = await getSuperFinalRating(chatId);
      await replyChunked(ctx, formatSuperFinalRatingList(rating));
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const list = dedupByNorm(cleanListParam(rest));
    if (!list.length) {
      await ctx.reply('Укажите игроков через запятую. Пример: /superfinal rating player1,player2');
      return;
    }
    await setSuperFinalRating(chatId, list);
    const rating = await getSuperFinalRating(chatId);
    await replyChunked(ctx, 'Superfinal rating is set (previous cleared).\n' + formatSuperFinalRatingList(rating));
    return;
  }


  // superfinal demos — по всем суперфиналам
  if (sub === 'demos') {
    const sfgs = await listSuperFinalGroups(chatId);
    const lines = [];
    let added = 0;
    for (const g of sfgs) {
      const ds = Array.isArray(g.demos) ? g.demos : [];
      if (ds.length) {
        lines.push(`Superfinal ${g.groupId}:`);
        for (const d of ds) lines.push(`- ${d}`);
        lines.push('');
        added++;
      }
    }
    if (!added) {
      await ctx.reply('Superfinal demos: (none)');
    } else {
      await replyChunked(ctx, lines.join('\n').trim());
    }
    return;
  }

  // superfinal make <C>
  if (sub === 'make') {
    if (!(await requireAdminGuard(ctx))) return;
    const C = Number(tokens[1]);
    if (!Number.isInteger(C) || C <= 0) {
      await ctx.reply('Использование: /superfinal make <C>. Пример: /superfinal make 2');
      return;
    }
    const res = await makeSuperFinals(chatId, C);
    if (res?.error) { await ctx.reply(`Ошибка: ${res.error}`); return; }
    await replyPre(ctx, formatSuperFinalGroupsList(res.groups || []));
    return;
  }

  // superfinal players
  if (sub === 'players') {
    const sfgs = await listSuperFinalGroups(chatId);
    if (!sfgs.length) { await ctx.reply('Players (superfinals): (none)'); return; }
    const seen = new Set();
    const all = [];
    for (const g of sfgs) for (const p of (g.players || [])) if (!seen.has(p.nameNorm)) { seen.add(p.nameNorm); all.push(p); }
    all.sort((a, b) => a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
    await replyChunked(ctx, `Players (superfinals): ${all.map(p => p.nameOrig).join(', ')}`);
    return;
  }

  // superfinal algo <1|2>
  if (sub === 'algo') {
    if (!(await requireAdminGuard(ctx))) return;
    const v = Number(tokens[1]);
    if (![1, 2].includes(v)) {
      await ctx.reply('Использование: /superfinal algo <1|2>');
      return;
    }
    await setChatSettings(chatId, { superfinalsAlgo: v });
    await ctx.reply(`Superfinals algo set to ${v}.`);
    return;
  }

  // superfinal maxplayers [N]
  if (sub === 'maxplayers') {
    if (!tokens[1]) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Superfinal maxPlayers: ${s.superfinalMaxPlayers}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const v = Number(tokens[1]);
    if (!Number.isInteger(v) || v <= 0) {
      await ctx.reply('Использование: /superfinal maxplayers <целое > 0>');
      return;
    }
    await setChatSettings(chatId, { superfinalMaxPlayers: v });
    await ctx.reply(`Superfinal maxPlayers set to ${v}.`);
    return;
  }

  // superfinal totalplayers [N|all|auto|0]
  if (sub === 'totalplayers') {
    if (!tokens[1]) {
      const s = await getChatSettings(chatId);
      await ctx.reply(`Superfinal totalPlayers: ${s.superfinalTotalPlayers ?? '(all available)'}${s.superfinalsAlgo === 2 ? ' (ignored when algo=2)' : ''}`);
      return;
    }
    if (!(await requireAdminGuard(ctx))) return;
    const valRaw = String(tokens[1]).toLowerCase();
    if (['all', 'auto', '0'].includes(valRaw)) {
      await setChatSettings(chatId, { superfinalTotalPlayers: null });
      await ctx.reply('Superfinal totalPlayers set to: all (auto).');
      return;
    }
    const v = Number(tokens[1]);
    if (!Number.isInteger(v) || v <= 0) {
      await ctx.reply('Использование: /superfinal totalplayers <целое > 0 | all|auto|0>');
      return;
    }
    await setChatSettings(chatId, { superfinalTotalPlayers: v });
    await ctx.reply(`Superfinal totalPlayers set to ${v}.`);
    return;
  }

  // superfinal screenshots [delall]
  if (sub === 'screenshots') {
    const op = (tokens[1] || '').toLowerCase();
    if (op === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const res = await deleteAllScreenshotsForScope(chatId, 'superfinal');
      await ctx.reply(`Screenshots deleted for superfinals: files=${res.deletedFiles}, sets=${res.deletedDocs}.`);
      return;
    }
    await showAllScopeScreenshots(ctx, chatId, 'superfinal');
    return;
  }

  // superfinal news [add <text> | delall]
  if (sub === 'news') {
    const op = (tokens[1] || '').toLowerCase();

    if (op === 'add') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const text = tokens.slice(2).join(' ').trim();
      if (!text) { await ctx.reply('Использование: /superfinal news add <текст новости>'); return; }
      const runId = await findLatestRunIdForScope(chatId, 'superfinal');
      if (!runId) { await ctx.reply('Нет текущих суперфинальных групп. Сначала сформируйте /superfinal make.'); return; }
      await addNews(chatId, 'superfinal', text, runId, { id: ctx.from.id, username: ctx.from.username });
      await ctx.reply('Новость суперфинала добавлена.');
      return;
    }

    if (op === 'edit') {
      //if (!(await requireAdminGuard(ctx))) return;
      if (!(await requireNewsGuard(ctx))) return;
      const idStr = tokens[2];
      const text = tokens.slice(3).join(' ').trim();
      if (!idStr || !text) { await ctx.reply('Использование: /superfinal news edit <id> <текст новости>'); return; }
      const res = await editNewsById(chatId, 'superfinal', idStr, text);
      if (res.error) { await ctx.reply(res.error); return; }
      await ctx.reply('Новость суперфинала обновлена.');
      return;
    }

    if (op === 'delall') {
      if (!(await requireAdminGuard(ctx))) return;
      const n = await delAllNews(chatId, 'superfinal');
      await ctx.reply(`Удалено новостей (superfinal): ${n}.`);
      return;
    }

    const runId = await findLatestRunIdForScope(chatId, 'superfinal');
    if (!runId) { await ctx.reply('Нет текущих суперфинальных групп.'); return; }
    const news = await listNews(chatId, 'superfinal', runId);
    await replyChunked(ctx, formatNewsList(news));
    return;
  }


  // superfinal points ...
  if (sub === 'points') {
    const tail = tokens.slice(1).join(' ').trim();

    if (!tail) {
      const ptsArr = await getSuperFinalPoints(chatId);
      if (!ptsArr.length) {
        await ctx.reply('Superfinal points: (none)');
        return;
      }
      const sorted = ptsArr
        .slice()
        .sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));
      const lines = ['Superfinal points:'].concat(sorted.map(p => `${p.nameOrig}[${p.pts}]`));
      await replyChunked(ctx, lines.join('\n'));
      return;
    }

    if (!(await requireAdminGuard(ctx))) return;
    const parsed = parsePointsList(tail);
    if (parsed.error) {
      await ctx.reply(parsed.error);
      return;
    }
    if (!parsed.length) {
      await ctx.reply('Укажите игроков и очки. Пример: /superfinal points David[10],aid[12]');
      return;
    }

    const sfgs = await listSuperFinalGroups(chatId);
    const present = new Map();
    for (const g of sfgs) for (const p of (g.players || [])) present.set(p.nameNorm, p.nameOrig);
    const missing = parsed.filter(p => !present.has(p.nameNorm)).map(p => p.nameOrig);
    if (missing.length) {
      await ctx.reply(`Не найдены в суперфиналах: ${missing.join(', ')}`);
      return;
    }

    const toSave = parsed.map(p => ({ nameNorm: p.nameNorm, nameOrig: present.get(p.nameNorm), pts: p.pts }));
    await setSuperFinalPoints(chatId, toSave);
    await ctx.reply('Superfinal points are set.');
    return;
  }

  // superfinal delall — удалить все суперфинальные группы
  if (sub === 'delall') {
    if (!(await requireAdminGuard(ctx))) return;
    await deleteAllSuperFinalGroups(chatId);
    await ctx.reply('All superfinal groups are deleted.');
    return;
  }

  await ctx.reply('Неизвестная опция /superfinal. Используйте /help.');
}

bot.command(['superfinal', 's'], superfinalHandler);

// ПОЛНАЯ ЗАМЕНА achievementsHandler — добавлены: /ac N type <type> и /ac N edit <kind> (name|logo|desc|all)
async function achievementsHandler(ctx) {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);

  // Без аргументов — показать все (публично)
  if (!tokens.length) {
    await showAllAchievements(ctx, chatId);
    return;
  }

  // /ac add <name> — админы ИЛИ роль Achievements
  if (tokens[0].toLowerCase() === 'add') {
    if (!(await requireAchievementsGuard(ctx))) return;
    const name = tokens.slice(1).join(' ').trim();
    if (!name) { await ctx.reply('Использование: /achievements add <name>'); return; }

    // >>> NEW: создаём ЧЕРНОВИК до старта сессии приёма изображений <<<
    const dKey = achvKey(chatId, ctx.from.id);
    achvDrafts.set(dKey, {
      chatId,
      userId: ctx.from.id,
      name,          // важно: имя попадёт в addAchievement() из bot.on('text')
      image: null,   // сюда /done положит sess.tempImage
      startedAt: Date.now(),
      expiresAt: Date.now() + 30 * 60 * 1000,
    });

    // стартуем приём изображения
    const key = ssKey(chatId, ctx.from.id);
    screenshotSessions.set(key, {
      chatId,
      userId: ctx.from.id,
      mode: 'achv',
      groupId: 0,
      runId: 'achievements', // формально не используется
      startedAt: Date.now(),
      expiresAt: Date.now() + 10 * 60 * 1000,
      count: 0,
      achvName: name,        // опционально; основной источник — achvDrafts
      tempImage: null,       // сюда handleIncomingScreenshot положит последнюю картинку
    });

    await ctx.reply(
      `Добавление ачивки: "${name}".\n` +
      'Пришлите изображение (JPG/PNG/WEBP/GIF). Можно отправить одно или несколько — сохранится последнее.\n' +
      'Завершить отправку изображения: /done. Отмена: /cancel.'
    );
    return;
  }

  // /ac delall — админы ИЛИ роль Achievements
  if (tokens[0].toLowerCase() === 'delall') {
    if (!(await requireAchievementsGuard(ctx))) return;
    const n = await deleteAllAchievements(chatId);
    await ctx.reply(`Удалено ачивок: ${n}.`);
    return;
  }

  // /ac <N> ...
  const N = Number(tokens[0]);
  if (Number.isInteger(N) && N > 0) {
    if (tokens.length === 1) {
      const a = await getAchievement(chatId, N);
      if (!a) { await ctx.reply(`Achievement ${N} not found.`); return; }
      await showAchievement(ctx, a);
      return;
    }

    const action = tokens[1].toLowerCase();

    // /ac N type <type> — установить тип (achievement|perc)
    if (action === 'type') {
      if (!(await requireAchievementsGuard(ctx))) return;
      const t = (tokens[2] || '').trim().toLowerCase();
      if (!isValidAchievementType(t)) {
        await ctx.reply('Некорректный тип. Доступны: achievement, perc');
        return;
      }
      const a = await getAchievement(chatId, N);
      if (!a) { await ctx.reply(`Achievement ${N} not found.`); return; }
      await colAchievements.updateOne(
        { chatId, idx: Number(N) },
        { $set: { type: normalizeAchievementType(t), updatedAt: new Date() } }
      );
      const updated = await getAchievement(chatId, N);
      await ctx.reply(`Тип ачивки ${N} установлен: ${updated.type}`);
      await showAchievement(ctx, updated);
      return;
    }

    // /ac N edit <kind> — редактирование атрибутов (name|logo|desc|all)
    if (action === 'edit') {
      if (!(await requireAchievementsGuard(ctx))) return;
      const kind = (tokens[2] || '').toLowerCase();
      if (!['name', 'logo', 'desc', 'all'].includes(kind)) {
        await ctx.reply('Использование: /achievements N edit <name|logo|desc|all>');
        return;
      }
      const a = await getAchievement(chatId, N);
      if (!a) { await ctx.reply(`Achievement ${N} not found.`); return; }

      const k = achvKey(chatId, ctx.from.id);
      if (kind === 'name') {
        achvEditSessions.set(k, { chatId, userId: ctx.from.id, idx: N, mode: 'name' });
        await ctx.reply(`Отправьте новое название для ачивки ${N} одним сообщением (или /cancel для отмены).`);
        return;
      }
      if (kind === 'desc') {
        achvEditSessions.set(k, { chatId, userId: ctx.from.id, idx: N, mode: 'desc' });
        await ctx.reply(`Отправьте новое описание для ачивки ${N} одним сообщением (или /cancel для отмены).`);
        return;
      }
      if (kind === 'logo') {
        const sKey = ssKey(chatId, ctx.from.id);
        screenshotSessions.set(sKey, {
          chatId,
          userId: ctx.from.id,
          mode: 'achv_logo',
          achvIdx: N,
          startedAt: Date.now(),
          expiresAt: Date.now() + 10 * 60 * 1000,
          count: 0,
          tempImage: null,
        });
        await ctx.reply(
          `Пришлите новое изображение для ачивки ${N} (JPG/PNG/WEBP/GIF).\n` +
          `Когда закончите — отправьте /done. Для отмены — /cancel.`
        );
        return;
      }
      if (kind === 'all') {
        // Шаг 1/3 — имя
        achvEditSessions.set(k, { chatId, userId: ctx.from.id, idx: N, mode: 'name', chain: true });
        await ctx.reply(
          `Шаг 1/3. Отправьте новое название для ачивки ${N} одним сообщением (или /cancel для отмены).`
        );
        return;
      }
    }

    // /ac N addp <player> — владелец
    if (action === 'addp') {
      if (!(await requireAchievementsGuard(ctx))) return;
      const playerRaw = tokens.slice(2).join(' ').trim();
      if (!playerRaw) {
        await ctx.reply('Использование: /achievements N addp <player>. Пример: /ac 1 addp aid');
        return;
      }
      const a = await getAchievement(chatId, N);
      if (!a) { await ctx.reply(`Achievement ${N} not found.`); return; }
      const player = { nameOrig: playerRaw, nameNorm: norm(playerRaw) };
      await colAchievements.updateOne(
        { chatId, idx: Number(N) },
        { $set: { player, updatedAt: new Date() } }
      );
      const updated = await getAchievement(chatId, N);
      await ctx.reply(`Владение ачивкой ${N} назначено: ${player.nameOrig}.`);
      await showAchievement(ctx, updated);
      return;
    }

    // /ac N del — удалить
    if (action === 'del') {
      if (!(await requireAchievementsGuard(ctx))) return;
      const res = await deleteAchievement(chatId, N);
      if (res.notFound) { await ctx.reply(`Achievement ${N} not found.`); return; }
      await ctx.reply(`Achievement ${N} удалена. Нумерация пересчитана.`);
      return;
    }

    await ctx.reply('Неизвестная опция /achievements. Используйте: add | delall | <N> | <N> type <type> | <N> edit <kind> | <N> addp <player> | <N> del | (пусто для списка)');
    return;
  }

  await ctx.reply('Неизвестная опция /achievements. Используйте: add | delall | <N> | <N> type <type> | <N> edit <kind> | <N> addp <player> | <N> del | (пусто для списка)');
}

bot.command(['achievements', 'ac'], achievementsHandler);


// /delall — удалить все SG, игровые группы, карты и финалы в текущем чате
bot.command('delall', async ctx => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  if (!(await requireAdminGuard(ctx))) return;

  const chatId = getEffectiveChatId(ctx);
  try {
    // Удалим файлы скриншотов + доки по всем scope
    const shots = await deleteAllScreenshotsForChat(chatId);

    // Удалим новости всех scope
    const newsDeleted = await colNews.deleteMany({ chatId });

    // внутри обработчика bot.command('delall', ...)
    await Promise.all([
      delAllSkillGroups(chatId),
      deleteAllGameGroups(chatId),
      deleteGroupMapResultsForChat(chatId),
      delAllMaps(chatId),
      deleteAllFinalGroups(chatId),
      deleteAllSuperFinalGroups(chatId),
      deleteAllAchievements(chatId),
      // NEW:
      deleteAllCustomGroups(chatId),
      colCustomPoints.deleteMany({ chatId }),
      clearWaitingPlayers(chatId),
      colRatings.deleteOne({ chatId }),
      colGroupPoints.deleteOne({ chatId }),
      colFinalPoints.deleteOne({ chatId }),
      setChatSettings(chatId, {
        tournamentName: null, tournamentSite: null, tournamentDesc: null, tournamentLogo: null,
      }),
      colFinalRatings.deleteOne({ chatId }),
    ]);


    await ctx.reply(
      'All skill-groups, game groups, maps, finals, superfinals removed for this chat.\n' +
      `Screenshots deleted: files=${shots.deletedFiles}, sets=${shots.deletedDocs}.\n` +
      `News deleted: ${newsDeleted.deletedCount || 0}.`
    );
  } catch (e) {
    console.error('delall error', e);
    await ctx.reply('Error during deletion. Please try again.');
  }
});

// Завершить приём скриншотов
// ПОЛНАЯ ЗАМЕНА: /done
bot.command('done', async ctx => {
  purgeExpiredSessions();
  purgeExpiredNews2Sessions();
  if (!requireGroupContext(ctx)) return;

  const chatId = getEffectiveChatId(ctx);
  const { userId, username, name, display } = getUserIdentity(ctx);

  // === FEEDBACK: приоритетная обработка, если есть активная фидбэк-сессия ===
  {
    const fKey = fbKey(chatId, userId);
    const fSess = feedbackSessions.get(fKey);
    if (fSess) {
      try {
        const textCombined = (fSess.buffer || []).join('\n\n').trim();
        feedbackSessions.delete(fKey);

        if (!textCombined) {
          await ctx.reply('Пустой фидбэк не сохранён.');
          return;
        }

        const now = new Date();
        const msk = formatMoscowDateTime2(now);

        if (fSess.mode === 'add') {
          await colFeedback.insertOne({
            chatId,
            userId,
            username,
            name,
            displayName: display,
            text: textCombined,
            createdAt: now,
            createdAtMSK: msk,
            updatedAt: now,
            updatedAtMSK: msk,
          });
          await ctx.reply('Ваш фидбэк сохранён. Спасибо!');
          return; // НЕ продолжаем к screenshotSessions
        } else if (fSess.mode === 'edit') {
          const upd = await colFeedback.updateOne(
            { chatId, userId },
            {
              $set: {
                username,
                name,
                displayName: display,
                text: textCombined,
                updatedAt: now,
                updatedAtMSK: msk,
              }
            }
          );
          if (upd.matchedCount === 0) {
            await ctx.reply('Ранее сохранённый фидбэк не найден. Используйте /feedback add.');
          } else {
            await ctx.reply('Ваш фидбэк обновлён.');
          }
          return; // НЕ продолжаем к screenshotSessions
        } else {
          await ctx.reply('Неизвестный режим фидбэка. Попробуйте /feedback add.');
          return;
        }
      } catch (e) {
        console.error('/done feedback error', e);
        try { await ctx.reply('Ошибка сохранения фидбэка. Попробуйте ещё раз.'); } catch (_) { }
        return;
      }
    }
  }

  // === TEAMS: завершение создания/редактирования команды ===
  {
    const tKey = teamSessionKey(userId);
    const tSess = teamSessions.get(tKey);
    if (tSess) {
      if (tSess.step !== 'confirm') {
        await ctx.reply('Сначала завершите ввод всех данных по команде (название, описание и список участников).');
        return;
      }

      const now = new Date();
      const from = ctx.from || {};

      if (tSess.mode === 'add') {
        const doc = {
          name: tSess.name,
          nameNorm: tSess.nameNorm,
          description: tSess.description || '',
          memberIds: tSess.memberIds || [],
          memberNicks: tSess.memberNicks || [],
          createdByTelegramId: userId,
          createdByUsername: from.username || null,
          createdAt: now,
          updatedAt: now,
        };

        const res = await colTeams.insertOne(doc);
        const saved = { ...doc, _id: res.insertedId };

        teamSessions.delete(tKey);
        await ctx.reply('Игровая команда создана.');
        await replyPre(ctx, formatTeamForDisplay(saved));
        return;
      }

      if (tSess.mode === 'edit') {
        const upd = await colTeams.updateOne(
          { _id: new ObjectId(tSess.teamId) },
          {
            $set: {
              name: tSess.name,
              nameNorm: tSess.nameNorm,
              description: tSess.description || '',
              memberIds: tSess.memberIds || [],
              memberNicks: tSess.memberNicks || [],
              updatedAt: now,
            },
          },
        );

        teamSessions.delete(tKey);

        if (!upd.matchedCount) {
          await ctx.reply('Команда не найдена. Попробуйте ещё раз выполнить /teams edit.');
          return;
        }

        const fresh = await findTeamById(tSess.teamId);
        await ctx.reply('Игровая команда обновлена.');
        if (fresh) {
          await replyPre(ctx, formatTeamForDisplay(fresh));
        }
        return;
      }

      teamSessions.delete(tKey);
      await ctx.reply('Неизвестный режим обработки команды. Попробуйте заново.');
      return;
    }
  }


  const sKey = ssKey(chatId, userId);
  const sess = screenshotSessions.get(sKey);

  if (!sess) {
    await ctx.reply('Нет активной операции. Нечего завершать.');
    return;
  }

  try {
    const count = sess.count || 0;

    // 1) Новая ачивка: переносим временное изображение в черновик и просим описание
    if (sess.mode === 'achv') {
      const dKey = achvKey(chatId, userId);
      const draft = achvDrafts.get(dKey);
      if (!draft) {
        screenshotSessions.delete(sKey);
        await ctx.reply('Черновик ачивки не найден. Начните заново командой /ac add <название>.');
        return;
      }
      if (!sess.tempImage) {
        await ctx.reply('Изображение не получено. Пришлите картинку или /cancel.');
        return;
      }
      draft.image = sess.tempImage;
      achvDrafts.set(dKey, draft);

      screenshotSessions.delete(sKey);
      await ctx.reply('Картинка сохранена. Отправьте описание ачивки одним сообщением (или /cancel).');
      return;
    }

    // 2) Редактирование логотипа существующей ачивки
    if (sess.mode === 'achv_logo') {
      const idx = Number(sess.achvIdx);
      if (!Number.isInteger(idx) || idx <= 0) {
        screenshotSessions.delete(sKey);
        await ctx.reply('Индекс ачивки не распознан. Сессия завершена.');
        return;
      }
      if (!sess.tempImage) {
        await ctx.reply('Изображение не получено. Пришлите картинку или /cancel.');
        return;
      }

      const ach = await getAchievement(chatId, idx);
      if (!ach) {
        screenshotSessions.delete(sKey);
        await ctx.reply(`Achievement ${idx} not found.`);
        return;
      }

      // удалить старый файл, если был
      if (ach.image?.relPath) {
        try { await fs.promises.unlink(path.join(SCREENSHOTS_DIR, ach.image.relPath)); } catch (_) { }
      }

      await colAchievements.updateOne(
        { chatId, idx },
        { $set: { image: sess.tempImage, updatedAt: new Date() } }
      );

      screenshotSessions.delete(sKey);

      if (sess.chain) {
        const eKey = achvKey(chatId, userId);
        achvEditSessions.set(eKey, { chatId, userId, idx, mode: 'desc', chain: true });
        await ctx.reply('Шаг 3/3. Отправьте новое описание ачивки одним сообщением (или /cancel).');
      } else {
        await ctx.reply('Картинка ачивки обновлена.');
      }
      return;
    }

    // 3) Замена картинки у новости после /news edit
    if (sess.mode === 'news_edit_img') {
      screenshotSessions.delete(sKey);
      await ctx.reply('Сессия замены картинки закрыта без изменений. Если нужно заменить — отправьте новое изображение после /news edit.');
      return;
    }

    // 4) Упрощённая новость (/t news2 add) — новость сохраняется при отправке изображения
    if (sess.mode === 'news2_one') {
      screenshotSessions.delete(sKey);
      await ctx.reply('Сессия закрыта. Новость создаётся в момент отправки изображения (картинка + подпись).');
      return;
    }

    // 5) Обычные сценарии: группы/финалы/кастомы/логотип турнира — просто закрываем сессию
    if (['group', 'final', 'superfinal', 'custom', 'tlogo'].includes(sess.mode)) {
      screenshotSessions.delete(sKey);
      await ctx.reply(`Готово. Принято файлов: ${count}.`);
      return;
    }

    // 6) Фолбэк — завершить любую другую сессию
    screenshotSessions.delete(sKey);
    await ctx.reply('Сессия завершена.');
  } catch (e) {
    console.error('/done error', e);
    try { await ctx.reply('Ошибка завершения. Попробуйте ещё раз.'); } catch (_) { }
  }
});


// Отмена приёма
bot.command('cancel', async ctx => {
  purgeExpiredSessions();
  purgeExpiredNews2Sessions();
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from.id;

  // === FEEDBACK: если есть активная фидбэк-сессия, отменяем её и выходим ===
  {
    const fKey = fbKey(chatId, userId);
    if (feedbackSessions.has(fKey)) {
      feedbackSessions.delete(fKey);
      await ctx.reply('Ввод фидбэка отменён.');
      return;
    }
  }

  // === SIGNUP WIZARD (/register): если активен мастер регистрации заявки, отменяем его ===
  {
    const sKey = signupWizardKey(chatId, userId);
    if (signupWizardSessions.has(sKey)) {
      signupWizardSessions.delete(sKey);

      // заодно сбросим разрешение на /register, если оно было выдано
      const rKey = signupRegisterKey(chatId, userId);
      if (signupRegisterAllowed.has(rKey)) {
        signupRegisterAllowed.delete(rKey);
      }

      await ctx.reply('Регистрация для подачи заявки на турнир отменена.');
      return;
    }
  }

  // === SIGNUP REGISTER FLAG: если есть только разрешение на /register после /signup ===
  {
    const rKey = signupRegisterKey(chatId, userId);
    if (signupRegisterAllowed.has(rKey)) {
      signupRegisterAllowed.delete(rKey);
      await ctx.reply('Подача заявки на турнир отменена.');
      return;
    }
  }

  // === USER PROFILE: если есть активная сессия профиля, отменяем её и выходим ===
  {
    const uKey = userProfileKey(userId);
    if (userProfileSessions.has(uKey)) {
      userProfileSessions.delete(uKey);
      await ctx.reply('Настройка вашего профиля отменена.');
      return;
    }
  }

  // === TEAMS: если есть активная сессия команды/выбора команды, отменяем ===
  {
    const tKey = teamSessionKey(userId);
    const tsKey = teamSelectKey(userId);
    const stKey = signupTeamSelectKey(chatId, userId);
    let had = false;

    if (teamSessions.has(tKey)) {
      teamSessions.delete(tKey);
      had = true;
    }
    if (teamSelectSessions.has(tsKey)) {
      teamSelectSessions.delete(tsKey);
      had = true;
    }
    if (signupTeamSelectSessions.has(stKey)) {
      signupTeamSelectSessions.delete(stKey);
      had = true;
    }

    if (had) {
      await ctx.reply('Операция с игровой командой отменена.');
      return;
    }
  }

  // === SCREENSHOTS & ACHIEVEMENTS: отмена загрузки скриншотов/логотипов/ачивок ===
  const sKey = ssKey(chatId, userId);
  const sess = screenshotSessions.get(sKey);
  let touched = false;

  // Удалим временный файл для режимов ачивок (создание и редактирование логотипа)
  if (sess && (sess.mode === 'achv' || sess.mode === 'achv_logo') && sess.tempImage?.relPath) {
    try { await fs.promises.unlink(path.join(SCREENSHOTS_DIR, sess.tempImage.relPath)); } catch (_) { }
    touched = true;
  }
  if (sess) {
    screenshotSessions.delete(sKey);
    touched = true;
  }

  // Удаляем черновик описания новой ачивки (если был)
  const akey = achvKey(chatId, userId);
  const draft = achvDrafts.get(akey);
  if (draft) {
    if (draft.image?.relPath) {
      try { await fs.promises.unlink(path.join(SCREENSHOTS_DIR, draft.image.relPath)); } catch (_) { }
    }
    achvDrafts.delete(akey);
    touched = true;
  }

  // Удаляем активную сессию редактирования (name/desc)
  if (achvEditSessions.has(akey)) {
    achvEditSessions.delete(akey);
    touched = true;
  }

  if (touched) {
    await ctx.reply('Отменено. Уже сохранённые файлы оставлены без изменений (кроме временных файлов ачивок).');
    return;
  }

  // Ничего отменять не пришлось — скажем пользователю, что активных операций нет
  await ctx.reply('Отмена: активных операций, связанных с заявками, профилем, командами или загрузками, не найдено.');
});



// Приём фото/документов в активной сессии
bot.on(['photo', 'document'], async ctx => {
  try {
    await handleIncomingScreenshot(ctx);
  } catch (e) {
    console.error('handleIncomingScreenshot error', e);
    try { await ctx.reply('Ошибка сохранения скриншота. Попробуйте ещё раз.'); } catch (_) { }
  }
});

bot.command('news', async ctx => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const [subRaw, idStr, ...rest] = args.split(' ').filter(Boolean);
  const sub = (subRaw || '').toLowerCase();
  const newText = rest.join(' ').trim();

  if (!sub) {
    await ctx.reply('Использование:\n/news del <id>\n/news edit <id> <текст>');
    return;
  }

  // Удаление новости (как было)
  if (sub === 'del') {
    //if (!(await requireAdminGuard(ctx))) return;
    if (!(await requireNewsGuard(ctx))) return;
    if (!idStr) { await ctx.reply('Использование: /news del <id>'); return; }
    const res = await deleteNewsById(chatId, idStr);
    if (res.error) { await ctx.reply(res.error); return; }
    await ctx.reply('Новость удалена.');
    return;
  }

  // Редактирование текста + предложение заменить картинку
  if (sub === 'edit') {
    //if (!(await requireAdminGuard(ctx))) return;
    if (!(await requireNewsGuard(ctx))) return;
    if (!idStr || !newText) { await ctx.reply('Использование: /news edit <id> <текст>'); return; }

    let _id;
    try { _id = new ObjectId(idStr); } catch (_) {
      await ctx.reply('Некорректный ID новости.');
      return;
    }

    // Ищем новость именно в текущем чате
    const doc = await colNews.findOne({ _id, chatId });
    if (!doc) {
      await ctx.reply('Новость с таким ID не найдена в текущем чате.');
      return;
    }

    // Обновляем текст
    await colNews.updateOne(
      { _id, chatId },
      { $set: { text: newText, updatedAt: new Date() } }
    );

    // Если есть картинка — предлагаем заменить её
    if (doc.news_img_file_name) {
      const sKey = ssKey(chatId, ctx.from.id);
      screenshotSessions.set(sKey, {
        chatId,
        userId: ctx.from.id,
        mode: 'news_edit_img',          // <<< ключевой режим
        newsId: String(_id),
        oldImgFileName: doc.news_img_file_name,
        startedAt: Date.now(),
        expiresAt: Date.now() + 10 * 60 * 1000,
        count: 0,
      });

      await ctx.reply(
        'Текст новости обновлён.\n' +
        'Хотите заменить картинку? Пришлите новое изображение (JPG/PNG/WEBP/GIF) ОДНИМ сообщением.\n' +
        'Чтобы оставить текущую — отправьте /skip. Отмена — /cancel.'
      );
    } else {
      await ctx.reply('Текст новости обновлён. У этой новости нет картинки.');
    }
    return;
  }

  await ctx.reply('Неизвестная опция /news. Используйте: del | edit');
});


// НОВАЯ КОМАНДА: /setid <ID> — установить/показать/сбросить целевой chatId для текущего пользователя
// /setid <ID> — установить/показать/сбросить целевой chatId для ТЕКУЩЕГО чата (контекст чата+пользователь)
bot.command('setid', async ctx => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }

  const args = parseCommandArgs(ctx.message.text || '').trim();
  const srcChatId = Number(ctx.chat?.id);
  const key = makeOverrideKey(ctx);

  // Показываем текущее состояние
  if (!args) {
    const cur = key ? userTargetChat.get(key) : null;
    const effective = getEffectiveChatId(ctx);
    await ctx.reply(
      `Текущий чат: #${srcChatId}\n` +
      `Целевой для ЭТОГО чата и вашего пользователя: ${Number.isInteger(cur) ? ('#' + cur) : '(не задан)'}\n` +
      `Фактически используемый для данных: #${effective}\n\n` +
      `Подсказки:\n` +
      `- Установить: /setid <числовой_id>\n` +
      `- Сбросить: /setid clear`
    );
    return;
  }

  // /setid clear — сбросить переопределение для ТЕКУЩЕГО чата (только ваш ключ)
  if (args.toLowerCase() === 'clear' || args.toLowerCase() === 'reset') {
    if (key) userTargetChat.delete(key);
    await ctx.reply('Целевой chatId для ЭТОГО чата сброшен. Теперь используются данные текущего чата.');
    return;
  }

  // /setid <ID> — установить переопределение для ТЕКУЩЕГО чата (только для вашего ключа)
  const idStr = args.replace(/\s+/g, '');
  if (!/^-?\d+$/.test(idStr)) {
    await ctx.reply('Укажите числовой ID чата/канала. Пример: /setid -1001234567890');
    return;
  }

  const tgt = Number(idStr);
  if (key) userTargetChat.set(key, tgt);

  try {
    const s = await getChatSettings(tgt);
    await ctx.reply(
      `Целевой chatId установлен для чата #${srcChatId}: #${tgt}.\n` +
      `Будут использоваться права и данные чата #${tgt}.\n` +
      `Турнир (в целевом чате): ${s.tournamentName || '(not set)'}`
    );
  } catch (_) {
    await ctx.reply(
      `Целевой chatId установлен для чата #${srcChatId}: #${tgt}.\n` +
      `Внимание: данных по этому чату может не быть до первого сохранения настроек.`
    );
  }
});

bot.command('text', async ctx => {
  purgeExpiredNews2Sessions();
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from.id;

  const key = n2Key(chatId, userId);
  const draft = news2Sessions.get(key);
  if (!draft) {
    await ctx.reply('Эта команда используется внутри конструктора новости. Запустите: /t news2 add');
    return;
  }

  draft.waiting = 'text';
  draft.expiresAt = Date.now() + 30 * 60 * 1000;
  news2Sessions.set(key, draft);
  await ctx.reply('Отправьте текст одним сообщением (или /cancel для отмены).');
});

bot.command('image', async ctx => {
  purgeExpiredSessions();
  purgeExpiredNews2Sessions();
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const userId = ctx.from.id;

  const key = n2Key(chatId, userId);
  const draft = news2Sessions.get(key);
  if (!draft) {
    await ctx.reply('Эта команда используется внутри конструктора новости. Запустите: /t news2 add');
    return;
  }

  const sKey = ssKey(chatId, userId);
  screenshotSessions.set(sKey, {
    chatId,
    userId,
    mode: 'news2',
    startedAt: Date.now(),
    expiresAt: Date.now() + 10 * 60 * 1000,
    count: 0,
    n2key: key,
  });

  await ctx.reply('Пришлите картинку (JPG/PNG/WEBP/GIF). Когда закончите — /done. Для отмены — /cancel.');
});

bot.command('skip', async ctx => {
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const sKey = ssKey(chatId, ctx.from.id);
  const sess = screenshotSessions.get(sKey);

  if (sess && sess.mode === 'news_edit_img') {
    screenshotSessions.delete(sKey);
    await ctx.reply('Ок, оставляем старую картинку новости.');
    return;
  }

  await ctx.reply('Нечего пропускать.');
});

// === FEEDBACK COMMANDS ===
bot.command(['feedback', 'fb'], async (ctx) => {
  const text = (ctx.message?.text || '').trim();
  const chatId = getEffectiveChatId(ctx); // учитывает targetChatId
  const { userId, username, name, display } = getUserIdentity(ctx);

  const lower = text.toLowerCase();
  const isAdd = lower.startsWith('/feedback add') || lower.startsWith('/fb add');
  const isEdit = lower.startsWith('/feedback edit') || lower.startsWith('/fb edit');
  const isDel = lower === '/feedback del' || lower === '/fb del';

  if (!isAdd && !isEdit && !isDel) {
    await ctx.reply([
      'Использование:',
      '/feedback (/fb) add , далее ввод нового фидбэка в поле с сообщением в Телеграм (поддерживаются длинные тексты с авторазбивкой на несколько сообщений (лимиты Телеграм), после завершения ввода нужно ввести команду /done для подтверждения или сделать отмену командой /cancel)',
      '/feedback (/fb) edit — отредактировать ваш фидбэк',
      '/feedback (/fb) del — удалить ваш фидбэк',
      '',
      'Алиас: /fb ...',
    ].join('\n'));
    return;
  }

  if (isDel) {
    const existing = await colFeedback.findOne({ chatId, userId });
    if (!existing) {
      await ctx.reply('У вас ещё нет фидбэка в этом чате. Используйте /feedback add <текст> чтобы добавить.');
      return;
    }
    await colFeedback.deleteOne({ chatId, userId });
    await ctx.reply('Ваш фидбэк удалён.');
    return;
  }

  // add / edit
  const argText = extractCommandArgText(ctx, ['feedback', 'fb']);
  const key = feedbackSessionKey(chatId, userId);

  if (isAdd) {
    // запретить повторное добавление если уже есть
    const exists = await colFeedback.findOne({ chatId, userId });
    if (exists) {
      await ctx.reply('Вы уже оставляли фидбэк в этом чате. Используйте /feedback edit <текст> для редактирования или /feedback del для удаления.');
      return;
    }
    feedbackSessions.set(key, { mode: 'add', buffer: argText ? [argText] : [] });
    await ctx.reply('Режим ввода фидбэка запущен. Отправляйте текст сообщениями. Когда закончите — отправьте /done. Для отмены — /cancel.');
    return;
  }

  if (isEdit) {
    const existing = await colFeedback.findOne({ chatId, userId });
    if (!existing) {
      await ctx.reply('У вас ещё нет фидбэка в этом чате. Используйте /feedback add <текст> чтобы добавить.');
      return;
    }
    feedbackSessions.set(key, { mode: 'edit', buffer: argText ? [argText] : [] });
    await ctx.reply('Режим редактирования фидбэка запущен. Отправляйте текст сообщениями. Когда закончите — отправьте /done. Для отмены — /cancel.');
    return;
  }
});

bot.command('lock', async (ctx) => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }

  const chatId = getEffectiveChatId(ctx);

  // Админ-проверка, НО игнорируем глобальный lock (саму блокировку можно делать всегда)
  if (!(await requireAdminGuard(ctx, { ignoreLock: true }))) return;

  if (await isTournamentLocked(chatId)) {
    await ctx.reply('Турнир уже заблокирован для изменений (locked = true).');
    return;
  }

  await setTournamentLocked(chatId, true);
  await ctx.reply(
    'Турнир заблокирован для изменений.\n' +
    'Любые команды, изменяющие данные турнира, теперь недоступны.\n' +
    'Для разблокировки используйте /unlock.'
  );
});

bot.command('unlock', async (ctx) => {
  if (!requireGroupContext(ctx)) {
    await ctx.reply('Эта команда доступна только в чатах (группы или личные), не в каналах.');
    return;
  }

  const chatId = getEffectiveChatId(ctx);

  // Разблокировка тоже должна работать даже при locked = true
  if (!(await requireAdminGuard(ctx, { ignoreLock: true }))) return;

  if (!(await isTournamentLocked(chatId))) {
    await ctx.reply('Турнир уже разблокирован (locked = false).');
    return;
  }

  await setTournamentLocked(chatId, false);
  await ctx.reply(
    'Турнир разблокирован. Изменения снова разрешены.\n' +
    'Чтобы снова запретить любые изменения, используйте /lock.'
  );
});

// REGISTRATION (настройки и админ-операции с заявками)
async function registrationHandler(ctx) {
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);
  const sub = (tokens[0] || '').toLowerCase();

  const reg = await getRegistrationSettings(chatId);

  // /reg  — показать сводку настроек + краткую инфу о заявках
  if (!sub) {
    const [playerSignups, teamSignups] = await Promise.all([
      colSignups.find({ chatId, kind: 'player' }).sort({ createdAt: 1 }).toArray(),
      colSignups.find({ chatId, kind: 'team' }).sort({ createdAt: 1 }).toArray(),
    ]);

    const parts = [];
    parts.push(formatRegistrationSettingsForDisplay(reg));
    parts.push('');
    parts.push(formatPlayerSignupsList(playerSignups));
    parts.push('');
    parts.push(formatTeamSignupsList(teamSignups));

    await replyPre(ctx, parts.join('\n\n'));
    return;
  }

  // /reg enabled <true/false>
  if (sub === 'enabled') {
    if (!(await requireAdminGuard(ctx))) return;
    const valRaw = (tokens[1] || '').toLowerCase();
    if (!['true', 'false'].includes(valRaw)) {
      await ctx.reply('Использование: /reg enabled true|false');
      return;
    }
    const flag = valRaw === 'true';
    const patch = {
      registrationEnabled: flag,
    };
    const now = new Date();
    if (flag) {
      patch.registrationOpenedAt = now;
      patch.registrationClosedAt = null;
    } else {
      patch.registrationClosedAt = now;
    }
    await updateRegistrationSettings(chatId, patch);
    await ctx.reply(`Регистрация на турнир теперь ${flag ? 'открыта' : 'закрыта'}.`);
    return;
  }

  // /reg maxplayers <count>
  if (sub === 'maxplayers') {
    if (!(await requireAdminGuard(ctx))) return;
    const raw = tokens[1];
    const n = Number(raw);
    if (!Number.isInteger(n) || n <= 0) {
      await ctx.reply('Использование: /reg maxplayers <целое положительное число>');
      return;
    }
    await updateRegistrationSettings(chatId, { maxPlayers: n });
    await ctx.reply(`Максимальное количество игроков для регистрации установлено: ${n}`);
    return;
  }

  // /reg deadline  (просмотр) и /reg deadline <YYYY-MM-DD HH:mm> (установка)
  if (sub === 'deadline') {
    // /reg deadline без параметров — публичный просмотр
    if (tokens.length === 1) {
      const text =
        'Текущий дедлайн регистрации: ' +
        (reg.deadline ? formatMoscowDateTime(reg.deadline) : '(not set)');
      await ctx.reply(text);
      return;
    }

    // /reg deadline <value> — только админы
    if (!(await requireAdminGuard(ctx))) return;

    const raw = tokens.slice(1).join(' ');
    const dt = parseMoscowDateTime(raw);
    if (!dt) {
      await ctx.reply('Не удалось распарсить дату. Формат: YYYY-MM-DD HH:mm, пример: 2025-12-31 23:00');
      return;
    }

    await updateRegistrationSettings(chatId, { deadline: dt });
    await ctx.reply('Дедлайн регистрации установлен: ' + formatMoscowDateTime(dt));
    return;
  }

  // --- Ниже команды, которые работают с записями в colSignups ---

  // /reg addp <nick> — добавить игрока (админ, только FFA/1v1)
  if (sub === 'addp') {
    if (!(await requireAdminGuard(ctx))) return;

    if (!reg.tournamentType || !['FFA', '1v1'].includes(reg.tournamentType)) {
      await ctx.reply('Регистрация игроков доступна только для турниров типа FFA или 1v1.');
      return;
    }

    const nickRaw = tokens.slice(1).join(' ').trim();
    if (!nickRaw) {
      await ctx.reply('Использование: /reg addp <nick>');
      return;
    }

    // Ищем игрока в users
    const user = await findUserByNick(nickRaw);
    if (!user) {
      await ctx.reply(
        'Игрок с таким ником не найден среди зарегистрированных пользователей.\n' +
        'Попросите игрока зарегистрироваться через /u add и используйте /signup add, либо /reg addp после регистрации.'
      );
      return;
    }

    // Проверка, нет ли уже заявки для этого игрока
    const existing = await colSignups.findOne({
      chatId,
      kind: 'player',
      userId: user._id,
    });
    if (existing) {
      await ctx.reply(
        'У этого игрока уже есть заявка на участие в турнире.\n\n' +
        'ID заявки: ' + existing.signupId
      );
      return;
    }

    const now = new Date();
    const signupId = generateSignupId();
    await colSignups.insertOne({
      chatId,
      signupId,
      kind: 'player',
      userId: user._id,
      playerNick: user.nick,
      playerNickNorm: user.nickNorm,
      confirmed: false,
      createdByTelegramId: ctx.from.id,
      createdByUsername: ctx.from.username || null,
      createdAt: now,
      updatedAt: now,
    });

    await ctx.reply(`Игрок ${user.nick} добавлен в регистрацию турнира. ID заявки: ${signupId}`);
    return;
  }

  // /reg addt <teamName> — добавить команду (админ, только TDM)
  if (sub === 'addt') {
    if (!(await requireAdminGuard(ctx))) return;

    if (reg.tournamentType !== 'TDM') {
      await ctx.reply('Регистрация команд доступна только для турниров типа TDM.');
      return;
    }

    const nameRaw = tokens.slice(1).join(' ').trim();
    if (!nameRaw) {
      await ctx.reply('Использование: /reg addt <team name>');
      return;
    }

    const nameNorm = norm(nameRaw);
    const team = await colTeams.findOne({ nameNorm });
    if (!team) {
      await ctx.reply(
        'Команда с таким названием не найдена среди зарегистрированных команд.\n' +
        'Сначала создайте команду через /teams add.'
      );
      return;
    }

    const existing = await colSignups.findOne({
      chatId,
      kind: 'team',
      teamId: team._id,
    });
    if (existing) {
      await ctx.reply(
        'Эта команда уже подала заявку на участие в турнире.\n\n' +
        'ID заявки: ' + existing.signupId
      );
      return;
    }

    const now = new Date();
    const signupId = generateSignupId();
    await colSignups.insertOne({
      chatId,
      signupId,
      kind: 'team',
      teamId: team._id,
      teamName: team.name,
      teamNameNorm: team.nameNorm,
      teamMembers: team.memberNicks || [],
      confirmed: false,
      createdByTelegramId: ctx.from.id,
      createdByUsername: ctx.from.username || null,
      createdAt: now,
      updatedAt: now,
    });

    await ctx.reply(`Команда "${team.name}" добавлена в регистрацию турнира. ID заявки: ${signupId}`);
    return;
  }

  // /reg delp <id> — удалить заявку игрока
  if (sub === 'delp') {
    if (!(await requireAdminGuard(ctx))) return;
    const id = tokens[1];
    if (!id) {
      await ctx.reply('Использование: /reg delp <id>');
      return;
    }
    const res = await colSignups.deleteOne({ chatId, kind: 'player', signupId: id });
    if (!res.deletedCount) {
      await ctx.reply('Заявка игрока с таким ID не найдена для этого турнира.');
      return;
    }
    await ctx.reply('Заявка игрока удалена.');
    return;
  }

  // /reg delt <id> — удалить заявку команды
  if (sub === 'delt') {
    if (!(await requireAdminGuard(ctx))) return;
    const id = tokens[1];
    if (!id) {
      await ctx.reply('Использование: /reg delt <id>');
      return;
    }
    const res = await colSignups.deleteOne({ chatId, kind: 'team', signupId: id });
    if (!res.deletedCount) {
      await ctx.reply('Заявка команды с таким ID не найдена для этого турнира.');
      return;
    }
    await ctx.reply('Заявка команды удалена.');
    return;
  }

  // /reg players <id> status <true/false>  ИЛИ /reg players — список
  if (sub === 'players') {
    if (tokens.length === 1) {
      const players = await colSignups.find({ chatId, kind: 'player' }).sort({ createdAt: 1 }).toArray();
      await replyPre(ctx, formatPlayerSignupsList(players));
      return;
    }

    if (!(await requireAdminGuard(ctx))) return;
    const id = tokens[1];
    const key = (tokens[2] || '').toLowerCase();
    const valRaw = (tokens[3] || '').toLowerCase();
    if (key !== 'status' || !['true', 'false'].includes(valRaw)) {
      await ctx.reply('Использование: /reg players <id> status <true|false>');
      return;
    }
    const flag = valRaw === 'true';
    const res = await colSignups.updateOne(
      { chatId, kind: 'player', signupId: id },
      { $set: { confirmed: flag, updatedAt: new Date() } },
    );
    if (!res.matchedCount) {
      await ctx.reply('Заявка игрока с таким ID не найдена.');
      return;
    }
    await ctx.reply(`Статус заявки игрока обновлён: ${flag ? 'подтверждена' : 'ожидает подтверждения'}.`);
    return;
  }

  // /reg teams <id> status <true/false>  ИЛИ /reg teams — список
  if (sub === 'teams') {
    if (tokens.length === 1) {
      const teams = await colSignups.find({ chatId, kind: 'team' }).sort({ createdAt: 1 }).toArray();
      await replyPre(ctx, formatTeamSignupsList(teams));
      return;
    }

    if (!(await requireAdminGuard(ctx))) return;
    const id = tokens[1];
    const key = (tokens[2] || '').toLowerCase();
    const valRaw = (tokens[3] || '').toLowerCase();
    if (key !== 'status' || !['true', 'false'].includes(valRaw)) {
      await ctx.reply('Использование: /reg teams <id> status <true|false>');
      return;
    }
    const flag = valRaw === 'true';
    const res = await colSignups.updateOne(
      { chatId, kind: 'team', signupId: id },
      { $set: { confirmed: flag, updatedAt: new Date() } },
    );
    if (!res.matchedCount) {
      await ctx.reply('Заявка команды с таким ID не найдена.');
      return;
    }
    await ctx.reply(`Статус заявки команды обновлён: ${flag ? 'подтверждена' : 'ожидает подтверждения'}.`);
    return;
  }

  await ctx.reply('Неизвестная подкоманда /reg. Доступные: enabled, maxplayers, deadline, addp, addt, delp, delt, players, teams.');
}

bot.command(['registration', 'reg'], registrationHandler);

// REGISTER — мастер регистрации игрока / команды с последующей подачей заявки
async function registerForSignupHandler(ctx) {
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);

  if (!ctx.from) {
    await ctx.reply('Не удалось определить пользователя.');
    return;
  }

  const userId = ctx.from.id;
  const reg = await getRegistrationSettings(chatId);

  if (!reg.tournamentType) {
    await ctx.reply('Тип турнира пока не задан. Обратитесь к администратору (/t type).');
    return;
  }

  // Проверяем, был ли перед этим /signup, который выдал право вызвать /register
  const rKey = signupRegisterKey(chatId, userId);
  const allow = signupRegisterAllowed.get(rKey);
  if (!allow) {
    await ctx.reply(
      'Команда /register используется только после /signup, когда бот сообщает о необходимости регистрации для подачи заявки на турнир.'
    );
    return;
  }
  // Одноразовый допуск — сразу удаляем
  signupRegisterAllowed.delete(rKey);

  const me = await findUserByTelegramId(userId);

  // FFA / 1v1 — если игрок уже есть, заявка подаётся через /signup; иначе запускаем мастер регистрации
  if (['FFA', '1v1'].includes(reg.tournamentType)) {
    if (me) {
      await ctx.reply(
        'Вы уже зарегистрированы как игрок.\n' +
        'Для подачи заявки используйте команду /signup.'
      );
      return;
    }

    const sKey = signupWizardKey(chatId, userId);
    signupWizardSessions.set(sKey, {
      chatId,
      userId,
      tournamentType: reg.tournamentType,
      step: 'player_nick',
      mode: 'ffa',
      playerNick: null,
      playerNickNorm: null,
      playerBio: null,
      playerId: null, // заполним после insert
    });

    await ctx.reply(
      'Регистрация профиля игрока.\n\n' +
      'Введите желаемый ник (игровое имя).\n\n' +
      'Отмена — /cancel.'
    );
    return;
  }

  // TDM — регистрируем игрока (если нужно) и создаём команду, затем подаём заявку
  if (reg.tournamentType === 'TDM') {
    // Если игрок уже есть
    if (me) {
      const teams = await colTeams.find({ memberIds: me._id }).toArray();

      if (teams.length > 0) {
        // У человека уже есть команда — логичнее использовать /signup
        await ctx.reply(
          'Вы уже зарегистрированы как игрок и состоите хотя бы в одной команде.\n' +
          'Для подачи заявки используйте команду /signup.'
        );
        return;
      }

      // Игрок есть, команд нет — сразу переходим к созданию команды
      const sKey = signupWizardKey(chatId, userId);
      signupWizardSessions.set(sKey, {
        chatId,
        userId,
        tournamentType: reg.tournamentType,
        step: 'team_name',
        mode: 'tdm',
        playerNick: me.nick,
        playerNickNorm: me.nickNorm,
        playerBio: me.bio || '',
        playerId: me._id,
        teamName: null,
        teamNameNorm: null,
        teamDesc: null,
      });

      await ctx.reply(
        'Добавление игровой команды.\n\n' +
        'Шаг 1. Отправьте название команды.\n\n' +
        'Отмена — /cancel.'
      );
      return;
    }

    // Игрока ещё нет — сначала регистрируем игрока, затем команду
    const sKey = signupWizardKey(chatId, userId);
    signupWizardSessions.set(sKey, {
      chatId,
      userId,
      tournamentType: reg.tournamentType,
      step: 'player_nick',
      mode: 'tdm',
      playerNick: null,
      playerNickNorm: null,
      playerBio: null,
      playerId: null,
      teamName: null,
      teamNameNorm: null,
      teamDesc: null,
    });

    await ctx.reply(
      'Регистрация профиля игрока для TDM.\n\n' +
      'Введите желаемый ник (игровое имя).\n\n' +
      'Отмена — /cancel.'
    );
    return;
  }

  await ctx.reply('Неизвестный тип турнира. Обратитесь к администратору.');
}

bot.command('register', registerForSignupHandler);


// SIGNUP (публичные заявки от игроков / команд)
// /signup или /sup — основной сценарий подачи заявки
async function signupHandler(ctx) {
  if (!requireGroupContext(ctx)) return;
  const chatId = getEffectiveChatId(ctx);
  const args = parseCommandArgs(ctx.message.text || '');
  const tokens = args.split(' ').filter(Boolean);
  const sub = (tokens[0] || '').toLowerCase();

  if (!ctx.from) {
    await ctx.reply('Не удалось определить пользователя.');
    return;
  }

  const userId = ctx.from.id;
  const reg = await getRegistrationSettings(chatId);

  const now = new Date();
  const registrationOpen =
    reg.registrationEnabled &&
    (!reg.deadline || now.getTime() <= new Date(reg.deadline).getTime());

  // /signup или /sup без подкоманд
  if (!sub) {
    if (!reg.tournamentType) {
      await ctx.reply('Тип турнира пока не задан. Обратитесь к администратору (/t type).');
      return;
    }

    const me = await findUserByTelegramId(userId);

    // --- FFA / 1v1 ---
    if (['FFA', '1v1'].includes(reg.tournamentType)) {
      if (!me) {
        // Разрешаем использовать /register ровно один раз для этого чата и пользователя
        const rKey = signupRegisterKey(chatId, userId);
        signupRegisterAllowed.set(rKey, {
          chatId,
          userId,
          tournamentType: reg.tournamentType,
          createdAt: Date.now(),
        });

        await ctx.reply(
          'Для подачи заявки требуется регистрация игрока.\n\n' +
          'Если хотите зарегистрироваться и подать заявку, используйте команду /register.\n' +
          'Для отмены — /cancel.'
        );
        return;
      }

      // Ищем уже существующую заявку
      const existing = await colSignups.findOne({
        chatId,
        kind: 'player',
        userId: me._id,
      });

      if (existing) {
        // Уже подавал заявку — показываем расширенную информацию
        const lines = [];
        lines.push('Вы уже подали заявку на участие в этом турнире.');
        lines.push('');
        lines.push(`Игрок: ${existing.playerNick || me.nick || '(без имени)'}`);
        if (existing.createdAt) {
          lines.push(`Дата подачи: ${formatMoscowDateTime2(existing.createdAt)}`);
        }
        lines.push(
          `Статус: ${existing.confirmed ? 'одобрена' : 'ожидает подтверждения администратором'}`
        );
        lines.push('');
        lines.push('Детали заявки:');
        lines.push('');
        lines.push(formatPlayerSignupsList([existing]));
        await replyPre(ctx, lines.join('\n'));
        return;
      }

      // Заявки ещё нет — создаём новую
      if (!registrationOpen) {
        await ctx.reply(
          'Сейчас регистрация на турнир закрыта либо истёк дедлайн.\n' +
          'Уточните актуальные настройки у администратора (/reg).'
        );
        return;
      }

      await createPlayerSignupAndReply(ctx, chatId, reg, me);
      return;
    }

    // --- TDM ---
    if (reg.tournamentType === 'TDM') {
      if (!me) {
        const rKey = signupRegisterKey(chatId, userId);
        signupRegisterAllowed.set(rKey, {
          chatId,
          userId,
          tournamentType: reg.tournamentType,
          createdAt: Date.now(),
        });

        await ctx.reply(
          'Для подачи заявки требуется регистрация игрока и принадлежность к команде.\n\n' +
          'Если хотите зарегистрироваться и создать команду, используйте команду /register.\n' +
          'Для отмены — /cancel.'
        );
        return;
      }

      const teams = await colTeams
        .find({ memberIds: me._id })
        .sort({ nameNorm: 1 })
        .toArray();

      if (!teams.length) {
        const rKey = signupRegisterKey(chatId, userId);
        signupRegisterAllowed.set(rKey, {
          chatId,
          userId,
          tournamentType: reg.tournamentType,
          createdAt: Date.now(),
        });

        await ctx.reply(
          'Вы пока не состоите ни в одной игровой командe.\n\n' +
          'Если хотите зарегистрироваться и создать команду, используйте команду /register.\n' +
          'Для отмены — /cancel.'
        );
        return;
      }

      // Проверим, не подана ли уже заявка от какой-либо команды, где он состоит
      const teamIds = teams.map(t => t._id);
      const existing = await colSignups.findOne({
        chatId,
        kind: 'team',
        teamId: { $in: teamIds },
      });

      if (existing) {
        const lines = [];
        lines.push('Вы уже подали заявку на турнир как участник одной из команд.');
        lines.push('');
        lines.push(`Команда: ${existing.teamName || '(без названия)'}`);
        if (existing.createdAt) {
          lines.push(`Дата подачи: ${formatMoscowDateTime2(existing.createdAt)}`);
        }
        lines.push(
          `Статус: ${existing.confirmed ? 'одобрена' : 'ожидает подтверждения администратором'}`
        );
        lines.push('');
        lines.push('Детали заявки:');
        lines.push('');
        lines.push(formatTeamSignupsList([existing]));
        await replyPre(ctx, lines.join('\n'));
        return;
      }

      if (!registrationOpen) {
        await ctx.reply(
          'Сейчас регистрация на турнир закрыта либо истёк дедлайн.\n' +
          'Уточните актуальные настройки у администратора (/reg).'
        );
        return;
      }

      // Если команда одна — сразу создаём заявку
      if (teams.length === 1) {
        await createTeamSignupAndReply(ctx, chatId, reg, teams[0]);
        return;
      }

      // Если команд больше одной — запускаем существующий механизм выбора команды
      const stKey = signupTeamSelectKey(chatId, userId);
      signupTeamSelectSessions.set(stKey, {
        chatId,
        userId,
        teams,
        startedAt: Date.now(),
      });

      const listText = formatTeamsListForDisplay(teams);
      await replyPre(
        ctx,
        listText +
        '\nУкажите номер команды, которую нужно зарегистрировать на турнир (1, 2, ...).\n' +
        'Для отмены используйте /cancel.'
      );
      return;
    }

    await ctx.reply('Неизвестный тип турнира. Обратитесь к администратору (/t type).');
    return;
  }

  // /signup del — отозвать свою заявку (оставляем существующую логику)
  if (sub === 'del') {
    const me = await findUserByTelegramId(userId);
    if (!me) {
      await ctx.reply('Вы ещё не зарегистрированы как игрок (/u add).');
      return;
    }

    if (!reg.tournamentType) {
      await ctx.reply('Тип турнира пока не задан. Обратитесь к администратору (/t type).');
      return;
    }

    // FFA / 1v1 — удаляем заявку игрока
    if (['FFA', '1v1'].includes(reg.tournamentType)) {
      const res = await colSignups.deleteOne({
        chatId,
        kind: 'player',
        userId: me._id,
      });
      if (!res.deletedCount) {
        await ctx.reply('Активной заявки игрока на этот турнир не найдено.');
        return;
      }
      await ctx.reply('Ваша заявка на участие в турнире отозвана.');
      return;
    }

    // TDM — удаляем заявку команды, в которой он состоит
    if (reg.tournamentType === 'TDM') {
      const teams = await colTeams.find({ memberIds: me._id }).toArray();
      if (!teams.length) {
        await ctx.reply('Вы не состоите ни в одной команде, зарегистрированной на этот турнир.');
        return;
      }

      const teamIds = teams.map(t => t._id);
      const res = await colSignups.deleteOne({
        chatId,
        kind: 'team',
        teamId: { $in: teamIds },
      });
      if (!res.deletedCount) {
        await ctx.reply('Для ваших команд не найдено активных заявок на этот турнир.');
        return;
      }
      await ctx.reply('Заявка одной из ваших команд на участие в турнире отозвана.');
      return;
    }

    await ctx.reply('Неизвестный тип турнира. Обратитесь к администратору.');
    return;
  }

  await ctx.reply('Неизвестная подкоманда /signup. Доступна: del (отозвать заявку).');
}

// Привязка команд
bot.command(['signup', 'sup'], signupHandler);

// Хелпер: создать заявку игрока и вывести подробный текст
async function createPlayerSignupAndReply(ctx, chatId, reg, userDoc) {
  const now = new Date();
  const signupId = generateSignupId();

  await colSignups.insertOne({
    chatId,
    signupId,
    kind: 'player',
    userId: userDoc._id,
    playerNick: userDoc.nick,
    playerNickNorm: userDoc.nickNorm,
    confirmed: false,
    createdByTelegramId: ctx.from.id,
    createdByUsername: ctx.from.username || null,
    createdAt: now,
    updatedAt: now,
  });

  const lines = [];
  lines.push('Ваша заявка на участие в турнире принята!');
  lines.push('');
  if (ctx.chat?.title) {
    lines.push(`Турнир: ${ctx.chat.title}`);
  }
  lines.push(`Игрок: ${userDoc.nick || '(без имени)'}`);
  lines.push(`ID заявки: ${signupId}`);
  lines.push(`Время: ${formatMoscowDateTime2(now)}`);
  lines.push('Статус: ожидает подтверждения администратором турнира');
  await replyPre(ctx, lines.join('\n'));
}

// Хелпер: создать заявку команды и вывести подробный текст
async function createTeamSignupAndReply(ctx, chatId, reg, teamDoc) {
  const now = new Date();
  const signupId = generateSignupId();

  await colSignups.insertOne({
    chatId,
    signupId,
    kind: 'team',
    teamId: teamDoc._id,
    teamName: teamDoc.name,
    teamNameNorm: teamDoc.nameNorm,
    teamMembers: teamDoc.memberNicks || [],
    confirmed: false,
    createdByTelegramId: ctx.from.id,
    createdByUsername: ctx.from.username || null,
    createdAt: now,
    updatedAt: now,
  });

  const lines = [];
  lines.push('Заявка команды на участие в турнире принята!');
  lines.push('');
  if (ctx.chat?.title) {
    lines.push(`Турнир: ${ctx.chat.title}`);
  }
  lines.push(`Команда: ${teamDoc.name}`);
  lines.push(`ID заявки: ${signupId}`);
  lines.push(`Время: ${formatMoscowDateTime2(now)}`);
  lines.push('Статус: ожидает подтверждения администратором турнира');
  await replyPre(ctx, lines.join('\n'));
}


// SetMyCommands (basic)
bot.telegram.setMyCommands([
  { command: 'help', description: 'Справка' },
  { command: 'info', description: 'Текущие настройки' },
  { command: 'admin', description: 'Управление администраторами' },
  { command: 'skillgroup', description: 'Скилл-группы' },
  { command: 'map', description: 'Карты' },
  { command: 'groups', description: 'Игровые группы' },
  { command: 'finals', description: 'Финальные группы' },
  { command: 'f', description: 'Финальные группы (краткая команда)' },
  { command: 'superfinal', description: 'Суперфинальные группы' },
  { command: 's', description: 'Суперфинал (краткая команда)' },
  // NEW:
  { command: 'custom', description: 'Произвольные (доп.) группы' },
  { command: 'c', description: 'Произвольные группы (краткая команда)' },
  { command: 'delall', description: 'Полный сброс настроек чата' },
  { command: 'whoami', description: 'Показать свой ID' },
  { command: 'chatid', description: 'Показать ID чата/темы' },
  { command: 'achievements', description: 'Ачивки' },
  { command: 'ac', description: 'Ачивки (краткая команда)' },
  { command: 'users', description: 'Профиль игрока (ник + описание)' },
  { command: 'teams', description: 'Игровые команды (создание/просмотр)' },
  { command: 'registration', description: 'Регистрация на турнир (настройки)' },
  { command: 'signup', description: 'Подать заявку на участие в турнире' },
]).catch(() => { });


// Mongo + launch
(async () => {
  const client = new MongoClient(MONGODB_URI, { ignoreUndefined: true });
  await client.connect();
  db = client.db(); // default DB from URI
  colChats = db.collection('chats');
  colAdmins = db.collection('chat_admins');
  colSkillGroups = db.collection('skill_groups');
  colMaps = db.collection('maps');
  colGameGroups = db.collection('game_groups');
  colCounters = db.collection('counters');
  colUserIndex = db.collection('user_index');
  colFinalGroups = db.collection('final_groups');
  colWaitingPlayers = db.collection('waiting_players');
  colRatings = db.collection('player_ratings');
  colGroupPoints = db.collection('group_points');
  colFinalPoints = db.collection('final_points');
  colScreenshots = db.collection('screenshots');
  colFinalRatings = db.collection('final_ratings');
  colSuperFinalGroups = db.collection('super_final_groups');
  colNews = db.collection('news');
  colUsers = db.collection('users'); // NEW
  colTeams = db.collection('teams'); // NEW
  colCustomGroups = db.collection('custom_groups');
  colCustomPoints = db.collection('custom_points');
  colAchievements = db.collection('achievements');
  colRoles = db.collection('roles');
  colGroupResults = db.collection('group_results');
  colFinalResults = db.collection('final_results');
  colSuperFinalRatings = db.collection('super_final_ratings');
  colSuperFinalResults = db.collection('superfinal_results');
  colFeedback = db.collection('feedback');
  // НОВОЕ:
  colRegistrationSettings = db.collection('registration_settings');
  colSignups = db.collection('signups');

  // Indexes
  await Promise.all([
    colAdmins.createIndex({ chatId: 1 }, { unique: true }),
    colSkillGroups.createIndex({ chatId: 1, groupNumber: 1 }, { unique: true }),
    colMaps.createIndex({ chatId: 1, nameNorm: 1 }, { unique: true }),
    colGameGroups.createIndex({ chatId: 1, groupId: 1 }, { unique: true }),
    colFinalGroups.createIndex({ chatId: 1, groupId: 1 }, { unique: true }),
    colCounters.createIndex({ chatId: 1, key: 1 }, { unique: true }),
    colUserIndex.createIndex({ userId: 1 }, { unique: true }),
    colUserIndex.createIndex({ username: 1 }),
    colWaitingPlayers.createIndex({ chatId: 1 }, { unique: true }),
    colRatings.createIndex({ chatId: 1 }, { unique: true }),
    colGroupPoints.createIndex({ chatId: 1 }, { unique: true }),
    colFinalPoints.createIndex({ chatId: 1 }, { unique: true }),
    colScreenshots.createIndex({ chatId: 1, scope: 1, groupId: 1, groupRunId: 1 }, { unique: true }),
    colFinalRatings.createIndex({ chatId: 1 }, { unique: true }),
    colSuperFinalGroups.createIndex({ chatId: 1, groupId: 1 }, { unique: true }),
    colNews.createIndex({ chatId: 1, scope: 1, groupRunId: 1, createdAt: -1 }),
    colCustomGroups.createIndex({ chatId: 1, groupId: 1 }, { unique: true }), // NEW
    colCustomPoints.createIndex({ chatId: 1, groupId: 1 }, { unique: true }), // NEW
    colAchievements.createIndex({ chatId: 1, idx: 1 }, { unique: true }),
    colRoles.createIndex({ chatId: 1, userId: 1 }, { unique: true }),
    colGroupResults.createIndex({ chatId: 1, groupId: 1, matchTs: 1 }),
    colFinalResults.createIndex({ chatId: 1, groupId: 1, matchTs: 1 }),
    colSuperFinalResults.createIndex({ chatId: 1, groupId: 1, matchTs: 1 }),
    colFeedback.createIndex({ chatId: 1, userId: 1 }, { unique: true }), // у каждого пользователя по одному фидбэку на чат
    colFeedback.createIndex({ chatId: 1, createdAt: -1 }),
    // --- Users (глобальные профили по Telegram ID) ---
    colUsers.createIndex({ telegramId: 1 }, { unique: true }),
    colUsers.createIndex({ nickNorm: 1 }, { unique: true }),
    colUsers.createIndex({ username: 1 }),
    // --- Teams (глобальные игровые команды) ---
    colTeams.createIndex({ nameNorm: 1 }, { unique: true }),
    colTeams.createIndex({ memberIds: 1 }), // поиск команд по участнику
    // --- Registration settings ---
    colRegistrationSettings.createIndex({ chatId: 1 }, { unique: true }),
    // --- Signups (заявки) ---
    colSignups.createIndex({ chatId: 1, signupId: 1 }, { unique: true }),
    colSignups.createIndex({ chatId: 1, kind: 1 }),
    colSignups.createIndex({ chatId: 1, userId: 1 }),
    colSignups.createIndex({ chatId: 1, teamId: 1 }),
  ]);

  console.log('Connected to MongoDB. Starting bot...');
  await bot.launch();
  console.log('Bot started.');
})().catch(err => {
  console.error('Startup error:', err);
  process.exit(1);
});

// Graceful stop
process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
