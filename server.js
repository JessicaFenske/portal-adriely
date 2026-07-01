const http = require('http');
const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');

const PORT = process.env.PORT || 80;
const API_KEY = '1765E369255C44601A45DEE600DA89AB520BF12B23904DF127344DD91E3A31EAE2EFDF4862A9F31757FE84FE842076258347E9DE1AF9E28C3BC719ED7782F286';
const API_HOST = 'public-api2.ploomes.com';
const RD_PUBLIC_TOKEN = '00bbd955e27e47c643cab874adf517a5'; // RD Marketing token publico (envio de conversoes)
const RD_PRIVATE_TOKEN = 'd0dd9d50d65ab0efefa3687ec6af3bc2'; // RD Marketing token privado (API legada)
const RD_CLIENT_ID = '893969';
const CACHE_DIR = '/tmp/portal-cache';
const CACHE_VERSION = 15; // v15: deals agora trazem Contact.Origin como fallback de origem

// ==================== Auth config ====================
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');
if (!process.env.SESSION_SECRET) {
    console.warn('[auth] WARNING: SESSION_SECRET nao configurado, gerando aleatorio. Logins serao invalidados a cada restart. Configure SESSION_SECRET no Render.');
}
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const RESEND_FROM = process.env.RESEND_FROM || 'Portal Lincros <onboarding@resend.dev>';
// Gemini foi descontinuado em favor de Claude (Anthropic, sem treinamento de modelos).
// A variável GEMINI_API_KEY foi removida — toda chamada de IA passa por ANTHROPIC_API_KEY.
const ANTHROPIC_API_KEY = (process.env.ANTHROPIC_API_KEY || '').trim().replace(/^["'`]+|["'`]+$/g, '').trim();
const ANTHROPIC_MODEL = (process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5').trim();
const APP_BASE_URL = process.env.APP_BASE_URL || 'https://portal-de-oportunidades.onrender.com';
const SESSION_DAYS = 30;
const RESET_TOKEN_TTL_MS = 60 * 60 * 1000; // 1 hora
const USERS_PATH = path.join(__dirname, 'users.json');
// Remove aspas/whitespace de env vars (paste do .env ou snippet costuma vir com aspas)
function cleanEnv(v) {
    return (v || '').trim().replace(/^["'`]+|["'`]+$/g, '').trim();
}
const REDIS_URL = cleanEnv(process.env.UPSTASH_REDIS_REST_URL);
const REDIS_TOKEN = cleanEnv(process.env.UPSTASH_REDIS_REST_TOKEN);
const REDIS_KEY = 'users:list';

// ==================== Marketing Ads (Google + Meta) env ====================
// Quando essas variáveis estiverem ausentes, os endpoints retornam dados MOCK
// (assim a Fernanda consegue ver a UI antes das credenciais reais chegarem).
const GOOGLE_ADS_DEVELOPER_TOKEN = cleanEnv(process.env.GOOGLE_ADS_DEVELOPER_TOKEN);
const GOOGLE_ADS_CLIENT_ID = cleanEnv(process.env.GOOGLE_ADS_CLIENT_ID);
const GOOGLE_ADS_CLIENT_SECRET = cleanEnv(process.env.GOOGLE_ADS_CLIENT_SECRET);
const GOOGLE_ADS_REFRESH_TOKEN = cleanEnv(process.env.GOOGLE_ADS_REFRESH_TOKEN);
const GOOGLE_ADS_CUSTOMER_ID = cleanEnv(process.env.GOOGLE_ADS_CUSTOMER_ID).replace(/\D/g, '');
const GOOGLE_ADS_LOGIN_CUSTOMER_ID = cleanEnv(process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace(/\D/g, '');
const META_ACCESS_TOKEN = cleanEnv(process.env.META_ACCESS_TOKEN);
const META_AD_ACCOUNT_ID = cleanEnv(process.env.META_AD_ACCOUNT_ID);
const META_APP_ID = cleanEnv(process.env.META_APP_ID);
const LINKEDIN_CLIENT_ID = cleanEnv(process.env.LINKEDIN_CLIENT_ID);
const LINKEDIN_CLIENT_SECRET = cleanEnv(process.env.LINKEDIN_CLIENT_SECRET);
const LINKEDIN_REFRESH_TOKEN = cleanEnv(process.env.LINKEDIN_REFRESH_TOKEN);
const LINKEDIN_AD_ACCOUNT_ID = cleanEnv(process.env.LINKEDIN_AD_ACCOUNT_ID).replace(/\D/g, '');
const LINKEDIN_API_VERSION = cleanEnv(process.env.LINKEDIN_API_VERSION) || '202405';
let redisStatusAtBoot = { configured: false, urlOk: false, seedOk: false, error: null };

// ==================== Upstash Redis helpers ====================
function redisCmd(args) {
    return new Promise((resolve, reject) => {
        if (!REDIS_URL || !REDIS_TOKEN) {
            return reject(new Error('UPSTASH_REDIS_REST_URL/TOKEN não configurados'));
        }
        const u = new URL(REDIS_URL);
        const body = JSON.stringify(args);
        const opts = {
            hostname: u.hostname,
            port: u.port || 443,
            path: '/',
            method: 'POST',
            headers: {
                'Authorization': 'Bearer ' + REDIS_TOKEN,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body)
            }
        };
        const req = https.request(opts, (rr) => {
            const chunks = [];
            rr.on('data', c => chunks.push(c));
            rr.on('end', () => {
                const text = Buffer.concat(chunks).toString('utf8');
                if (rr.statusCode < 200 || rr.statusCode >= 300) {
                    return reject(new Error('Redis HTTP ' + rr.statusCode + ': ' + text.slice(0, 200)));
                }
                try {
                    const parsed = JSON.parse(text);
                    if (parsed.error) return reject(new Error('Redis error: ' + parsed.error));
                    resolve(parsed.result);
                } catch (e) { reject(new Error('Redis parse error: ' + e.message)); }
            });
        });
        req.on('error', reject);
        req.setTimeout(15000, () => { req.destroy(); reject(new Error('Redis timeout')); });
        req.write(body);
        req.end();
    });
}
async function redisGet(key) { return await redisCmd(['GET', key]); }
async function redisSet(key, val) { return await redisCmd(['SET', key, val]); }

// ==================== Cache + bootstrap dos usuários ====================
let usersCache = [];
let usersCacheReady = false;
let usersBootPromise = null;

async function initUsersFromRedis() {
    redisStatusAtBoot.configured = !!(REDIS_URL && REDIS_TOKEN);
    redisStatusAtBoot.urlOk = REDIS_URL.startsWith('https://') && REDIS_URL.includes('.upstash.io');
    if (!REDIS_URL || !REDIS_TOKEN) {
        try {
            const raw = fs.readFileSync(USERS_PATH, 'utf8');
            usersCache = JSON.parse(raw).users || [];
            console.warn('[auth] Redis NÃO configurado — usando users.json local. URL:', REDIS_URL ? 'set' : 'EMPTY', 'TOKEN:', REDIS_TOKEN ? 'set' : 'EMPTY');
            redisStatusAtBoot.error = 'REDIS_URL/TOKEN ausentes';
        } catch (e) {
            console.error('[auth] sem Redis e sem users.json:', e.message);
            usersCache = [];
            redisStatusAtBoot.error = e.message;
        }
        usersCacheReady = true;
        return;
    }
    try {
        const data = await redisGet(REDIS_KEY);
        if (data) {
            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
            usersCache = Array.isArray(parsed) ? parsed : [];
            console.log('[auth] users carregados do Redis:', usersCache.length);
            redisStatusAtBoot.seedOk = true;
        } else {
            const raw = fs.readFileSync(USERS_PATH, 'utf8');
            usersCache = JSON.parse(raw).users || [];
            await redisSet(REDIS_KEY, JSON.stringify(usersCache));
            console.log('[auth] users seeded para Redis:', usersCache.length);
            redisStatusAtBoot.seedOk = true;
        }
    } catch (e) {
        console.error('[auth] ERRO inicializando Redis:', e.message);
        redisStatusAtBoot.error = e.message;
        try {
            const raw = fs.readFileSync(USERS_PATH, 'utf8');
            usersCache = JSON.parse(raw).users || [];
        } catch (er) { usersCache = []; }
    }
    usersCacheReady = true;
}
function waitUsersReady() {
    if (usersCacheReady) return Promise.resolve();
    return usersBootPromise || Promise.resolve();
}
function loadUsers() {
    return usersCache || [];
}
async function saveUsers(users) {
    usersCache = users;
    if (REDIS_URL && REDIS_TOKEN) {
        try {
            await redisSet(REDIS_KEY, JSON.stringify(users));
        } catch (e) {
            console.error('[auth] falha ao salvar no Redis:', e.message);
            throw e;
        }
    } else {
        // Fallback dev: grava no arquivo
        try { fs.writeFileSync(USERS_PATH, JSON.stringify({ users }, null, 2)); }
        catch (e) { console.error('[auth] falha gravando users.json:', e.message); throw e; }
    }
}
function findUser(email) {
    if (!email) return null;
    const e = String(email).toLowerCase().trim();
    return loadUsers().find(u => u.email === e) || null;
}
async function updateUser(email, patch) {
    const users = loadUsers().slice();
    const i = users.findIndex(u => u.email === String(email).toLowerCase().trim());
    if (i < 0) return false;
    users[i] = { ...users[i], ...patch };
    await saveUsers(users);
    return true;
}
function b64url(buf) {
    return Buffer.from(buf).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
}
function b64urlDecode(s) {
    s = String(s).replace(/-/g, '+').replace(/_/g, '/');
    while (s.length % 4) s += '=';
    return Buffer.from(s, 'base64');
}
function hmacSign(payload) {
    return b64url(crypto.createHmac('sha256', SESSION_SECRET).update(payload).digest());
}
function signToken(payloadObj, ttlMs) {
    const payload = { ...payloadObj, exp: Date.now() + ttlMs };
    const body = b64url(JSON.stringify(payload));
    const sig = hmacSign(body);
    return `${body}.${sig}`;
}
function verifyToken(token) {
    if (!token || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 2) return null;
    const [body, sig] = parts;
    const expected = hmacSign(body);
    // timing-safe compare
    if (sig.length !== expected.length) return null;
    if (!crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
    try {
        const payload = JSON.parse(b64urlDecode(body).toString('utf8'));
        if (!payload.exp || Date.now() > payload.exp) return null;
        return payload;
    } catch (e) { return null; }
}
function parseCookies(req) {
    const out = {};
    const raw = req.headers.cookie || '';
    raw.split(';').forEach(p => {
        const idx = p.indexOf('=');
        if (idx < 0) return;
        const k = p.slice(0, idx).trim();
        const v = p.slice(idx + 1).trim();
        if (k) out[k] = decodeURIComponent(v);
    });
    return out;
}
function setSessionCookie(res, token) {
    const maxAge = SESSION_DAYS * 86400;
    res.setHeader('Set-Cookie', `session=${encodeURIComponent(token)}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`);
}
function clearSessionCookie(res) {
    res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0');
}
function getCurrentUser(req) {
    const cookies = parseCookies(req);
    const payload = verifyToken(cookies.session);
    if (!payload || !payload.email) return null;
    const user = findUser(payload.email);
    if (!user) return null;
    return user;
}
function readBody(req) {
    return new Promise((resolve, reject) => {
        let data = '';
        req.on('data', c => { data += c; if (data.length > 100000) { req.destroy(); reject(new Error('too large')); } });
        req.on('end', () => resolve(data));
        req.on('error', reject);
    });
}
async function readJSON(req) {
    const body = await readBody(req);
    try { return body ? JSON.parse(body) : {}; } catch (e) { return {}; }
}
function jsonReply(res, code, obj) {
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(obj));
}
function sendResetEmail(toEmail, toName, resetLink) {
    return new Promise((resolve) => {
        if (!RESEND_API_KEY) {
            console.warn('[auth] RESEND_API_KEY nao configurado. Link de reset (manual):', resetLink);
            return resolve({ ok: false, reason: 'no_resend_key', link: resetLink });
        }
        const payload = JSON.stringify({
            from: RESEND_FROM,
            to: [toEmail],
            subject: 'Portal Lincros — Redefinir senha',
            html: `
                <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px;color:#222">
                    <h2 style="color:#1a1a2e">Portal de Oportunidades Lincros</h2>
                    <p>Olá, ${toName || ''}.</p>
                    <p>Recebemos uma solicitação para redefinir sua senha. Para criar uma nova senha, clique no botão abaixo:</p>
                    <p style="margin:28px 0">
                        <a href="${resetLink}" style="background:#6c3fb5;color:#fff;padding:12px 22px;border-radius:8px;text-decoration:none;font-weight:600">Redefinir senha</a>
                    </p>
                    <p style="font-size:13px;color:#666">Esse link expira em 1 hora. Se você não solicitou a redefinição, ignore este e-mail.</p>
                    <p style="font-size:12px;color:#999;margin-top:30px">— Equipe Lincros</p>
                </div>
            `
        });
        const opts = {
            hostname: 'api.resend.com',
            path: '/emails',
            method: 'POST',
            headers: {
                'Authorization': 'Bearer ' + RESEND_API_KEY,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload)
            }
        };
        const r = https.request(opts, (rr) => {
            const chunks = [];
            rr.on('data', c => chunks.push(c));
            rr.on('end', () => {
                const body = Buffer.concat(chunks).toString('utf8');
                if (rr.statusCode >= 200 && rr.statusCode < 300) {
                    console.log('[auth] Reset email enviado para', toEmail);
                    resolve({ ok: true });
                } else {
                    console.error('[auth] Resend falhou:', rr.statusCode, body);
                    resolve({ ok: false, reason: 'resend_error', status: rr.statusCode, body });
                }
            });
        });
        r.on('error', (e) => { console.error('[auth] Resend net error:', e.message); resolve({ ok: false, reason: e.message }); });
        r.write(payload);
        r.end();
    });
}
// Rotas/paths públicos (não exigem sessão)
function isPublicPath(url) {
    const p = url.split('?')[0];
    if (p === '/login' || p === '/login.html') return true;
    if (p === '/forgot-password' || p === '/forgot-password.html') return true;
    if (p === '/reset-password' || p === '/reset-password.html') return true;
    if (p.startsWith('/auth/')) return true;
    if (p === '/keepalive') return true;
    if (p.startsWith('/api/sankhya/')) return true; // bearer-auth próprio
    // Assets puros (favicon, imagens). HTML/JS principal continua protegido.
    if (/\.(png|jpg|jpeg|gif|svg|ico|woff2?)$/i.test(p)) return true;
    return false;
}

// ==================== In-memory cache ====================
const cache = {
    won: null, lost: null, open: null, contacts: null, forecast: null,
    lastRefresh: {}, refreshing: {}, initError: null
};
const responseCache = {};

function apiCall(apiPath) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: API_HOST,
            path: apiPath,
            method: 'GET',
            headers: { 'User-Key': API_KEY, 'Accept-Encoding': 'gzip', 'Content-Type': 'application/json' }
        };
        const req = https.request(options, (res) => {
            const chunks = [];
            const stream = res.headers['content-encoding'] === 'gzip' ? res.pipe(zlib.createGunzip()) : res;
            stream.on('data', c => chunks.push(c));
            stream.on('end', () => {
                try {
                    const body = Buffer.concat(chunks).toString('utf8');
                    if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}: ${body.substring(0, 200)}`));
                    resolve(JSON.parse(body));
                } catch (e) { reject(e); }
            });
            stream.on('error', reject);
        });
        req.on('error', reject);
        req.setTimeout(90000, () => { req.destroy(new Error('Timeout')); });
        req.end();
    });
}

async function fetchAllPages(baseUrl, pageSize = 200) {
    // Strip any existing $top/$skip from baseUrl
    const cleanUrl = baseUrl.replace(/[?&]\$top=\d+/g, '').replace(/[?&]\$skip=\d+/g, '').replace(/[?&]\$count=true/g, '');
    const sep = cleanUrl.includes('?') ? '&' : '?';
    console.log('[fetch]', cleanUrl.substring(0, 100));
    const first = await apiCall(`${cleanUrl}${sep}$count=true&$top=${pageSize}&$skip=0`);
    let all = first.value || [];
    const total = first['@odata.count'] || all.length;
    console.log('[fetch]  total=' + total + ', got first=' + all.length);
    if (all.length >= total) return all;

    const skips = [];
    for (let skip = pageSize; skip < total; skip += pageSize) skips.push(skip);

    // Reduced batch size to avoid memory pressure on Render free tier (512MB)
    const BATCH = 2;
    for (let i = 0; i < skips.length; i += BATCH) {
        const batch = skips.slice(i, i + BATCH);
        try {
            const results = await Promise.all(batch.map(skip => apiCall(`${cleanUrl}${sep}$top=${pageSize}&$skip=${skip}`)));
            results.forEach(r => { all = all.concat(r.value || []); });
            console.log('[fetch]  progress=' + all.length + '/' + total);
        } catch (e) {
            console.error('[fetch]  batch error at skip=' + batch[0] + ':', e.message);
            throw e;
        }
    }
    return all;
}

function buildResponse(data, meta = {}) {
    const body = JSON.stringify({ value: data, ...meta });
    const gzipped = zlib.gzipSync(body);
    return { body, gzipped };
}

// Disk persistence
try { fs.mkdirSync(CACHE_DIR, { recursive: true }); } catch (e) {}

function saveToDisk(key, data) {
    try {
        fs.writeFileSync(path.join(CACHE_DIR, `${key}.json`), JSON.stringify({ data, ts: Date.now(), v: CACHE_VERSION }));
    } catch (e) { console.warn(`[cache] save ${key} failed:`, e.message); }
}

function loadFromDisk(key) {
    try {
        const p = path.join(CACHE_DIR, `${key}.json`);
        if (!fs.existsSync(p)) return null;
        const parsed = JSON.parse(fs.readFileSync(p, 'utf8'));
        // Invalidate if cache version mismatches (schema changes)
        if (parsed.v !== CACHE_VERSION) return null;
        // Expire after 24h on disk
        if (Date.now() - parsed.ts > 24 * 60 * 60 * 1000) return null;
        return parsed.data;
    } catch (e) { return null; }
}

async function refreshOne(key, url) {
    if (cache.refreshing[key]) return;
    cache.refreshing[key] = true;
    const start = Date.now();
    try {
        const data = await fetchAllPages(url);
        cache[key] = data;
        cache.lastRefresh[key] = new Date().toISOString();
        responseCache[key] = buildResponse(data, { lastRefresh: cache.lastRefresh[key] });
        saveToDisk(key, data);
        console.log(`[cache] ${key} OK in ${((Date.now() - start)/1000).toFixed(1)}s: ${data.length} items`);
    } catch (e) {
        console.error(`[cache] ${key} error:`, e.message);
    }
    cache.refreshing[key] = false;
}

// Load from disk immediately on startup
console.log('[cache] Loading cache from disk...');
const keys = ['won', 'lost', 'open', 'contacts', 'forecast', 'orders', 'segments', 'companies', 'people', 'meetings', 'users'];
keys.forEach(key => {
    const data = loadFromDisk(key);
    if (data) {
        cache[key] = data;
        cache.lastRefresh[key] = new Date(Date.now() - 60000).toISOString() + ' (disco)';
        responseCache[key] = buildResponse(data, { lastRefresh: cache.lastRefresh[key] });
        console.log(`[cache] Loaded ${key} from disk: ${data.length} items`);
    }
});

// Safe URL encoder: only encodes chars that MUST be encoded in OData URLs.
// Preserves: / ? ( ) ' = & $ : , + - _ . ~ * # letters digits
// Encodes: spaces (as %20) and other special chars
function odataEncode(url) {
    return url.replace(/ /g, '%20');
}

async function refreshAll() {
    console.log('[cache] Starting full refresh...');
    // Contact.Origin é fallback quando deal.Origin tá vazio (alguns deals não herdam)
    const dealExpand = '$expand=Contact($expand=Phones,City($expand=State),OtherProperties,Origin),Owner,Stage,Pipeline,OtherProperties,Origin';
    const forecastFilter = "OtherProperties/any(o: o/FieldKey eq 'deal_7F644269-46FE-4486-AD12-BEFA9C7E27BC')";
    const stateFilter = "OtherProperties/any(o: o/FieldKey eq 'contact_486DE9AD-FCFE-4A7B-8B56-DA5AB3D55848')";

    // Each runs independently - available as soon as it finishes
    // Start forecast first (smallest, used by mobile often)
    refreshOne('forecast', odataEncode(`/Deals?$filter=StatusId eq 1 and ${forecastFilter}&$orderby=CreateDate desc&$expand=Owner,Pipeline,Stage,OtherProperties`));
    refreshOne('contacts', odataEncode(`/Contacts?$filter=${stateFilter}&$expand=OtherProperties&$select=Id,LineOfBusinessId`));
    refreshOne('won', odataEncode(`/Deals?$filter=StatusId eq 2&$orderby=FinishDate desc&${dealExpand}`));
    // Lost deals need LossReason expanded for reason analysis
    refreshOne('lost', odataEncode(`/Deals?$filter=StatusId eq 3&$orderby=FinishDate desc&${dealExpand},LossReason`));
    refreshOne('open', odataEncode(`/Deals?$filter=StatusId eq 1&$orderby=CreateDate desc&${dealExpand}`));
    // Orders (pedidos fechados) com produtos para analise de produtos vendidos
    refreshOne('orders', odataEncode('/Orders?$expand=Products&$orderby=Date desc'));
    // Segmentos (Ramo de Atividade / LineOfBusiness) - pequena lista fixa
    refreshOne('segments', '/Contacts@LinesOfBusiness');
    // Empresas (TypeId=1) - para achar Segmento (LineOfBusinessId) e Clientes Lincros (Tag 60146250)
    // Filtra apenas as que tem LineOfBusinessId OR alguma Tag - reduz volume
    refreshOne('companies', odataEncode("/Contacts?$filter=TypeId eq 1 and (LineOfBusinessId ne null or Tags/any(t: t/TagId eq 60146250))&$expand=Tags($select=TagId)&$select=Id,LineOfBusinessId"));
    // Pessoas (TypeId=2) com email para deteccao de MQLs (ultimos 90 dias somente)
    // Sem expand de Phones para reduzir payload - vamos buscar phones em batch separado se necessario
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
    refreshOne('people', odataEncode(`/Contacts?$filter=TypeId eq 2 and Email ne null and CreateDate ge ${ninetyDaysAgo}&$expand=Phones,Origin,Owner($select=Id,Name),Creator($select=Id,Name)&$select=Id,Name,Email,CompanyId,CreateDate,OriginId,OwnerId,CreatorId`));
    // Atividades "Reuniao Agendada" / "Reuniao Realizada"
    // Pega ultimos 120 dias de tasks com Title comecando com Reun.
    // Filtragem case-sensitive simples (Reun maiusculo cobre os dois tipos comuns).
    // Sem $select pra trazer todos os campos (Finished, FinishDate, etc).
    const oneTwentyDaysAgo = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString();
    refreshOne('meetings', odataEncode(`/Tasks?$filter=DateTime ge ${oneTwentyDaysAgo} and startswith(Title,'Reun')&$orderby=DateTime desc`));
    // Interactions = WhatsApp, e-mail, calls registradas no Ploomes (não atualiza LastUpdateDate do deal)
    // Necessário pra calcular "última atividade real" — sem isso o cockpit acusa dias errados
    // Inclui Content+CreatorId pra detectar Reunião Agendada/Realizada registrada como
    // interaction (algumas SDRs preferem registrar via Interaction em vez de Task)
    refreshOne('interactions', odataEncode(`/Interactions?$filter=Date ge ${oneTwentyDaysAgo}&$expand=InteractionType($select=Id,Name)&$select=Id,DealId,Date,TypeId,Content,CreatorId&$orderby=Date desc`));
    // Lista de usuarios para resolver OwnerId -> Name nas atividades
    refreshOne('users', odataEncode('/Users?$select=Id,Name'));
}

refreshAll();
setInterval(refreshAll, 5 * 60 * 1000);

// ==================== Sankhya Public API (Bearer auth) ====================
// Token configuravel via env var SANKHYA_API_TOKEN. Default abaixo eh um fallback
// (gerado aleatoriamente — substitua pelo Render env var antes de compartilhar).
const SANKHYA_API_TOKEN = process.env.SANKHYA_API_TOKEN
    || 'sk_lincros_2026_xQ7n4mBz9LpVtR2yH8eK3jW6sFcGdA';

// Field keys do Ploomes (descobertos via /Fields)
const FIELD_KEY_CANAL    = 'deal_D796B31C-4382-431C-AB1B-62398F7841BF'; // Canal (Sankhya)
const FIELD_KEY_SDR      = 'deal_55DF811B-D677-40FE-895C-4BDBE5C277DA'; // SDR (User ref)
const FIELD_KEY_MRR      = 'deal_1F7F1DEC-39B3-4621-9237-96D7793DAD03'; // MRR
const FIELD_KEY_SETUP    = 'deal_90CB9147-95C6-4A5F-8607-A2B5225ADFC3'; // Setup
const FIELD_KEY_PROP_DT  = 'deal_D7ED2D45-3C8B-479E-B0B0-F8CB8E053E5A'; // Data Marcador Proposta
const FIELD_KEY_MRR_NOVO = 'deal_FFC0BC11-4F38-44B0-B6F0-CE5E70B5E12D'; // MRR Novo (fallback)
const FIELD_KEY_SETUP_ALT= 'deal_72B86F1D-3F1F-419D-A574-19A3D6F4B6E1'; // Setup (fallback)
const FIELD_KEY_PROP_DT_ALT = 'deal_12C64ECD-CD5C-4C83-B0CD-7E7CCB415D7E'; // Data Proposta Enviada (fallback)

function getDealOtherProp(deal, fieldKey) {
    return (deal.OtherProperties || []).find(p => p.FieldKey === fieldKey);
}
function getDealCanal(d) { return getDealOtherProp(d, FIELD_KEY_CANAL)?.StringValue || null; }
function getDealMRR(d) {
    return getDealOtherProp(d, FIELD_KEY_MRR)?.DecimalValue
        || getDealOtherProp(d, FIELD_KEY_MRR_NOVO)?.DecimalValue
        || 0;
}
function getDealSetup(d) {
    return getDealOtherProp(d, FIELD_KEY_SETUP)?.DecimalValue
        || getDealOtherProp(d, FIELD_KEY_SETUP_ALT)?.DecimalValue
        || 0;
}
function getDealPropDate(d) {
    return getDealOtherProp(d, FIELD_KEY_PROP_DT)?.DateTimeValue
        || getDealOtherProp(d, FIELD_KEY_PROP_DT_ALT)?.DateTimeValue
        || null;
}

// "Sankhya" = deal com o campo customizado "Canal" preenchido
function isSankhyaDeal(d) {
    const canal = getDealCanal(d);
    return !!(canal && canal.trim());
}

function dealOriginName(d) {
    return d.Origin?.Name || '(sem origem)';
}

function checkSankhyaAuth(req) {
    const auth = req.headers['authorization'] || '';
    const m = auth.match(/^Bearer\s+(.+)$/);
    if (!m) return false;
    return m[1] === SANKHYA_API_TOKEN;
}

function unauthorized(res) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
        error: 'Unauthorized',
        message: 'Inclua o header: Authorization: Bearer <token>'
    }));
}

function buildSankhyaLead(d, hasMeeting) {
    const status = d.StatusId === 2 ? 'won' : (d.StatusId === 3 ? 'lost' : 'open');
    const mrr = getDealMRR(d), setup = getDealSetup(d);
    const propDate = getDealPropDate(d);
    const hasProposal = mrr > 0 || setup > 0 || !!propDate;
    const canal = getDealCanal(d);
    return {
        dealId: d.Id,
        title: d.Title || '',
        canal,                          // valor do campo customizado "Canal"
        origin: dealOriginName(d),      // Origin do Ploomes (separado)
        pipeline: d.Pipeline?.Name || '',
        stage: d.Stage?.Name || '',
        owner: d.Owner?.Name || '',
        status,
        hasMeeting: !!hasMeeting,
        hasProposal,
        mrr,
        setup,
        lossReason: d.LossReason?.Name || null,
        createDate: d.CreateDate || null,
        finishDate: d.FinishDate || null,
        proposalDate: propDate,
        ploomesLink: 'https://app10.ploomes.com/deal/' + d.Id
    };
}

function sankhyaDataReady() {
    return cache.won && cache.lost && cache.open && cache.meetings;
}

function buildSankhyaPayload() {
    if (!sankhyaDataReady()) return null;
    const allDeals = [...(cache.open||[]), ...(cache.won||[]), ...(cache.lost||[])]
        .filter(isSankhyaDeal);
    // Set de DealIds com task "Reuniao Agendada" no cache (qualquer data — = ja teve reuniao agendada algum dia)
    const dealsWithMeeting = new Set();
    (cache.meetings||[]).forEach(t => {
        const tn = (t.Title||'').toLowerCase();
        if (tn.startsWith('reuni') && t.DealId) dealsWithMeeting.add(t.DealId);
    });
    const leads = allDeals.map(d => buildSankhyaLead(d, dealsWithMeeting.has(d.Id)));
    return { leads, dealsWithMeeting };
}

function aggregateByChannel(leads) {
    const ch = {};
    leads.forEach(l => {
        const k = l.canal || '(sem canal)';
        if (!ch[k]) ch[k] = {
            canal: k, totalLeads: 0, withMeeting: 0, withProposal: 0,
            won: 0, lost: 0, openNoMeeting: 0, mrrWon: 0, setupWon: 0,
            mrrPipeline: 0, setupPipeline: 0 // potencial em propostas abertas
        };
        ch[k].totalLeads++;
        if (l.hasMeeting) ch[k].withMeeting++;
        if (l.hasProposal) ch[k].withProposal++;
        if (l.status === 'won') { ch[k].won++; ch[k].mrrWon += l.mrr; ch[k].setupWon += l.setup; }
        if (l.status === 'lost') ch[k].lost++;
        if (l.status === 'open' && !l.hasMeeting) ch[k].openNoMeeting++;
        if (l.status === 'open' && l.hasProposal) { ch[k].mrrPipeline += l.mrr; ch[k].setupPipeline += l.setup; }
    });
    return Object.values(ch).sort((a, b) => b.totalLeads - a.totalLeads);
}

function handleSankhyaApi(req, res, pathname, query) {
    if (!checkSankhyaAuth(req)) return unauthorized(res);
    if (!sankhyaDataReady()) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            error: 'ServiceUnavailable',
            message: 'Cache de deals ainda carregando. Tente em alguns segundos.'
        }));
    }
    const payload = buildSankhyaPayload();
    const asOf = new Date().toISOString();

    if (pathname === '/api/sankhya/v1/channels') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            asOf,
            totalLeads: payload.leads.length,
            channels: aggregateByChannel(payload.leads)
        }));
    }

    if (pathname === '/api/sankhya/v1/leads') {
        let filtered = payload.leads;
        // Filtros: canal (preferido), channel (alias retrocompat), status, hasMeeting, hasProposal
        const canalQ = query.canal || query.channel;
        if (canalQ) filtered = filtered.filter(l => l.canal === canalQ);
        if (query.status) filtered = filtered.filter(l => l.status === query.status);
        if (query.hasMeeting === 'true') filtered = filtered.filter(l => l.hasMeeting);
        if (query.hasMeeting === 'false') filtered = filtered.filter(l => !l.hasMeeting);
        if (query.hasProposal === 'true') filtered = filtered.filter(l => l.hasProposal);
        if (query.hasProposal === 'false') filtered = filtered.filter(l => !l.hasProposal);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            asOf, count: filtered.length, leads: filtered
        }));
    }

    if (pathname === '/api/sankhya/v1/leads/no-meeting') {
        const filtered = payload.leads.filter(l => !l.hasMeeting && l.status === 'open');
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            asOf, count: filtered.length, leads: filtered
        }));
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
        error: 'NotFound',
        availableEndpoints: [
            'GET /api/sankhya/v1/channels',
            'GET /api/sankhya/v1/leads?canal=X&status=open|won|lost&hasMeeting=true|false&hasProposal=true|false',
            'GET /api/sankhya/v1/leads/no-meeting'
        ],
        notes: '"Sankhya" = deal com campo customizado "Canal" preenchido no Ploomes. Canal e o valor desse campo (ex: "Espirito Santo", "Sul", etc).'
    }));
}

// ==================== Marketing Ads helpers ====================
// Cache em memória (1h) — depois pode migrar pro Redis se múltiplas instâncias.
const ADS_CACHE_TTL_MS = 60 * 60 * 1000; // 1h
const adsCache = {
    google:   { data: null, ts: 0, error: null },
    meta:     { data: null, ts: 0, error: null },
    linkedin: { data: null, ts: 0, error: null }
};

function isGoogleAdsConfigured() {
    return !!(GOOGLE_ADS_DEVELOPER_TOKEN && GOOGLE_ADS_CLIENT_ID && GOOGLE_ADS_CLIENT_SECRET
              && GOOGLE_ADS_REFRESH_TOKEN && GOOGLE_ADS_CUSTOMER_ID);
}
function isMetaAdsConfigured() {
    return !!(META_ACCESS_TOKEN && META_AD_ACCOUNT_ID);
}
function isLinkedinAdsConfigured() {
    return !!(LINKEDIN_CLIENT_ID && LINKEDIN_CLIENT_SECRET && LINKEDIN_REFRESH_TOKEN && LINKEDIN_AD_ACCOUNT_ID);
}

// HTTP helper genérico (POST com body, retorna JSON parsed)
function httpsJsonRequest({ hostname, path: p, method, headers, body }) {
    return new Promise((resolve, reject) => {
        const opts = {
            hostname, path: p, method,
            headers: Object.assign({}, headers || {})
        };
        if (body && !opts.headers['Content-Length']) {
            opts.headers['Content-Length'] = Buffer.byteLength(body);
        }
        const r = https.request(opts, (rr) => {
            const chunks = [];
            rr.on('data', c => chunks.push(c));
            rr.on('end', () => {
                const raw = Buffer.concat(chunks).toString('utf8');
                let json;
                try { json = JSON.parse(raw); } catch { json = null; }
                if (rr.statusCode < 200 || rr.statusCode >= 300) {
                    // Loga o body cru pra debug em Render logs
                    console.warn(`[httpsJsonRequest] HTTP ${rr.statusCode} ${hostname}${p}`);
                    console.warn(`[httpsJsonRequest] body: ${raw.slice(0, 600)}`);
                    // Tenta várias estruturas de erro comuns (Meta, Google, LinkedIn, etc)
                    let msg;
                    if (json) {
                        msg = json.error?.message
                           || json.error_description
                           || json.message
                           || json.serviceErrorMessage
                           || (typeof json.error === 'string' ? json.error : null)
                           || (json.errors && json.errors[0]?.message)
                           || JSON.stringify(json).slice(0, 400);
                    } else {
                        msg = raw.slice(0, 400) || `HTTP ${rr.statusCode}`;
                    }
                    return reject(new Error(`HTTP ${rr.statusCode}: ${msg}`));
                }
                resolve(json || {});
            });
        });
        r.on('error', reject);
        r.setTimeout(20000, () => { r.destroy(); reject(new Error('timeout')); });
        if (body) r.write(body);
        r.end();
    });
}

// === GOOGLE ADS ===
// Troca refresh_token por access_token (válido 1h)
async function googleAdsAccessToken() {
    const body = `client_id=${encodeURIComponent(GOOGLE_ADS_CLIENT_ID)}`
               + `&client_secret=${encodeURIComponent(GOOGLE_ADS_CLIENT_SECRET)}`
               + `&refresh_token=${encodeURIComponent(GOOGLE_ADS_REFRESH_TOKEN)}`
               + `&grant_type=refresh_token`;
    const json = await httpsJsonRequest({
        hostname: 'oauth2.googleapis.com',
        path: '/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body
    });
    if (!json.access_token) throw new Error('OAuth response sem access_token');
    return json.access_token;
}

async function googleAdsFetchPeriod(period) {
    const accessToken = await googleAdsAccessToken();
    // period: THIS_MONTH | LAST_MONTH
    const query = `
        SELECT
            campaign.id, campaign.name, campaign.status,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value, metrics.ctr, metrics.average_cpc
        FROM campaign
        WHERE segments.date DURING ${period}
    `.replace(/\s+/g, ' ').trim();
    const headers = {
        'Authorization': 'Bearer ' + accessToken,
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'Content-Type': 'application/json'
    };
    if (GOOGLE_ADS_LOGIN_CUSTOMER_ID) headers['login-customer-id'] = GOOGLE_ADS_LOGIN_CUSTOMER_ID;
    const json = await httpsJsonRequest({
        hostname: 'googleads.googleapis.com',
        path: `/v17/customers/${GOOGLE_ADS_CUSTOMER_ID}/googleAds:search`,
        method: 'POST',
        headers,
        body: JSON.stringify({ query })
    });
    return normalizeGoogleAds(json);
}

function normalizeGoogleAds(json) {
    const results = json.results || [];
    let totalSpend = 0, totalImp = 0, totalClicks = 0, totalConv = 0, totalConvValue = 0;
    const campaigns = results.map(r => {
        const c = r.campaign || {};
        const m = r.metrics || {};
        const spend = Number(m.costMicros || 0) / 1_000_000;
        const imp = Number(m.impressions || 0);
        const clicks = Number(m.clicks || 0);
        const conv = Number(m.conversions || 0);
        const convValue = Number(m.conversionsValue || 0);
        totalSpend += spend; totalImp += imp; totalClicks += clicks;
        totalConv += conv; totalConvValue += convValue;
        return {
            id: c.id, name: c.name, status: c.status,
            spend, impressions: imp, clicks, conversions: conv, conversionsValue: convValue,
            ctr: imp > 0 ? clicks / imp : 0,
            cpc: clicks > 0 ? spend / clicks : 0,
            cpl: conv > 0 ? spend / conv : 0
        };
    });
    campaigns.sort((a, b) => b.spend - a.spend);
    return {
        source: 'google-ads',
        fetchedAt: new Date().toISOString(),
        period: 'this_month',
        currency: 'BRL',
        totals: {
            spend: totalSpend,
            impressions: totalImp,
            clicks: totalClicks,
            conversions: totalConv,
            conversionsValue: totalConvValue,
            ctr: totalImp > 0 ? totalClicks / totalImp : 0,
            cpc: totalClicks > 0 ? totalSpend / totalClicks : 0,
            cpl: totalConv > 0 ? totalSpend / totalConv : 0,
            roas: totalSpend > 0 ? totalConvValue / totalSpend : 0
        },
        campaigns
    };
}

// Mock data — usado enquanto a Fernanda não tem todas as credenciais
function googleAdsMock() {
    return {
        source: 'google-ads',
        mock: true,
        fetchedAt: new Date().toISOString(),
        period: 'this_month',
        currency: 'BRL',
        totals: {
            spend: 8420.50, impressions: 145320, clicks: 2103,
            conversions: 47, conversionsValue: 188000,
            ctr: 0.0145, cpc: 4.00, cpl: 179.16, roas: 22.33
        },
        campaigns: [
            { id: '101', name: '[MOCK] Search - Marca Lincros', status: 'ENABLED', spend: 3200, impressions: 52000, clicks: 980, conversions: 28, conversionsValue: 112000, ctr: 0.0188, cpc: 3.27, cpl: 114.28 },
            { id: '102', name: '[MOCK] Search - TMS Transportadora', status: 'ENABLED', spend: 2870, impressions: 48500, clicks: 612, conversions: 12, conversionsValue: 48000, ctr: 0.0126, cpc: 4.69, cpl: 239.16 },
            { id: '103', name: '[MOCK] Display - Remarketing', status: 'ENABLED', spend: 1450, impressions: 35200, clicks: 380, conversions: 5, conversionsValue: 20000, ctr: 0.0108, cpc: 3.82, cpl: 290.00 },
            { id: '104', name: '[MOCK] Performance Max - Lead Gen', status: 'PAUSED', spend: 900.50, impressions: 9620, clicks: 131, conversions: 2, conversionsValue: 8000, ctr: 0.0136, cpc: 6.87, cpl: 450.25 }
        ]
    };
}

async function googleAdsGetCached(force) {
    const c = adsCache.google;
    if (!force && c.data && (Date.now() - c.ts) < ADS_CACHE_TTL_MS) return c.data;
    if (!isGoogleAdsConfigured()) {
        const mock = googleAdsMock();
        c.data = mock; c.ts = Date.now(); c.error = null;
        return mock;
    }
    try {
        const [current, previous] = await Promise.all([
            googleAdsFetchPeriod('THIS_MONTH'),
            googleAdsFetchPeriod('LAST_MONTH').catch(e => null)
        ]);
        const combined = Object.assign({}, current, {
            previous: previous ? { totals: previous.totals, campaigns: previous.campaigns } : null
        });
        c.data = combined; c.ts = Date.now(); c.error = null;
        return combined;
    } catch (e) {
        c.error = e.message;
        if (c.data) return Object.assign({}, c.data, { stale: true, lastError: e.message });
        return Object.assign(googleAdsMock(), { _apiError: e.message });
    }
}

// === META ADS ===
async function metaAdsFetchPeriod(datePreset) {
    // datePreset: this_month | last_month
    const acct = META_AD_ACCOUNT_ID.startsWith('act_') ? META_AD_ACCOUNT_ID : 'act_' + META_AD_ACCOUNT_ID;
    const fields = ['campaign_id', 'campaign_name', 'spend', 'impressions', 'clicks',
                    'cpc', 'ctr', 'actions', 'action_values'].join(',');
    const params = `fields=${fields}&level=campaign&date_preset=${datePreset}&limit=200`
                 + `&access_token=${encodeURIComponent(META_ACCESS_TOKEN)}`;
    const json = await httpsJsonRequest({
        hostname: 'graph.facebook.com',
        path: `/v19.0/${acct}/insights?${params}`,
        method: 'GET',
        headers: {}
    });
    return normalizeMetaAds(json);
}

function normalizeMetaAds(json) {
    const data = json.data || [];
    let totalSpend = 0, totalImp = 0, totalClicks = 0, totalLeads = 0, totalLeadValue = 0;
    const campaigns = data.map(r => {
        const spend = Number(r.spend || 0);
        const imp = Number(r.impressions || 0);
        const clicks = Number(r.clicks || 0);
        const leadAction = (r.actions || []).find(a => /lead/i.test(a.action_type));
        const leadValueAction = (r.action_values || []).find(a => /lead/i.test(a.action_type));
        const leads = Number(leadAction?.value || 0);
        const leadValue = Number(leadValueAction?.value || 0);
        totalSpend += spend; totalImp += imp; totalClicks += clicks;
        totalLeads += leads; totalLeadValue += leadValue;
        return {
            id: r.campaign_id, name: r.campaign_name || '(sem nome)',
            spend, impressions: imp, clicks, conversions: leads, conversionsValue: leadValue,
            ctr: Number(r.ctr || 0) / 100, cpc: Number(r.cpc || 0),
            cpl: leads > 0 ? spend / leads : 0
        };
    });
    campaigns.sort((a, b) => b.spend - a.spend);
    return {
        source: 'meta-ads',
        fetchedAt: new Date().toISOString(),
        period: 'this_month',
        currency: 'BRL',
        totals: {
            spend: totalSpend, impressions: totalImp, clicks: totalClicks,
            conversions: totalLeads, conversionsValue: totalLeadValue,
            ctr: totalImp > 0 ? totalClicks / totalImp : 0,
            cpc: totalClicks > 0 ? totalSpend / totalClicks : 0,
            cpl: totalLeads > 0 ? totalSpend / totalLeads : 0,
            roas: totalSpend > 0 ? totalLeadValue / totalSpend : 0
        },
        campaigns
    };
}

function metaAdsMock() {
    return {
        source: 'meta-ads', mock: true,
        fetchedAt: new Date().toISOString(),
        period: 'this_month', currency: 'BRL',
        totals: {
            spend: 5310.20, impressions: 287400, clicks: 4120,
            conversions: 62, conversionsValue: 124000,
            ctr: 0.0143, cpc: 1.29, cpl: 85.65, roas: 23.35
        },
        campaigns: [
            { id: '201', name: '[MOCK] Leads - TMS Transportadoras', spend: 2410, impressions: 138000, clicks: 2050, conversions: 35, conversionsValue: 70000, ctr: 0.0148, cpc: 1.18, cpl: 68.85 },
            { id: '202', name: '[MOCK] Awareness - Logística B2B', spend: 1620.20, impressions: 95400, clicks: 1380, conversions: 18, conversionsValue: 36000, ctr: 0.0145, cpc: 1.17, cpl: 90.01 },
            { id: '203', name: '[MOCK] Remarketing - Site Lincros', spend: 1280, impressions: 54000, clicks: 690, conversions: 9, conversionsValue: 18000, ctr: 0.0128, cpc: 1.85, cpl: 142.22 }
        ]
    };
}

async function metaAdsGetCached(force) {
    const c = adsCache.meta;
    if (!force && c.data && (Date.now() - c.ts) < ADS_CACHE_TTL_MS) return c.data;
    if (!isMetaAdsConfigured()) {
        const mock = metaAdsMock();
        c.data = mock; c.ts = Date.now(); c.error = null;
        return mock;
    }
    try {
        const [current, previous] = await Promise.all([
            metaAdsFetchPeriod('this_month'),
            metaAdsFetchPeriod('last_month').catch(e => null)
        ]);
        const combined = Object.assign({}, current, {
            previous: previous ? { totals: previous.totals, campaigns: previous.campaigns } : null
        });
        c.data = combined; c.ts = Date.now(); c.error = null;
        return combined;
    } catch (e) {
        c.error = e.message;
        if (c.data) return Object.assign({}, c.data, { stale: true, lastError: e.message });
        return Object.assign(metaAdsMock(), { _apiError: e.message });
    }
}

// === LINKEDIN ADS ===
// OAuth 2.0 refresh token flow — refresh tokens válidos por ~1 ano
async function linkedinAccessToken() {
    const body = `grant_type=refresh_token`
               + `&refresh_token=${encodeURIComponent(LINKEDIN_REFRESH_TOKEN)}`
               + `&client_id=${encodeURIComponent(LINKEDIN_CLIENT_ID)}`
               + `&client_secret=${encodeURIComponent(LINKEDIN_CLIENT_SECRET)}`;
    const json = await httpsJsonRequest({
        hostname: 'www.linkedin.com',
        path: '/oauth/v2/accessToken',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body
    });
    if (!json.access_token) throw new Error('LinkedIn OAuth sem access_token');
    return json.access_token;
}

// monthOffset: 0 = mês atual, -1 = mês passado
async function linkedinAdsFetchMonth(monthOffset) {
    const accessToken = await linkedinAccessToken();
    const now = new Date();
    const ref = new Date(now.getFullYear(), now.getMonth() + (monthOffset || 0), 1);
    const startY = ref.getFullYear(), startM = ref.getMonth() + 1, startD = 1;
    // Fim: último dia do mês ref (ou hoje se for mês atual)
    let endY, endM, endD;
    if (monthOffset === 0 || monthOffset === undefined) {
        endY = now.getFullYear(); endM = now.getMonth() + 1; endD = now.getDate();
    } else {
        const lastDay = new Date(ref.getFullYear(), ref.getMonth() + 1, 0);
        endY = lastDay.getFullYear(); endM = lastDay.getMonth() + 1; endD = lastDay.getDate();
    }
    // adAnalytics REST — usa formato dotted (v202405+ deprecou o nested parens)
    const fields = 'impressions,clicks,costInLocalCurrency,externalWebsiteConversions,oneClickLeads,pivotValues';
    const qs = [
        'q=analytics',
        'pivot=CAMPAIGN',
        'timeGranularity=ALL',
        `dateRange.start.day=${startD}`,
        `dateRange.start.month=${startM}`,
        `dateRange.start.year=${startY}`,
        `dateRange.end.day=${endD}`,
        `dateRange.end.month=${endM}`,
        `dateRange.end.year=${endY}`,
        `accounts=List(urn:li:sponsoredAccount:${LINKEDIN_AD_ACCOUNT_ID})`,
        `fields=${fields}`
    ].join('&');
    const headers = {
        'Authorization': 'Bearer ' + accessToken,
        'LinkedIn-Version': LINKEDIN_API_VERSION,
        'X-Restli-Protocol-Version': '2.0.0'
    };
    const insights = await httpsJsonRequest({
        hostname: 'api.linkedin.com',
        path: `/rest/adAnalytics?${qs}`,
        method: 'GET',
        headers
    });
    // Pega nomes das campanhas — chamada extra (analytics retorna URN da campanha mas não nome)
    const campaignNames = await linkedinFetchCampaignNames(accessToken, insights, accountUrn).catch(() => ({}));
    return normalizeLinkedinAds(insights, campaignNames);
}

async function linkedinFetchCampaignNames(accessToken, insights, accountUrn) {
    const urns = new Set();
    for (const r of (insights.elements || [])) {
        const pv = (r.pivotValues || [])[0];
        if (pv && pv.startsWith('urn:li:sponsoredCampaign:')) urns.add(pv);
    }
    if (!urns.size) return {};
    const ids = Array.from(urns).map(u => u.replace('urn:li:sponsoredCampaign:', ''));
    const headers = {
        'Authorization': 'Bearer ' + accessToken,
        'LinkedIn-Version': LINKEDIN_API_VERSION,
        'X-Restli-Protocol-Version': '2.0.0'
    };
    const map = {};
    // Busca em lotes pequenos pra não estourar tamanho de URL
    for (let i = 0; i < ids.length; i += 20) {
        const batch = ids.slice(i, i + 20);
        const idsParam = batch.map(id => `List(urn%3Ali%3AsponsoredCampaign%3A${id})`).join(',');
        const path = `/rest/adCampaigns?ids=${idsParam}`;
        try {
            const j = await httpsJsonRequest({ hostname: 'api.linkedin.com', path, method: 'GET', headers });
            for (const k of Object.keys(j.results || {})) {
                map[k] = j.results[k]?.name || k;
            }
        } catch { /* não-fatal: nome fica como URN */ }
    }
    return map;
}

function normalizeLinkedinAds(insights, campaignNames) {
    const elements = insights.elements || [];
    let totalSpend = 0, totalImp = 0, totalClicks = 0, totalConv = 0;
    const campaigns = elements.map(r => {
        const urn = (r.pivotValues || [])[0] || 'unknown';
        const id = urn.replace('urn:li:sponsoredCampaign:', '');
        const name = campaignNames[urn] || `Campaign ${id}`;
        const spend = Number(r.costInLocalCurrency || 0);
        const imp = Number(r.impressions || 0);
        const clicks = Number(r.clicks || 0);
        const webConv = Number(r.externalWebsiteConversions || 0);
        const leadConv = Number(r.oneClickLeads || 0);
        const conv = webConv + leadConv;
        totalSpend += spend; totalImp += imp; totalClicks += clicks; totalConv += conv;
        return {
            id, name,
            spend, impressions: imp, clicks, conversions: conv,
            conversionsValue: 0, // LinkedIn não retorna valor monetário por padrão
            ctr: imp > 0 ? clicks / imp : 0,
            cpc: clicks > 0 ? spend / clicks : 0,
            cpl: conv > 0 ? spend / conv : 0
        };
    });
    campaigns.sort((a, b) => b.spend - a.spend);
    return {
        source: 'linkedin-ads',
        fetchedAt: new Date().toISOString(),
        period: 'this_month',
        currency: 'BRL',
        totals: {
            spend: totalSpend, impressions: totalImp, clicks: totalClicks,
            conversions: totalConv, conversionsValue: 0,
            ctr: totalImp > 0 ? totalClicks / totalImp : 0,
            cpc: totalClicks > 0 ? totalSpend / totalClicks : 0,
            cpl: totalConv > 0 ? totalSpend / totalConv : 0,
            roas: 0 // sem valor de conversão monetário no LinkedIn
        },
        campaigns
    };
}

function linkedinAdsMock() {
    return {
        source: 'linkedin-ads', mock: true,
        fetchedAt: new Date().toISOString(),
        period: 'this_month', currency: 'BRL',
        totals: {
            spend: 3920.00, impressions: 62500, clicks: 730,
            conversions: 18, conversionsValue: 0,
            ctr: 0.0117, cpc: 5.37, cpl: 217.78, roas: 0
        },
        campaigns: [
            { id: '601', name: '[MOCK] Sponsored Content - Decisores TMS', spend: 1840, impressions: 29000, clicks: 380, conversions: 11, conversionsValue: 0, ctr: 0.0131, cpc: 4.84, cpl: 167.27 },
            { id: '602', name: '[MOCK] Message Ads - C-Level Logística', spend: 1280, impressions: 18500, clicks: 215, conversions: 5, conversionsValue: 0, ctr: 0.0116, cpc: 5.95, cpl: 256.00 },
            { id: '603', name: '[MOCK] Lead Gen Form - Diretores Operações', spend: 800, impressions: 15000, clicks: 135, conversions: 2, conversionsValue: 0, ctr: 0.0090, cpc: 5.92, cpl: 400.00 }
        ]
    };
}

async function linkedinAdsGetCached(force) {
    const c = adsCache.linkedin;
    if (!force && c.data && (Date.now() - c.ts) < ADS_CACHE_TTL_MS) return c.data;
    if (!isLinkedinAdsConfigured()) {
        const m = linkedinAdsMock();
        c.data = m; c.ts = Date.now(); c.error = null;
        return m;
    }
    try {
        const [current, previous] = await Promise.all([
            linkedinAdsFetchMonth(0),
            linkedinAdsFetchMonth(-1).catch(e => null)
        ]);
        const combined = Object.assign({}, current, {
            previous: previous ? { totals: previous.totals, campaigns: previous.campaigns } : null
        });
        c.data = combined; c.ts = Date.now(); c.error = null;
        return combined;
    } catch (e) {
        c.error = e.message;
        if (c.data) return Object.assign({}, c.data, { stale: true, lastError: e.message });
        return Object.assign(linkedinAdsMock(), { _apiError: e.message });
    }
}


// ==================== HTTP server ====================
const server = http.createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

    const urlPath = req.url.split('?')[0];

    // ==================== Auth endpoints ====================
    if (urlPath === '/auth/login' && req.method === 'POST') {
        const body = await readJSON(req);
        const email = String(body.email || '').toLowerCase().trim();
        const password = String(body.password || '');
        if (!email || !password) return jsonReply(res, 400, { error: 'E-mail e senha obrigatórios' });
        const user = findUser(email);
        if (!user) return jsonReply(res, 401, { error: 'E-mail ou senha inválidos' });
        let ok = false;
        try { ok = bcrypt.compareSync(password, user.passwordHash); } catch (e) { ok = false; }
        if (!ok) return jsonReply(res, 401, { error: 'E-mail ou senha inválidos' });
        // Registra ultimo login (best-effort, não bloqueia)
        updateUser(user.email, { lastLoginAt: new Date().toISOString() }).catch(e => console.error('[auth] lastLoginAt:', e.message));
        const token = signToken({ email: user.email }, SESSION_DAYS * 86400 * 1000);
        setSessionCookie(res, token);
        return jsonReply(res, 200, {
            ok: true,
            user: { email: user.email, name: user.name, isAdmin: !!user.isAdmin, mustChangePassword: !!user.mustChangePassword }
        });
    }

    if (urlPath === '/auth/logout' && (req.method === 'POST' || req.method === 'GET')) {
        clearSessionCookie(res);
        if (req.method === 'GET') {
            res.writeHead(302, { Location: '/login' });
            return res.end();
        }
        return jsonReply(res, 200, { ok: true });
    }

    if (urlPath === '/auth/me' && req.method === 'GET') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        return jsonReply(res, 200, { user: { email: u.email, name: u.name, isAdmin: !!u.isAdmin, mustChangePassword: !!u.mustChangePassword } });
    }

    if (urlPath === '/auth/change-password' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        const body = await readJSON(req);
        const currentPassword = String(body.currentPassword || '');
        const newPassword = String(body.newPassword || '');
        if (!newPassword || newPassword.length < 8) return jsonReply(res, 400, { error: 'A nova senha deve ter ao menos 8 caracteres.' });
        let ok = false;
        try { ok = bcrypt.compareSync(currentPassword, u.passwordHash); } catch (e) {}
        if (!ok) return jsonReply(res, 401, { error: 'Senha atual incorreta.' });
        if (bcrypt.compareSync(newPassword, u.passwordHash)) return jsonReply(res, 400, { error: 'A nova senha deve ser diferente da atual.' });
        const newHash = bcrypt.hashSync(newPassword, 10);
        try { await updateUser(u.email, { passwordHash: newHash, mustChangePassword: false, passwordChangedAt: new Date().toISOString() }); }
        catch (e) { return jsonReply(res, 500, { error: 'Falha ao salvar nova senha. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true });
    }

    if (urlPath === '/auth/forgot-password' && req.method === 'POST') {
        const body = await readJSON(req);
        const email = String(body.email || '').toLowerCase().trim();
        // Resposta sempre "ok" pra não vazar quais e-mails existem
        if (!email) return jsonReply(res, 200, { ok: true });
        const user = findUser(email);
        if (user) {
            const token = signToken({ email: user.email, purpose: 'reset' }, RESET_TOKEN_TTL_MS);
            const link = `${APP_BASE_URL}/reset-password?token=${encodeURIComponent(token)}`;
            await sendResetEmail(user.email, user.name, link);
        }
        return jsonReply(res, 200, { ok: true });
    }

    if (urlPath === '/auth/reset-password' && req.method === 'POST') {
        const body = await readJSON(req);
        const token = String(body.token || '');
        const newPassword = String(body.newPassword || '');
        if (!newPassword || newPassword.length < 8) return jsonReply(res, 400, { error: 'A nova senha deve ter ao menos 8 caracteres.' });
        const payload = verifyToken(token);
        if (!payload || payload.purpose !== 'reset' || !payload.email) return jsonReply(res, 400, { error: 'Link inválido ou expirado.' });
        const user = findUser(payload.email);
        if (!user) return jsonReply(res, 400, { error: 'Usuário não encontrado.' });
        const newHash = bcrypt.hashSync(newPassword, 10);
        try { await updateUser(user.email, { passwordHash: newHash, mustChangePassword: false, passwordChangedAt: new Date().toISOString() }); }
        catch (e) { return jsonReply(res, 500, { error: 'Falha ao salvar. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true });
    }

    // ==================== Admin API endpoints (só isAdmin=true) ====================
    if (urlPath === '/api/admin/users' && req.method === 'GET') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        if (!u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const users = loadUsers().map(x => ({
            email: x.email,
            name: x.name,
            isAdmin: !!x.isAdmin,
            mustChangePassword: !!x.mustChangePassword,
            passwordChangedAt: x.passwordChangedAt || null,
            lastLoginAt: x.lastLoginAt || null,
            createdAt: x.createdAt || null,
            hasMcpToken: !!x.mcpTokenHash,
            mcpTokenCreatedAt: x.mcpTokenCreatedAt || null
        }));
        return jsonReply(res, 200, { users });
    }

    if (urlPath === '/api/admin/_health' && req.method === 'GET') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const status = {
            boot: redisStatusAtBoot,
            redis: {
                urlEnvSet: !!process.env.UPSTASH_REDIS_REST_URL,
                tokenEnvSet: !!process.env.UPSTASH_REDIS_REST_TOKEN,
                urlHost: REDIS_URL ? (function(){ try { return new URL(REDIS_URL).hostname; } catch(e){return 'INVALID:'+REDIS_URL.slice(0,40);} })() : null,
                tokenLen: REDIS_TOKEN.length,
                tokenPrefix: REDIS_TOKEN ? REDIS_TOKEN.slice(0, 6) + '…' : null
            },
            users: { cached: usersCache.length, cacheReady: usersCacheReady }
        };
        if (REDIS_URL && REDIS_TOKEN) {
            try { status.redis.livePing = await redisCmd(['PING']); }
            catch (e) { status.redis.livePingError = e.message; }
            try { status.redis.liveGet = (await redisCmd(['GET', REDIS_KEY])) ? 'key_exists' : 'key_missing'; }
            catch (e) { status.redis.liveGetError = e.message; }
        }
        return jsonReply(res, 200, status);
    }

    if (urlPath === '/api/admin/reset-user-password' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        if (!u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        if (!targetEmail) return jsonReply(res, 400, { error: 'email obrigatório' });
        const target = findUser(targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'usuário não encontrado' });
        const INITIAL = 'Lincros2026!';
        const newHash = bcrypt.hashSync(INITIAL, 10);
        try {
            await updateUser(target.email, {
                passwordHash: newHash,
                mustChangePassword: true,
                passwordChangedAt: null,
                resetByAdminAt: new Date().toISOString(),
                resetByAdminEmail: u.email
            });
        } catch (e) { return jsonReply(res, 500, { error: 'Falha ao resetar. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true, message: `Senha de ${target.name} resetada para a senha inicial. O usuário será obrigado a trocá-la no próximo login.` });
    }

    if (urlPath === '/api/admin/add-user' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const email = String(body.email || '').toLowerCase().trim();
        const name = String(body.name || '').trim();
        const isAdmin = !!body.isAdmin;
        if (!email || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return jsonReply(res, 400, { error: 'E-mail inválido' });
        if (!name || name.length < 2) return jsonReply(res, 400, { error: 'Nome muito curto (mín. 2 caracteres)' });
        if (findUser(email)) return jsonReply(res, 409, { error: 'Já existe um usuário com este e-mail' });
        const INITIAL = 'Lincros2026!';
        const users = loadUsers().slice();
        users.push({
            email, name,
            passwordHash: bcrypt.hashSync(INITIAL, 10),
            isAdmin,
            mustChangePassword: true,
            createdAt: new Date().toISOString(),
            createdBy: u.email,
            passwordChangedAt: null,
            lastLoginAt: null
        });
        try { await saveUsers(users); }
        catch (e) { return jsonReply(res, 500, { error: 'Falha ao salvar. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true, message: `${name} adicionado(a) com sucesso. Senha inicial: ${INITIAL}` });
    }

    if (urlPath === '/api/admin/delete-user' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        if (!targetEmail) return jsonReply(res, 400, { error: 'email obrigatório' });
        if (targetEmail === u.email) return jsonReply(res, 400, { error: 'Você não pode excluir a própria conta' });
        const users = loadUsers();
        const target = users.find(x => x.email === targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'Usuário não encontrado' });
        const filtered = users.filter(x => x.email !== targetEmail);
        try { await saveUsers(filtered); }
        catch (e) { return jsonReply(res, 500, { error: 'Falha ao excluir. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true, message: `${target.name} foi excluído(a) do portal.` });
    }

    if (urlPath === '/api/admin/toggle-admin' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        const makeAdmin = !!body.isAdmin;
        if (!targetEmail) return jsonReply(res, 400, { error: 'email obrigatório' });
        if (targetEmail === u.email && !makeAdmin) return jsonReply(res, 400, { error: 'Você não pode remover seu próprio acesso de admin' });
        const target = findUser(targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'Usuário não encontrado' });
        try { await updateUser(target.email, { isAdmin: makeAdmin, adminChangedAt: new Date().toISOString(), adminChangedBy: u.email }); }
        catch (e) { return jsonReply(res, 500, { error: 'Falha ao atualizar. Tente novamente.' }); }
        return jsonReply(res, 200, { ok: true, message: makeAdmin ? `${target.name} agora é administrador(a).` : `${target.name} não é mais administrador(a).` });
    }

    // ========== MCP Token Management ==========
    // Gera token MCP pra um usuário (só admin). Retorna o token EM CLARO uma única vez,
    // depois armazenamos apenas o hash. Se o usuário perder, gera de novo.
    if (urlPath === '/api/admin/generate-mcp-token' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        const target = findUser(targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'Usuário não encontrado' });
        // Gera token: prefix "mcp_" + 40 chars random hex
        const token = 'mcp_' + crypto.randomBytes(20).toString('hex');
        const tokenHash = crypto.createHash('sha256').update(token).digest('hex');
        try {
            await updateUser(target.email, {
                mcpTokenHash: tokenHash,
                mcpTokenCreatedAt: new Date().toISOString(),
                mcpTokenCreatedBy: u.email
            });
        } catch (e) { return jsonReply(res, 500, { error: 'Falha ao salvar token' }); }
        return jsonReply(res, 200, { ok: true, token, message: `Token MCP gerado pra ${target.name}. ATENÇÃO: copie agora, ele não será mostrado de novo.` });
    }

    if (urlPath === '/api/admin/revoke-mcp-token' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u || !u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        const target = findUser(targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'Usuário não encontrado' });
        try {
            await updateUser(target.email, {
                mcpTokenHash: null,
                mcpTokenCreatedAt: null,
                mcpTokenRevokedAt: new Date().toISOString(),
                mcpTokenRevokedBy: u.email
            });
        } catch (e) { return jsonReply(res, 500, { error: 'Falha ao revogar token' }); }
        return jsonReply(res, 200, { ok: true, message: `Token MCP de ${target.name} revogado.` });
    }

    // ========== MCP Query Endpoint (auth via Bearer token) ==========
    // Esse endpoint NÃO usa sessão de cookie — usa Bearer token gerado pelo admin.
    // É chamado pelo servidor MCP rodando no PC do líder via Claude Desktop.
    if (urlPath === '/api/mcp/query' && req.method === 'POST') {
        const authHeader = req.headers['authorization'] || '';
        const m = authHeader.match(/^Bearer\s+(mcp_[a-f0-9]{40})\s*$/i);
        if (!m) return jsonReply(res, 401, { error: 'Bearer token MCP requerido' });
        const providedToken = m[1];
        const providedHash = crypto.createHash('sha256').update(providedToken).digest('hex');
        const mcpUser = loadUsers().find(u => u.mcpTokenHash === providedHash);
        if (!mcpUser) return jsonReply(res, 401, { error: 'Token MCP inválido ou revogado' });

        const body = await readJSON(req);
        const queryType = String(body.type || '').toLowerCase().trim();
        const params = body.params || {};

        // Carrega caches locais pra responder (sem auth de cookie)
        const won = (responseCache.won?.data?.value || []);
        const lost = (responseCache.lost?.data?.value || []);
        const open = (responseCache.open?.data?.value || []);

        const SALES_PIPELINES = ['Funil de Vendas', 'Sankhya', 'Farmer', 'Farmer IPCA'];
        const NEW_BIZ = ['Funil de Vendas', 'Sankhya'];
        const FARMER_PIP = ['Farmer', 'Farmer IPCA'];
        const FIELD_MRR = 'deal_1F7F1DEC-39B3-4621-9237-96D7793DAD03';
        const FIELD_SETUP = 'deal_90CB9147-95C6-4A5F-8607-A2B5225ADFC3';
        const getMRR = (d) => { const p = (d.OtherProperties || []).find(x => x.FieldKey === FIELD_MRR); return p?.DecimalValue || 0; };
        const getSetup = (d) => { const p = (d.OtherProperties || []).find(x => x.FieldKey === FIELD_SETUP); return p?.DecimalValue || 0; };
        const getPipelineName = (d) => d.Pipeline?.Name || '';
        const isSalesPipeline = (d) => SALES_PIPELINES.includes(getPipelineName(d));
        const getOwnerName = (d) => d.Owner?.Name || '';

        // Filtra por período
        const periodToRange = (period) => {
            const now = new Date();
            const y = now.getFullYear(), m = now.getMonth();
            if (period === 'this_month') return { start: new Date(y,m,1), end: new Date(y,m+1,0,23,59,59) };
            if (period === 'last_month') return { start: new Date(y,m-1,1), end: new Date(y,m,0,23,59,59) };
            if (period === 'this_quarter') { const q = Math.floor(m/3)*3; return { start: new Date(y,q,1), end: new Date(y,q+3,0,23,59,59) }; }
            if (period === 'last_6_months') return { start: new Date(y,m-6,1), end: new Date(y,m+1,0,23,59,59) };
            if (period === 'this_year') return { start: new Date(y,0,1), end: new Date(y,11,31,23,59,59) };
            return null; // global
        };
        const inRange = (iso, range) => { if (!range) return true; if (!iso) return false; const t = new Date(iso).getTime(); return t >= range.start.getTime() && t <= range.end.getTime(); };

        // Filtro de permissão: admin vê tudo, leader só seu pipeline, vendedor só seus deals
        const isAdmin = !!mcpUser.isAdmin;
        const PIPELINE_LEADERS = {
            'jessica.fenske@lincros.com': 'Funil de Vendas',
            'jessica.martins@lincros.com': 'Farmer',
            'luis.esteves@lincros.com': 'Sankhya'
        };
        const leaderPipeline = PIPELINE_LEADERS[mcpUser.email.toLowerCase()];
        const canSeeDeal = (d) => {
            if (!isSalesPipeline(d)) return false;
            if (isAdmin) return true;
            if (leaderPipeline) {
                if (leaderPipeline === 'Funil de Vendas' || leaderPipeline === 'Sankhya') {
                    return NEW_BIZ.includes(getPipelineName(d)); // novos negocios vê funil + sankhya
                }
                return FARMER_PIP.includes(getPipelineName(d));
            }
            // Vendedor comum: só seus deals
            return getOwnerName(d) === mcpUser.name;
        };

        let result;
        switch (queryType) {
            case 'whoami':
                result = {
                    name: mcpUser.name,
                    email: mcpUser.email,
                    accessLevel: isAdmin ? 'admin' : (leaderPipeline ? 'leader' : 'vendedor'),
                    leaderPipeline: leaderPipeline || null,
                    visibleDeals: (won.concat(lost, open)).filter(canSeeDeal).length
                };
                break;
            case 'overview': {
                const range = periodToRange(params.period || 'this_month');
                const wonInRange = won.filter(d => canSeeDeal(d) && inRange(d.FinishDate, range));
                const lostInRange = lost.filter(d => canSeeDeal(d) && inRange(d.FinishDate, range));
                const openVisible = open.filter(canSeeDeal);
                const totalMrr = wonInRange.filter(d => getPipelineName(d) !== 'Farmer IPCA').reduce((s,d) => s + getMRR(d), 0);
                const totalSetup = wonInRange.reduce((s,d) => s + getSetup(d), 0);
                result = {
                    period: params.period || 'this_month',
                    wonCount: wonInRange.length,
                    lostCount: lostInRange.length,
                    openCount: openVisible.length,
                    openMrrPotential: openVisible.reduce((s,d) => s + getMRR(d), 0),
                    totalMrrNew: totalMrr,
                    totalSetup,
                    winRate: (wonInRange.length + lostInRange.length) > 0 ? Math.round(wonInRange.length / (wonInRange.length + lostInRange.length) * 100) : 0,
                    avgTicketMrr: wonInRange.length > 0 ? Math.round(totalMrr / wonInRange.length) : 0
                };
                break;
            }
            case 'funnel_metrics': {
                const range = periodToRange(params.period || 'last_6_months');
                const wonInRange = won.filter(d => canSeeDeal(d) && inRange(d.FinishDate, range));
                const lostInRange = lost.filter(d => canSeeDeal(d) && inRange(d.FinishDate, range));
                const isNewBiz = (d) => NEW_BIZ.includes(getPipelineName(d));
                const isFarmer = (d) => FARMER_PIP.includes(getPipelineName(d));
                const aggregate = (deals, filter) => {
                    const map = {};
                    deals.filter(filter).forEach(d => {
                        const o = getOwnerName(d); if (!o) return;
                        if (!map[o]) map[o] = { won: 0, lost: 0, mrrSum: 0, cycleSum: 0, cycleCount: 0 };
                        map[o].lost++; // overwritten by won below
                    });
                    return map;
                };
                const calcModelMetrics = (filter) => {
                    const map = {};
                    wonInRange.filter(filter).forEach(d => {
                        const o = getOwnerName(d); if (!o) return;
                        if (!map[o]) map[o] = { won: 0, lost: 0, mrrSum: 0, cycleSum: 0, cycleCount: 0 };
                        map[o].won++;
                        map[o].mrrSum += getMRR(d);
                        if (d.CreateDate && d.FinishDate) {
                            const days = (new Date(d.FinishDate) - new Date(d.CreateDate)) / 86400000;
                            if (days >= 0) { map[o].cycleSum += days; map[o].cycleCount++; }
                        }
                    });
                    lostInRange.filter(filter).forEach(d => {
                        const o = getOwnerName(d); if (!o) return;
                        if (!map[o]) map[o] = { won: 0, lost: 0, mrrSum: 0, cycleSum: 0, cycleCount: 0 };
                        map[o].lost++;
                    });
                    return Object.entries(map).map(([owner, v]) => ({
                        owner,
                        won: v.won, lost: v.lost,
                        cr: (v.won + v.lost) > 0 ? Math.round(v.won / (v.won + v.lost) * 100) : null,
                        vm: v.won > 0 ? Math.round(v.mrrSum / v.won) : null,
                        deltaT: v.cycleCount > 0 ? Math.round(v.cycleSum / v.cycleCount) : null
                    })).sort((a, b) => (b.cr || 0) - (a.cr || 0));
                };
                result = {
                    period: params.period || 'last_6_months',
                    novosNegocios: calcModelMetrics(isNewBiz),
                    farmer: calcModelMetrics(isFarmer)
                };
                break;
            }
            case 'vendor_performance': {
                const name = params.vendor || '';
                if (!name) return jsonReply(res, 400, { error: 'param "vendor" obrigatório' });
                const norm = (s) => (s || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
                const match = (d) => norm(getOwnerName(d)).includes(norm(name));
                const range = periodToRange(params.period || 'this_quarter');
                const vWon = won.filter(d => canSeeDeal(d) && match(d) && inRange(d.FinishDate, range));
                const vLost = lost.filter(d => canSeeDeal(d) && match(d) && inRange(d.FinishDate, range));
                const vOpen = open.filter(d => canSeeDeal(d) && match(d));
                result = {
                    vendor: name,
                    period: params.period || 'this_quarter',
                    wonCount: vWon.length,
                    lostCount: vLost.length,
                    openCount: vOpen.length,
                    totalMrr: vWon.filter(d => getPipelineName(d) !== 'Farmer IPCA').reduce((s,d) => s + getMRR(d), 0),
                    openMrr: vOpen.reduce((s,d) => s + getMRR(d), 0),
                    winRate: (vWon.length + vLost.length) > 0 ? Math.round(vWon.length / (vWon.length + vLost.length) * 100) : 0,
                    avgCycle: (function() {
                        const cycles = vWon.filter(d => d.CreateDate && d.FinishDate).map(d => (new Date(d.FinishDate) - new Date(d.CreateDate)) / 86400000);
                        return cycles.length > 0 ? Math.round(cycles.reduce((s,c)=>s+c,0) / cycles.length) : null;
                    })(),
                    topOpenDeals: vOpen.sort((a,b) => getMRR(b) - getMRR(a)).slice(0, 5).map(d => ({
                        title: d.Title, mrr: getMRR(d), setup: getSetup(d), stage: d.Stage?.Name, pipeline: getPipelineName(d), daysOpen: Math.floor((Date.now() - new Date(d.CreateDate).getTime()) / 86400000)
                    }))
                };
                break;
            }
            case 'deals_at_risk': {
                const visibleOpen = open.filter(canSeeDeal);
                const risks = visibleOpen.map(d => {
                    const daysSinceUpdate = Math.floor((Date.now() - new Date(d.LastUpdateDate || d.CreateDate).getTime()) / 86400000);
                    let reason = null, severity = 0;
                    if (daysSinceUpdate >= 21 && (d.Stage?.Ordination ?? -1) >= 2) { reason = `Estagnado em ${d.Stage?.Name} há ${daysSinceUpdate}d`; severity = 3; }
                    else if (daysSinceUpdate >= 14) { reason = `Sem atividade há ${daysSinceUpdate}d`; severity = 2; }
                    else if (daysSinceUpdate >= 7 && getMRR(d) > 5000) { reason = `Deal grande sem atividade há ${daysSinceUpdate}d`; severity = 1; }
                    return reason ? { title: d.Title, owner: getOwnerName(d), mrr: getMRR(d), stage: d.Stage?.Name, pipeline: getPipelineName(d), daysSinceUpdate, reason, severity } : null;
                }).filter(Boolean).sort((a, b) => b.severity - a.severity || b.daysSinceUpdate - a.daysSinceUpdate).slice(0, params.limit || 20);
                result = { deals: risks, count: risks.length };
                break;
            }
            case 'top_sellers': {
                const range = periodToRange(params.period || 'this_month');
                const sellerMrr = {};
                won.filter(d => canSeeDeal(d) && inRange(d.FinishDate, range) && getPipelineName(d) !== 'Farmer IPCA').forEach(d => {
                    const o = getOwnerName(d); if (!o) return;
                    sellerMrr[o] = (sellerMrr[o] || { count: 0, mrr: 0 });
                    sellerMrr[o].count++;
                    sellerMrr[o].mrr += getMRR(d);
                });
                result = {
                    period: params.period || 'this_month',
                    sellers: Object.entries(sellerMrr).sort((a,b) => b[1].mrr - a[1].mrr).slice(0, params.limit || 10).map(([name, v]) => ({ name, mrr: v.mrr, deals: v.count }))
                };
                break;
            }
            default:
                return jsonReply(res, 400, { error: `tipo de query desconhecido: ${queryType}. Tipos válidos: whoami, overview, funnel_metrics, vendor_performance, deals_at_risk, top_sellers` });
        }

        return jsonReply(res, 200, { ok: true, data: result, queryType, accessLevel: isAdmin ? 'admin' : (leaderPipeline ? 'leader' : 'vendedor') });
    }

    if (urlPath === '/api/admin/send-reminder' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        if (!u.isAdmin) return jsonReply(res, 403, { error: 'forbidden' });
        const body = await readJSON(req);
        const targetEmail = String(body.email || '').toLowerCase().trim();
        const target = findUser(targetEmail);
        if (!target) return jsonReply(res, 404, { error: 'usuário não encontrado' });
        if (!RESEND_API_KEY) return jsonReply(res, 500, { error: 'Serviço de e-mail não configurado' });
        const html = `
            <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px;color:#222">
                <h2 style="color:#1a1a2e">Portal de Oportunidades Lincros</h2>
                <p>Olá, ${target.name}.</p>
                <p>Notamos que você ainda <strong>não acessou o Portal de Oportunidades</strong> ou ainda não trocou sua senha inicial.</p>
                <p>Para acessar o portal, use:</p>
                <ul>
                    <li><strong>E-mail:</strong> ${target.email}</li>
                    <li><strong>Senha inicial:</strong> <code style="background:#f3f4f6;padding:2px 6px;border-radius:4px">Lincros2026!</code></li>
                </ul>
                <p style="margin:28px 0">
                    <a href="${APP_BASE_URL}" style="background:#6c3fb5;color:#fff;padding:12px 22px;border-radius:8px;text-decoration:none;font-weight:600">Acessar o Portal</a>
                </p>
                <p style="font-size:13px;color:#666">No primeiro acesso, você será solicitado a definir uma nova senha pessoal (mínimo 8 caracteres).</p>
                <p style="font-size:12px;color:#999;margin-top:30px">— Lembrete enviado por ${u.name}</p>
            </div>
        `;
        await new Promise((resolve) => {
            const payload = JSON.stringify({ from: RESEND_FROM, to: [target.email], subject: 'Lembrete: acesso ao Portal de Oportunidades', html });
            const opts = {
                hostname: 'api.resend.com', path: '/emails', method: 'POST',
                headers: { 'Authorization': 'Bearer ' + RESEND_API_KEY, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
            };
            const r = https.request(opts, (rr) => { rr.on('data', () => {}); rr.on('end', () => resolve(rr.statusCode)); });
            r.on('error', () => resolve(0)); r.write(payload); r.end();
        });
        return jsonReply(res, 200, { ok: true, message: `Lembrete enviado para ${target.email}` });
    }

    // ==================== Chat IA "Lia" (somente Claude Haiku 4.5 / Anthropic) ====================
    // Provider Gemini foi descontinuado por razões de privacidade — só Claude (Anthropic, no-training).
    // ==================== Marketing Ads endpoints ====================
    if (urlPath === '/api/marketing/google-ads' && req.method === 'GET') {
        const user = getCurrentUser(req);
        if (!user) return jsonReply(res, 401, { error: 'not authenticated' });
        const force = req.url.includes('refresh=1');
        try {
            const data = await googleAdsGetCached(force);
            return jsonReply(res, 200, data);
        } catch (e) {
            return jsonReply(res, 502, { error: e.message, source: 'google-ads' });
        }
    }
    if (urlPath === '/api/marketing/meta-ads' && req.method === 'GET') {
        const user = getCurrentUser(req);
        if (!user) return jsonReply(res, 401, { error: 'not authenticated' });
        const force = req.url.includes('refresh=1');
        try {
            const data = await metaAdsGetCached(force);
            return jsonReply(res, 200, data);
        } catch (e) {
            return jsonReply(res, 502, { error: e.message, source: 'meta-ads' });
        }
    }
    if (urlPath === '/api/marketing/linkedin-ads' && req.method === 'GET') {
        const user = getCurrentUser(req);
        if (!user) return jsonReply(res, 401, { error: 'not authenticated' });
        const force = req.url.includes('refresh=1');
        try {
            const data = await linkedinAdsGetCached(force);
            return jsonReply(res, 200, data);
        } catch (e) {
            return jsonReply(res, 502, { error: e.message, source: 'linkedin-ads' });
        }
    }
    // Endpoint combinado — única chamada do frontend pra puxar tudo
    if (urlPath === '/api/marketing/ads-summary' && req.method === 'GET') {
        const user = getCurrentUser(req);
        if (!user) return jsonReply(res, 401, { error: 'not authenticated' });
        const force = req.url.includes('refresh=1');
        const [google, meta, linkedin] = await Promise.all([
            googleAdsGetCached(force).catch(e => ({ error: e.message, source: 'google-ads' })),
            metaAdsGetCached(force).catch(e => ({ error: e.message, source: 'meta-ads' })),
            linkedinAdsGetCached(force).catch(e => ({ error: e.message, source: 'linkedin-ads' }))
        ]);
        return jsonReply(res, 200, {
            fetchedAt: new Date().toISOString(),
            config: {
                googleAdsConfigured: isGoogleAdsConfigured(),
                metaAdsConfigured: isMetaAdsConfigured(),
                linkedinAdsConfigured: isLinkedinAdsConfigured()
            },
            google,
            meta,
            linkedin
        });
    }

    if (urlPath === '/api/chat' && req.method === 'POST') {
        const u = getCurrentUser(req);
        if (!u) return jsonReply(res, 401, { error: 'not authenticated' });
        // Só funciona com Claude (Anthropic). Sem essa key configurada, recusa.
        if (!ANTHROPIC_API_KEY) {
            return jsonReply(res, 503, { error: 'IA temporariamente indisponível — aguardando configuração da chave Anthropic. Fale com a Jessica.' });
        }
        const useClaude = true; // sempre Claude (Gemini fallback removido)

        const body = await readJSON(req);
        const messages = body.messages || [];
        const dataContext = body.dataContext || {};
        const userInfo = body.userInfo || {};

        // Validações simples
        if (!messages.length) return jsonReply(res, 400, { error: 'mensagens vazias' });
        if (messages.length > 30) return jsonReply(res, 400, { error: 'histórico muito longo' });
        const lastUserMsg = messages[messages.length - 1];
        if (!lastUserMsg || lastUserMsg.role !== 'user') return jsonReply(res, 400, { error: 'última mensagem deve ser do usuário' });
        if (lastUserMsg.text && lastUserMsg.text.length > 2000) return jsonReply(res, 400, { error: 'pergunta muito longa (máx 2000 caracteres)' });

        // Determina nível de acesso
        const accessLevel = u.isAdmin ? 'admin'
            : (userInfo.isLeader ? 'leader' : 'vendedor');
        const userName = u.name || u.email;

        // System prompt
        const systemPrompt = `Você é a "Lia", assistente de IA da Lincros pra vendas e operações comerciais.

CONTEXTO DO USUÁRIO LOGADO:
- Nome: ${userName}
- E-mail: ${u.email}
- Papel: ${accessLevel === 'admin' ? 'Administrador(a)' : accessLevel === 'leader' ? `Líder de funil (${userInfo.leaderPipeline || 'pipeline atribuído'})` : 'Vendedor(a)'}

REGRAS DE ACESSO (CRÍTICO — NÃO QUEBRE):
${accessLevel === 'admin' ? '- Você pode ver e analisar dados de TODOS os vendedores, funis e pipelines.' : ''}
${accessLevel === 'leader' ? `- Você pode ver dados do funil "${userInfo.leaderPipeline}" e todos os vendedores DESSE funil.\n- Para outros funis, oriente o usuário a falar com o líder respectivo.` : ''}
${accessLevel === 'vendedor' ? '- Você só pode ver dados PESSOAIS do usuário (deals dele, performance dele).\n- Se ele perguntar sobre OUTRO vendedor especificamente (ex: "como tá o Bruno?"), recuse educadamente: "Você não tem acesso a métricas individuais de outros vendedores. Fala com a sua liderança se precisar disso."\n- Pode mostrar dados agregados do funil (ex: total ganho do mês), mas NUNCA performance individual de outro vendedor.' : ''}

DADOS DISPONÍVEIS NESTA CONVERSA:
${JSON.stringify(dataContext, null, 2).slice(0, 8000)}

DIRETRIZES:
1. Use português brasileiro casual mas profissional. Pode usar "vc" ocasionalmente, mas sem exagero.
2. Seja CONCISO e ACIONÁVEL. Vendedor não tem tempo pra ler parágrafo longo.
3. Cite números específicos sempre que tiver dado.
4. Use frameworks: MEDDIC (Metrics, Economic Buyer, Decision Criteria, Decision Process, Identify Pain, Champion), SPIN (Situation/Problem/Implication/Need-payoff), SPICED (Situation, Pain, Impact, Critical Event, Decision Criteria) quando relevante.
5. Sugira AÇÃO CONCRETA, não só análise. Ex: "Marque uma call de qualificação com X" em vez de "considere uma call".
6. Se faltar dado pra responder, diga: "Não tenho esse dado aqui — confira em [seção do dashboard]" + sugira o caminho.
7. Formate respostas em markdown leve (negrito **assim**, listas com -, links). Pode usar emojis com moderação.
8. Quando o usuário pedir cálculo (ex: "quanto preciso fechar pra bater meta"), faça a conta com os dados que tem.
9. NUNCA invente dados. Se não tem informação, fala que não tem.
10. Se a pergunta for fora de escopo (vendas/operação Lincros), redirecione gentilmente.`;

        // Anthropic Claude API
        const claudeMessages = messages.map(m => ({
            role: m.role === 'assistant' ? 'assistant' : 'user',
            content: m.text || ''
        }));
        const payload = JSON.stringify({
            model: ANTHROPIC_MODEL,
            max_tokens: 1024,
            system: systemPrompt,
            messages: claudeMessages,
            temperature: 0.7
        });
        const opts = {
            hostname: 'api.anthropic.com',
            path: '/v1/messages',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload),
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01'
            }
        };

        const reply = await new Promise((resolve) => {
            const r = https.request(opts, (rr) => {
                const chunks = [];
                rr.on('data', c => chunks.push(c));
                rr.on('end', () => {
                    const raw = Buffer.concat(chunks).toString('utf8');
                    try {
                        const json = JSON.parse(raw);
                        if (rr.statusCode < 200 || rr.statusCode >= 300) {
                            console.error('[chat] Claude HTTP', rr.statusCode, raw.slice(0, 400));
                            const msg = json.error?.message || (typeof json.error === 'string' ? json.error : `HTTP ${rr.statusCode}`);
                            return resolve({ error: 'Erro na IA: ' + msg });
                        }
                        // Claude: content array de blocks, pegamos os blocks de texto
                        const text = (json.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
                        if (!text) return resolve({ error: 'Sem resposta da IA' });
                        resolve({ text });
                    } catch (e) {
                        console.error('[chat] parse error:', e.message);
                        resolve({ error: 'Erro ao processar resposta da IA' });
                    }
                });
            });
            r.on('error', (e) => { console.error('[chat] request error:', e.message); resolve({ error: 'Falha de conexão com a IA' }); });
            r.setTimeout(30000, () => { r.destroy(); resolve({ error: 'IA demorou demais pra responder' }); });
            r.write(payload);
            r.end();
        });

        if (reply.error) return jsonReply(res, 502, { error: reply.error });
        return jsonReply(res, 200, { text: reply.text });
    }

    // ==================== Middleware de proteção ====================
    if (!isPublicPath(req.url)) {
        const u = getCurrentUser(req);
        if (!u) {
            // Pra requisições HTML: redireciona pro login. Pra API/cache: 401 JSON.
            const accepts = req.headers['accept'] || '';
            const wantsHtml = accepts.includes('text/html') && req.method === 'GET';
            if (wantsHtml) {
                res.writeHead(302, { Location: '/login' });
                return res.end();
            }
            return jsonReply(res, 401, { error: 'not authenticated' });
        }
        // Se senha precisa ser trocada e o usuário tá tentando carregar o app principal, força change-password
        if (u.mustChangePassword && (urlPath === '/' || urlPath === '/index.html')) {
            res.writeHead(302, { Location: '/change-password' });
            return res.end();
        }
    }

    // Rewrite limpo: /login -> /login.html etc
    if (urlPath === '/login') { req.url = '/login.html' + (req.url.slice('/login'.length).replace(/^[^?]*/, '')); }
    if (urlPath === '/forgot-password') { req.url = '/forgot-password.html' + (req.url.slice('/forgot-password'.length).replace(/^[^?]*/, '')); }
    if (urlPath === '/reset-password') { req.url = '/reset-password.html' + (req.url.slice('/reset-password'.length).replace(/^[^?]*/, '')); }
    if (urlPath === '/change-password') { req.url = '/change-password.html' + (req.url.slice('/change-password'.length).replace(/^[^?]*/, '')); }
    if (urlPath === '/admin/users') { req.url = '/admin-users.html' + (req.url.slice('/admin/users'.length).replace(/^[^?]*/, '')); }

    // Sankhya API (autenticada)
    if (req.url.startsWith('/api/sankhya/')) {
        const u = new URL(req.url, 'http://localhost');
        const query = Object.fromEntries(u.searchParams);
        return handleSankhyaApi(req, res, u.pathname, query);
    }

    if (req.url === '/keepalive') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        return res.end('ok');
    }

    if (req.url.startsWith('/cache/')) {
        const key = req.url.split('/')[2].split('?')[0];
        const cached = responseCache[key];
        if (!cached) {
            res.writeHead(202, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({
                status: 'loading',
                refreshing: cache.refreshing[key] || false,
                message: 'Cache ainda carregando, tente novamente em alguns segundos'
            }));
        }
        const acceptEncoding = req.headers['accept-encoding'] || '';
        if (acceptEncoding.includes('gzip')) {
            res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Encoding': 'gzip', 'Cache-Control': 'public, max-age=60' });
            return res.end(cached.gzipped);
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' });
        return res.end(cached.body);
    }

    if (req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            lastRefresh: cache.lastRefresh,
            refreshing: cache.refreshing,
            counts: {
                won: cache.won?.length || 0,
                lost: cache.lost?.length || 0,
                open: cache.open?.length || 0,
                contacts: cache.contacts?.length || 0,
                forecast: cache.forecast?.length || 0,
                orders: cache.orders?.length || 0,
                people: cache.people?.length || 0,
                meetings: cache.meetings?.length || 0,
                users: cache.users?.length || 0
            }
        }));
    }

    if (req.url === '/refresh') {
        refreshAll();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: 'refreshing' }));
    }

    // RD Station - tenta varios formatos de auth ate encontrar o que funciona
    if (req.url.startsWith('/rdstation/')) {
        const subPath = req.url.replace('/rdstation/', '');
        const m = subPath.match(/^([^?]+)(\?.*)?$/);
        const resource = m ? m[1] : subPath;
        const qs = m && m[2] ? m[2] : '';
        const sep = qs ? '&' : '?';
        // Endpoints possiveis a tentar
        const attempts = [
            // 1) API legada GET /conversions - eh o endpoint correto pra listar conversoes (leads)
            { host: 'www.rdstation.com.br', path: '/api/1.2/conversions.json' + qs + sep + 'auth_token=' + RD_PRIVATE_TOKEN, auth: 'none', label: 'rdstation.com.br/api/1.2/conversions + auth_token' },
            { host: 'www.rdstation.com.br', path: '/api/1.3/conversions' + qs + sep + 'auth_token=' + RD_PRIVATE_TOKEN, auth: 'none', label: 'rdstation.com.br/api/1.3/conversions + auth_token' },
            // 2) Tenta tambem o que o usuario pediu (resource) caso seja diferente de "leads"
            { host: 'www.rdstation.com.br', path: '/api/1.2/' + resource + qs + sep + 'auth_token=' + RD_PRIVATE_TOKEN, auth: 'none', label: 'rdstation.com.br/api/1.2/' + resource + ' + auth_token' },
            // 3) API moderna com Bearer (precisa OAuth, pode falhar)
            { host: 'api.rd.services', path: '/platform/contacts' + qs, auth: 'bearer', label: 'api.rd.services platform/contacts (Bearer)' },
            // 4) Header X-Auth-Token (alguns endpoints aceitam)
            { host: 'api.rd.services', path: '/platform/contacts' + qs, auth: 'xauth', label: 'api.rd.services X-Auth-Token' }
        ];
        const errors = [];
        function tryRd(idx) {
            if (idx >= attempts.length) {
                res.writeHead(502, { 'Content-Type': 'application/json' });
                return res.end(JSON.stringify({
                    error: 'RD Station: nenhum endpoint disponivel respondeu OK',
                    hint: 'Para listar leads no RD Marketing eh necessario OAuth (Bearer token gerado via fluxo de autorizacao). O token privado so funciona em alguns endpoints legados.',
                    attempts: errors
                }));
            }
            const a = attempts[idx];
            const opts = {
                hostname: a.host,
                path: a.path,
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            };
            if (a.auth === 'bearer') opts.headers['Authorization'] = 'Bearer ' + RD_PRIVATE_TOKEN;
            if (a.auth === 'xauth') opts.headers['X-Auth-Token'] = RD_PRIVATE_TOKEN;
            const r = https.request(opts, (rr) => {
                const chunks = [];
                rr.on('data', c => chunks.push(c));
                rr.on('end', () => {
                    const body = Buffer.concat(chunks).toString('utf8');
                    if (rr.statusCode >= 200 && rr.statusCode < 300) {
                        res.writeHead(200, { 'Content-Type': 'application/json' });
                        return res.end(body);
                    }
                    errors.push({ endpoint: a.label, status: rr.statusCode, body: body.substring(0, 200) });
                    tryRd(idx + 1);
                });
            });
            r.on('error', (e) => { errors.push({ endpoint: a.label, error: e.message }); tryRd(idx + 1); });
            r.setTimeout(10000, () => { r.destroy(); errors.push({ endpoint: a.label, error: 'timeout' }); tryRd(idx + 1); });
            r.end();
        }
        return tryRd(0);
    }

    if (req.url.startsWith('/api/')) {
        const apiPath = req.url.replace('/api/', '/');
        const options = {
            hostname: API_HOST, path: apiPath, method: 'GET',
            headers: { 'User-Key': API_KEY, 'Content-Type': 'application/json' }
        };
        const proxyReq = https.request(options, (proxyRes) => {
            res.writeHead(proxyRes.statusCode, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            proxyRes.pipe(res);
        });
        proxyReq.on('error', (err) => {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
        });
        proxyReq.end();
        return;
    }

    let filePath = req.url === '/' ? '/index.html' : req.url.split('?')[0];
    filePath = path.join(__dirname, filePath);
    const ext = path.extname(filePath);
    const mimeTypes = {
        '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css',
        '.json': 'application/json', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml'
    };
    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); return res.end('Not found'); }
        const contentType = mimeTypes[ext] || 'text/plain';
        // HTML pages: no cache (sempre buscar versão nova do server) para evitar problemas de deploy stale.
        // Assets (js/css/png): cache curto (60s).
        const isHTML = ext === '.html' || req.url === '/';
        const cacheControl = isHTML
            ? 'no-cache, no-store, must-revalidate'
            : 'public, max-age=60';
        if ((req.headers['accept-encoding'] || '').includes('gzip') && /html|javascript|css|json|svg/.test(contentType)) {
            zlib.gzip(data, (e, zipped) => {
                if (e) { res.writeHead(200, { 'Content-Type': contentType }); return res.end(data); }
                res.writeHead(200, { 'Content-Type': contentType, 'Content-Encoding': 'gzip', 'Cache-Control': cacheControl });
                res.end(zipped);
            });
        } else {
            res.writeHead(200, { 'Content-Type': contentType, 'Cache-Control': cacheControl });
            res.end(data);
        }
    });
});

// Inicializa users do Redis (ou seed se vazio) antes de subir o server
usersBootPromise = initUsersFromRedis();
usersBootPromise.finally(() => {
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`Portal Lincros rodando em http://0.0.0.0:${PORT}`);
    });
});
