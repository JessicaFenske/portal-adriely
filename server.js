const http = require('http');
const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 80;
const API_KEY = '1765E369255C44601A45DEE600DA89AB520BF12B23904DF127344DD91E3A31EAE2EFDF4862A9F31757FE84FE842076258347E9DE1AF9E28C3BC719ED7782F286';
const API_HOST = 'public-api2.ploomes.com';
const RD_PUBLIC_TOKEN = '00bbd955e27e47c643cab874adf517a5'; // RD Marketing token publico (envio de conversoes)
const RD_PRIVATE_TOKEN = 'd0dd9d50d65ab0efefa3687ec6af3bc2'; // RD Marketing token privado (API legada)
const RD_CLIENT_ID = '893969';
const CACHE_DIR = '/tmp/portal-cache';
const CACHE_VERSION = 6; // Bump to invalidate disk cache after schema changes

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
    const dealExpand = '$expand=Contact($expand=Phones,City($expand=State)),Owner,Stage,Pipeline,OtherProperties,Origin';
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
    // Atividades "Reuniao Agendada" - filtra Title local depois (RFE/encoding instavel no Ploomes)
    // Pega ultimos 120 dias de tasks com Title comecando com Reun, Select minimo
    const oneTwentyDaysAgo = new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString();
    refreshOne('meetings', odataEncode(`/Tasks?$filter=DateTime ge ${oneTwentyDaysAgo} and (startswith(Title,'Reuni') or startswith(Title,'reuni'))&$select=Id,Title,DateTime,OwnerId,DealId,ContactId,TypeId&$orderby=DateTime desc`));
    // Lista de usuarios para resolver OwnerId -> Name nas atividades
    refreshOne('users', odataEncode('/Users?$select=Id,Name'));
}

refreshAll();
setInterval(refreshAll, 5 * 60 * 1000);

// ==================== HTTP server ====================
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

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
        if ((req.headers['accept-encoding'] || '').includes('gzip') && /html|javascript|css|json|svg/.test(contentType)) {
            zlib.gzip(data, (e, zipped) => {
                if (e) { res.writeHead(200, { 'Content-Type': contentType }); return res.end(data); }
                res.writeHead(200, { 'Content-Type': contentType, 'Content-Encoding': 'gzip', 'Cache-Control': 'public, max-age=300' });
                res.end(zipped);
            });
        } else {
            res.writeHead(200, { 'Content-Type': contentType, 'Cache-Control': 'public, max-age=300' });
            res.end(data);
        }
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Portal Lincros rodando em http://0.0.0.0:${PORT}`);
});
