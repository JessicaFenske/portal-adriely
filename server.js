const http = require('http');
const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 80;
const API_KEY = '1765E369255C44601A45DEE600DA89AB520BF12B23904DF127344DD91E3A31EAE2EFDF4862A9F31757FE84FE842076258347E9DE1AF9E28C3BC719ED7782F286';
const API_HOST = 'public-api2.ploomes.com';
const CACHE_DIR = '/tmp/portal-cache';
const CACHE_VERSION = 2; // Bump to invalidate disk cache after schema changes

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
    const first = await apiCall(`${cleanUrl}${sep}$count=true&$top=${pageSize}&$skip=0`);
    let all = first.value || [];
    const total = first['@odata.count'] || all.length;
    if (all.length >= total) return all;

    const skips = [];
    for (let skip = pageSize; skip < total; skip += pageSize) skips.push(skip);

    const BATCH = 5;
    for (let i = 0; i < skips.length; i += BATCH) {
        const batch = skips.slice(i, i + BATCH);
        const results = await Promise.all(batch.map(skip => apiCall(`${cleanUrl}${sep}$top=${pageSize}&$skip=${skip}`)));
        results.forEach(r => { all = all.concat(r.value || []); });
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
const keys = ['won', 'lost', 'open', 'contacts', 'forecast'];
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
    const dealExpand = '$expand=Contact($expand=Phones,City($expand=State)),Owner,Stage,Pipeline,OtherProperties';
    const forecastFilter = "OtherProperties/any(o: o/FieldKey eq 'deal_7F644269-46FE-4486-AD12-BEFA9C7E27BC')";
    const stateFilter = "OtherProperties/any(o: o/FieldKey eq 'contact_486DE9AD-FCFE-4A7B-8B56-DA5AB3D55848')";

    // Each runs independently - available as soon as it finishes
    // Start forecast first (smallest, used by mobile often)
    refreshOne('forecast', odataEncode(`/Deals?$filter=StatusId eq 1 and ${forecastFilter}&$orderby=CreateDate desc&$expand=Owner,Pipeline,Stage,OtherProperties`));
    refreshOne('contacts', odataEncode(`/Contacts?$filter=${stateFilter}&$expand=OtherProperties&$select=Id`));
    refreshOne('won', odataEncode(`/Deals?$filter=StatusId eq 2&$orderby=FinishDate desc&${dealExpand}`));
    // Lost deals need LossReason expanded for reason analysis
    refreshOne('lost', odataEncode(`/Deals?$filter=StatusId eq 3&$orderby=FinishDate desc&${dealExpand},LossReason`));
    refreshOne('open', odataEncode(`/Deals?$filter=StatusId eq 1&$orderby=CreateDate desc&${dealExpand}`));
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
                forecast: cache.forecast?.length || 0
            }
        }));
    }

    if (req.url === '/refresh') {
        refreshAll();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: 'refreshing' }));
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
