const http = require('http');
const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 80;
const API_KEY = '1765E369255C44601A45DEE600DA89AB520BF12B23904DF127344DD91E3A31EAE2EFDF4862A9F31757FE84FE842076258347E9DE1AF9E28C3BC719ED7782F286';
const API_HOST = 'public-api2.ploomes.com';

// ==================== In-memory cache ====================
const cache = {
    wonDeals: null,
    lostDeals: null,
    openDeals: null,
    contactStates: null,
    forecastDeals: null,
    lastRefresh: null,
    refreshing: false,
    initError: null
};

// Cache compiled JSON responses to avoid re-stringifying + re-gzipping on every request
const responseCache = {};

function apiCall(apiPath) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: API_HOST,
            path: apiPath,
            method: 'GET',
            headers: {
                'User-Key': API_KEY,
                'Accept-Encoding': 'gzip',
                'Content-Type': 'application/json'
            }
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
        req.setTimeout(60000, () => { req.destroy(new Error('Timeout')); });
        req.end();
    });
}

async function fetchAllPages(baseUrl, pageSize = 200) {
    // Get first page with count to know total
    const sep = baseUrl.includes('?') ? '&' : '?';
    const first = await apiCall(`${baseUrl}${sep}$count=true&$top=${pageSize}&$skip=0`);
    let all = first.value || [];
    const total = first['@odata.count'] || all.length;
    if (all.length >= total) return all;

    // Fetch all remaining pages in parallel (batches of 5 to avoid overloading API)
    const skips = [];
    for (let skip = pageSize; skip < total; skip += pageSize) skips.push(skip);

    const BATCH = 5;
    for (let i = 0; i < skips.length; i += BATCH) {
        const batch = skips.slice(i, i + BATCH);
        const results = await Promise.all(batch.map(skip => apiCall(`${baseUrl}${sep}$top=${pageSize}&$skip=${skip}`)));
        results.forEach(r => { all = all.concat(r.value || []); });
    }
    return all;
}

function buildResponse(data) {
    const body = JSON.stringify({ value: data, lastRefresh: cache.lastRefresh });
    const gzipped = zlib.gzipSync(body);
    return { body, gzipped };
}

async function refreshCache() {
    if (cache.refreshing) { console.log('Skipping refresh, already in progress'); return; }
    cache.refreshing = true;
    const start = Date.now();
    console.log('[cache] Starting refresh at', new Date().toISOString());
    try {
        const dealExpand = '$expand=Contact($expand=Phones,City($expand=State)),Owner,Stage,Pipeline,OtherProperties';
        const forecastFilter = "OtherProperties/any(o: o/FieldKey eq 'deal_7F644269-46FE-4486-AD12-BEFA9C7E27BC')";
        const stateFilter = "OtherProperties/any(o: o/FieldKey eq 'contact_486DE9AD-FCFE-4A7B-8B56-DA5AB3D55848')";

        const [won, lost, open, contacts, forecast] = await Promise.all([
            fetchAllPages(`/Deals?$filter=StatusId eq 2&$orderby=FinishDate desc&${dealExpand}`),
            fetchAllPages(`/Deals?$filter=StatusId eq 3&$orderby=FinishDate desc&${dealExpand}`),
            fetchAllPages(`/Deals?$filter=StatusId eq 1&$orderby=CreateDate desc&${dealExpand}`),
            fetchAllPages(`/Contacts?$filter=${encodeURIComponent(stateFilter)}&$expand=OtherProperties&$select=Id`),
            fetchAllPages(`/Deals?$filter=StatusId eq 1 and ${encodeURIComponent(forecastFilter)}&$orderby=CreateDate desc&$expand=Owner,Pipeline,Stage,OtherProperties`)
        ]);

        cache.wonDeals = won;
        cache.lostDeals = lost;
        cache.openDeals = open;
        cache.contactStates = contacts;
        cache.forecastDeals = forecast;
        cache.lastRefresh = new Date().toISOString();
        cache.initError = null;

        // Pre-build gzipped responses
        responseCache.won = buildResponse(won);
        responseCache.lost = buildResponse(lost);
        responseCache.open = buildResponse(open);
        responseCache.contacts = buildResponse(contacts);
        responseCache.forecast = buildResponse(forecast);

        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`[cache] Refresh OK in ${elapsed}s: won=${won.length} lost=${lost.length} open=${open.length} contacts=${contacts.length} forecast=${forecast.length}`);
    } catch (e) {
        console.error('[cache] Refresh error:', e.message);
        cache.initError = e.message;
    }
    cache.refreshing = false;
}

// Start cache refresh on boot and every 5 minutes
refreshCache();
setInterval(refreshCache, 5 * 60 * 1000);

// ==================== HTTP server ====================
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

    // ---- Cached data endpoints ----
    if (req.url.startsWith('/cache/')) {
        const key = req.url.split('/')[2].split('?')[0];
        const cached = responseCache[key];
        if (!cached) {
            res.writeHead(202, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({
                status: 'loading',
                message: 'Cache ainda carregando, tente novamente em alguns segundos',
                refreshing: cache.refreshing,
                error: cache.initError
            }));
        }
        const acceptEncoding = req.headers['accept-encoding'] || '';
        if (acceptEncoding.includes('gzip')) {
            res.writeHead(200, {
                'Content-Type': 'application/json',
                'Content-Encoding': 'gzip',
                'Cache-Control': 'public, max-age=60'
            });
            return res.end(cached.gzipped);
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' });
        return res.end(cached.body);
    }

    // ---- Status endpoint ----
    if (req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
            lastRefresh: cache.lastRefresh,
            refreshing: cache.refreshing,
            counts: {
                won: cache.wonDeals?.length || 0,
                lost: cache.lostDeals?.length || 0,
                open: cache.openDeals?.length || 0,
                contacts: cache.contactStates?.length || 0,
                forecast: cache.forecastDeals?.length || 0
            }
        }));
    }

    // ---- Force refresh ----
    if (req.url === '/refresh') {
        refreshCache();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: 'refreshing' }));
    }

    // ---- Proxy API calls (fallback for uncached queries) ----
    if (req.url.startsWith('/api/')) {
        const apiPath = req.url.replace('/api/', '/');
        const options = {
            hostname: API_HOST,
            path: apiPath,
            method: 'GET',
            headers: { 'User-Key': API_KEY, 'Content-Type': 'application/json' }
        };
        const proxyReq = https.request(options, (proxyRes) => {
            res.writeHead(proxyRes.statusCode, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
            proxyRes.pipe(res);
        });
        proxyReq.on('error', (err) => {
            console.error('Proxy error:', err.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
        });
        proxyReq.end();
        return;
    }

    // ---- Static files ----
    let filePath = req.url === '/' ? '/index.html' : req.url.split('?')[0];
    filePath = path.join(__dirname, filePath);
    const ext = path.extname(filePath);
    const mimeTypes = {
        '.html': 'text/html',
        '.js': 'application/javascript',
        '.css': 'text/css',
        '.json': 'application/json',
        '.png': 'image/png',
        '.ico': 'image/x-icon',
        '.svg': 'image/svg+xml'
    };
    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); return res.end('Not found'); }
        const contentType = mimeTypes[ext] || 'text/plain';
        // Gzip HTML/JS/CSS/JSON for faster transfer
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
