const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const DATA_DIR = (() => {
  const d = process.env.DATA_DIR || '/app/data';
  try { if (!fs.existsSync(d)) fs.mkdirSync(d, {recursive:true}); return d; } catch { return __dirname; }
})();
const DATA_FILE = path.join(DATA_DIR, 'monitor-data.json');
let MS_TOKEN = process.env.MOYSKLAD_TOKEN || '';

// ─── helpers ──────────────────────────────────────────────────
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
}
function sendJSON(res, data, status=200) {
  res.writeHead(status, {'Content-Type':'application/json; charset=utf-8'});
  res.end(JSON.stringify(data));
}
function sendErr(res, msg, status=400) { sendJSON(res, {error:msg}, status); }
function readBody(req) {
  return new Promise((ok,fail) => {
    let b='';
    req.on('data', c => { b+=c; if(b.length>50e6) req.destroy(); });
    req.on('end', () => { try{ok(JSON.parse(b||'{}'))}catch{ok({})} });
    req.on('error', fail);
  });
}
function loadData() {
  try { if(fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE,'utf8')); } catch{}
  return { rowOrder: [], competitorPrices: {}, collapsedGroups: [] };
}
function saveData(d) { fs.writeFileSync(DATA_FILE, JSON.stringify(d,null,2),'utf8'); }

// ─── МойСклад API ─────────────────────────────────────────────
function msRequest(endpoint, method='GET', body=null) {
  return new Promise((ok, fail) => {
    if (!MS_TOKEN) { fail(new Error('MOYSKLAD_TOKEN не задан')); return; }
    const bodyStr = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: 'api.moysklad.ru',
      path: '/api/remap/1.2' + endpoint,
      method,
      headers: {
        'Authorization': 'Bearer ' + MS_TOKEN,
        'Accept-Encoding': 'gzip',
        'Content-Type': 'application/json',
        ...(bodyStr ? {'Content-Length': Buffer.byteLength(bodyStr)} : {})
      }
    };
    const req = https.request(opts, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const buf = Buffer.concat(chunks);
          const raw = res.headers['content-encoding'] === 'gzip'
            ? zlib.gunzipSync(buf).toString('utf8')
            : buf.toString('utf8');
          if (!raw) { ok({}); return; }
          const data = JSON.parse(raw);
          if (res.statusCode >= 400) fail(new Error(data.errors?.[0]?.error || 'HTTP ' + res.statusCode));
          else ok(data);
        } catch(e) { fail(e); }
      });
    });
    req.on('error', fail);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

function msGet(endpoint) { return msRequest(endpoint, 'GET'); }

// Paginate through all results
async function msGetAll(endpoint) {
  let offset = 0;
  const limit = 100;
  let all = [];
  while (true) {
    const sep = endpoint.includes('?') ? '&' : '?';
    const data = await msGet(`${endpoint}${sep}limit=${limit}&offset=${offset}`);
    const rows = data.rows || [];
    all = all.concat(rows);
    if (all.length >= (data.meta?.size || 0) || rows.length === 0) break;
    offset += limit;
  }
  return all;
}

// ─── Data builders ────────────────────────────────────────────
function msVal(v) { return (v || v===0) ? v/100 : null; }

function buildProduct(p, stockMap, salesThis, salesLast, priceTypes) {
  const stock = stockMap[p.id] ?? 0;
  const costPrice = msVal(p.buyPrice?.value) || 0;

  // All sale prices in order of priceTypes
  const prices = priceTypes.map(pt => {
    const sp = (p.salePrices||[]).find(x => x.priceType?.id === pt.id);
    return sp ? msVal(sp.value) : 0;
  });
  // Pad to 3
  while (prices.length < 3) prices.push(0);

  const markup0 = (prices[0] > 0 && costPrice > 0)
    ? Math.round((prices[0] - costPrice) / costPrice * 100) : null;

  const saleThis = salesThis[p.id];
  const saleLast = salesLast[p.id];

  const realPrice = saleThis ? saleThis.avgPrice : null;
  const realMarkup = (realPrice && costPrice > 0)
    ? Math.round((realPrice - costPrice) / costPrice * 100) : null;

  const prevRealMarkup = (saleLast && costPrice > 0)
    ? Math.round((saleLast.avgPrice - costPrice) / costPrice * 100) : null;

  const markupDelta = (realMarkup !== null && prevRealMarkup !== null)
    ? realMarkup - prevRealMarkup : null;

  // Причина изменения наценки
  let deltaReason = null;
  if (markupDelta !== null && Math.abs(markupDelta) >= 1) {
    const priceDiff = realPrice - (saleLast?.avgPrice || realPrice);
    if (Math.abs(priceDiff) > 0.01) {
      deltaReason = priceDiff < 0 ? 'цена ↓' : 'цена ↑';
    } else {
      deltaReason = 'себест ↑';
    }
  }

  return {
    id: p.id,
    name: p.name,
    code: p.code || p.article || '',
    category: p.pathName || 'Без категории',
    stock,
    costPrice,
    prices,
    markup0,
    realPrice,
    realMarkup,
    markupDelta,
    deltaReason,
    archived: p.archived || false,
    salePriceTypeIds: (p.salePrices||[]).map(sp => sp.priceType?.id),
  };
}

// ─── Sales aggregation ────────────────────────────────────────
async function getSalesData(dateFrom, dateTo) {
  const result = {};
  try {
    // Use sales by product report
    const report = await msGet(
      `/report/sales/byproduct?momentFrom=${dateFrom} 00:00:00&momentTo=${dateTo} 23:59:59&limit=1000`
    );
    (report.rows || []).forEach(row => {
      const href = row.assortment?.meta?.href || '';
      const id = href.split('/').pop();
      if (!id) return;
      const revenue = msVal(row.sellSum) || 0;
      const qty = row.sellQuantity || 0;
      result[id] = {
        avgPrice: qty > 0 ? revenue / qty : 0,
        qty
      };
    });
  } catch(e) {
    console.warn('Sales report failed, trying demands:', e.message);
    // Fallback: parse demands
    try {
      const demands = await msGetAll(
        `/entity/demand?filter=moment>=${dateFrom} 00:00:00;moment<=${dateTo} 23:59:59&expand=positions.assortment`
      );
      for (const demand of demands) {
        const positions = demand.positions?.rows || [];
        for (const pos of positions) {
          const href = pos.assortment?.meta?.href || pos.assortment?.id || '';
          const id = typeof href === 'string' ? href.split('/').pop() : href;
          if (!id) continue;
          const price = msVal(pos.price) || 0;
          const qty = pos.quantity || 0;
          if (!result[id]) result[id] = { totalRev: 0, totalQty: 0, avgPrice: 0 };
          result[id].totalRev += price * qty;
          result[id].totalQty += qty;
        }
      }
      Object.values(result).forEach(r => {
        if (r.totalQty > 0) r.avgPrice = r.totalRev / r.totalQty;
      });
    } catch(e2) {
      console.warn('Demands fallback failed:', e2.message);
    }
  }
  return result;
}

// ─── Main data loader ──────────────────────────────────────────
async function loadMSData() {
  // 1. Price types — берём из первого товара
  const ptSample = await msGet('/entity/product?limit=1');
  const sampleProduct = ptSample.rows?.[0];
  const priceTypes = (sampleProduct?.salePrices || []).map(sp => ({
    id: sp.priceType?.id || sp.priceType?.meta?.href?.split('/').pop() || '',
    name: sp.priceType?.name || 'Цена'
  }));

  // 2. Products (non-archived)
  const products = await msGetAll('/entity/product?archived=false');

  // 3. Stock
  let stockMap = {};
  try {
    const stockResp = await msGet('/report/stock/all?stockMode=all&limit=1000');
    (stockResp.rows || []).forEach(r => {
      const id = r.meta?.href?.split('/').pop() || r.id;
      if (id) stockMap[id] = r.stock || 0;
    });
  } catch(e) {
    console.warn('Stock report failed:', e.message);
  }

  // 4. Sales data
  const now = new Date();
  const pad = n => String(n).padStart(2,'0');
  const thisMonthStart = `${now.getFullYear()}-${pad(now.getMonth()+1)}-01`;
  const today = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(now.getDate())}`;
  const lastMonth = new Date(now.getFullYear(), now.getMonth()-1, 1);
  const lastMonthStart = `${lastMonth.getFullYear()}-${pad(lastMonth.getMonth()+1)}-01`;
  const lastMonthEnd = new Date(now.getFullYear(), now.getMonth(), 0);
  const lastMonthEndStr = `${lastMonthEnd.getFullYear()}-${pad(lastMonthEnd.getMonth()+1)}-${pad(lastMonthEnd.getDate())}`;

  const [salesThis, salesLast] = await Promise.all([
    getSalesData(thisMonthStart, today).catch(() => ({})),
    getSalesData(lastMonthStart, lastMonthEndStr).catch(() => ({}))
  ]);

  // 5. Build
  const built = products.map(p => buildProduct(p, stockMap, salesThis, salesLast, priceTypes));

  return {
    products: built,
    priceTypes: priceTypes.map(pt => ({ id: pt.id, name: pt.name })),
    loadedAt: new Date().toISOString()
  };
}

// ─── router ───────────────────────────────────────────────────
async function router(req, res) {
  cors(res);
  if (req.method==='OPTIONS') { res.writeHead(204); res.end(); return; }
  const {pathname, query} = url.parse(req.url, true);

  // GET /api/data — all products, prices, stock, sales
  if (pathname==='/api/data' && req.method==='GET') {
    if (!MS_TOKEN) return sendErr(res, 'MOYSKLAD_TOKEN не задан — добавьте переменную в Railway', 401);
    try {
      const data = await loadMSData();
      return sendJSON(res, data);
    } catch(e) {
      console.error('loadMSData error:', e);
      return sendErr(res, e.message, 500);
    }
  }

  // PUT /api/price — update product price in МС
  if (pathname==='/api/price' && req.method==='PUT') {
    if (!MS_TOKEN) return sendErr(res, 'MOYSKLAD_TOKEN не задан', 401);
    const body = await readBody(req);
    const { productId, priceTypeId, value } = body;
    if (!productId || value === undefined) return sendErr(res, 'Нужны productId и value');
    try {
      // Get current product
      const prod = await msGet(`/entity/product/${productId}`);
      const salePrices = prod.salePrices ? JSON.parse(JSON.stringify(prod.salePrices)) : [];
      const idx = salePrices.findIndex(sp => sp.priceType?.id === priceTypeId);
      if (idx >= 0) {
        salePrices[idx].value = Math.round(value * 100);
      } else {
        // append
        salePrices.push({ value: Math.round(value * 100), priceType: { id: priceTypeId } });
      }
      await msRequest(`/entity/product/${productId}`, 'PUT', { salePrices });
      return sendJSON(res, { ok: true });
    } catch(e) {
      return sendErr(res, e.message, 500);
    }
  }

  // PUT /api/archive — archive product
  if (pathname==='/api/archive' && req.method==='PUT') {
    if (!MS_TOKEN) return sendErr(res, 'MOYSKLAD_TOKEN не задан', 401);
    const body = await readBody(req);
    if (!body.productId) return sendErr(res, 'Нужен productId');
    try {
      await msRequest(`/entity/product/${body.productId}`, 'PUT', { archived: true });
      return sendJSON(res, { ok: true });
    } catch(e) {
      return sendErr(res, e.message, 500);
    }
  }

  // GET/POST /api/settings — persist row order, competitor prices etc
  if (pathname==='/api/settings') {
    if (req.method==='GET') return sendJSON(res, loadData());
    if (req.method==='POST') {
      const body = await readBody(req);
      saveData(body);
      return sendJSON(res, { ok: true });
    }
  }

  // Serve index.html
  const MIME = {'.html':'text/html; charset=utf-8','.js':'application/javascript','.css':'text/css','.json':'application/json'};
  if (pathname==='/' || pathname==='/index.html') {
    try {
      res.writeHead(200, {'Content-Type':'text/html; charset=utf-8'});
      res.end(fs.readFileSync(path.join(__dirname,'index.html')));
    } catch { res.writeHead(404); res.end('index.html not found'); }
    return;
  }

  res.writeHead(404); res.end('Not found');
}

// ─── start ────────────────────────────────────────────────────
const saved = loadData();
if (!MS_TOKEN && saved._msToken) MS_TOKEN = saved._msToken;

http.createServer(async (req,res) => {
  try { await router(req,res); }
  catch(e) { console.error(e); if(!res.headersSent){res.writeHead(500);res.end('Error');} }
}).listen(PORT, () => {
  console.log('📊 Price Monitor http://localhost:' + PORT);
  console.log(MS_TOKEN ? '✅ Токен МойСклад загружен' : '⚠️  MOYSKLAD_TOKEN не задан');
});
