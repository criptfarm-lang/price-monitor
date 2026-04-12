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

function buildProduct(p, stockMap, costMap, salesThis, salesLast, priceTypes) {
  const stock = stockMap[p.id] ?? 0;
  // Для готовой продукции себестоимость из отчёта прибыльности точнее
  const costFromSales = salesThis[p.id]?.avgCost || salesLast[p.id]?.avgCost || 0;
  const costPrice = costFromSales || costMap[p.id] || msVal(p.buyPrice?.value) || 0;

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

// ─── Helpers ──────────────────────────────────────────────────
function median(arr) {
  if (!arr.length) return 0;
  const s = [...arr].sort((a,b) => a-b);
  const m = Math.floor(s.length/2);
  return s.length % 2 ? s[m] : (s[m-1]+s[m])/2;
}

// Убираем выбросы отклоняющиеся более чем на 80% от медианы (порог 20%)
function trimmedAvg(values) {
  if (!values.length) return null;
  if (values.length === 1) return values[0];
  const med = median(values);
  if (med === 0) return null;
  const filtered = values.filter(v => Math.abs(v - med) / med <= 0.80);
  if (!filtered.length) return med;
  return filtered.reduce((a,b) => a+b, 0) / filtered.length;
}

// ─── Sales + cost via demands (с фильтрацией выбросов себест.) ─
async function getSalesData(dateFrom, dateTo) {
  const result = {};
  const costSamples = {}; // id -> [cost per unit, ...]

  try {
    // Сначала пробуем profit/byproduct — быстро, без выбросов (агрегат)
    let offset = 0;
    while (true) {
      const report = await msGet(
        `/report/profit/byproduct?momentFrom=${dateFrom} 00:00:00&momentTo=${dateTo} 23:59:59&limit=1000&offset=${offset}`
      );
      const rows = report.rows || [];
      rows.forEach(row => {
        const href = row.assortment?.meta?.href || '';
        const id = href.split('/').pop();
        if (!id) return;
        const sellQty = row.sellQuantity || 0;
        const sellSum = msVal(row.sellSum) || 0;
        const costSum = msVal(row.costSum) || 0;
        if (sellQty > 0) {
          result[id] = { avgPrice: sellSum / sellQty, qty: sellQty };
          // costSum/sellQty — это уже агрегат, сохраняем как sample
          const costPerUnit = costSum / sellQty;
          if (costPerUnit > 0) {
            if (!costSamples[id]) costSamples[id] = [];
            costSamples[id].push(costPerUnit);
          }
        }
      });
      if (rows.length < 1000) break;
      offset += 1000;
    }
    console.log('Profit report OK:', Object.keys(result).length, 'products');
  } catch(e) {
    console.warn('Profit report failed:', e.message);
  }

  // Для каждого товара — собираем себестоимость по отдельным отгрузкам
  // чтобы можно было отфильтровать выбросы
  try {
    const demands = await msGetAll(
      `/entity/demand?filter=moment>=${dateFrom} 00:00:00;moment<=${dateTo} 23:59:59&expand=positions`
    );
    for (const demand of demands) {
      const positions = demand.positions?.rows || demand.positions || [];
      for (const pos of positions) {
        const href = pos.assortment?.meta?.href || '';
        const id = href.split('/').pop();
        if (!id) continue;
        const cost = msVal(pos.cost) || 0;   // себестоимость единицы
        const price = msVal(pos.price) || 0;
        const qty = pos.quantity || 0;
        if (qty <= 0) continue;
        // Обновляем продажи если profit report не дал данных
        if (!result[id] && price > 0) {
          result[id] = { avgPrice: price, qty };
        }
        if (cost > 0) {
          if (!costSamples[id]) costSamples[id] = [];
          costSamples[id].push(cost);
        }
      }
    }
  } catch(e) {
    console.warn('Demands expand failed:', e.message);
  }

  // Применяем trimmedAvg к себестоимостям
  Object.keys(costSamples).forEach(id => {
    const avg = trimmedAvg(costSamples[id]);
    if (avg !== null && avg > 0) {
      if (!result[id]) result[id] = { avgPrice: 0, qty: 0 };
      result[id].avgCost = avg;
    }
  });

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

  // 2. Products (non-archived) — только нужные каталоги
  const ALLOWED_CATEGORIES = ['ГОТОВАЯ ПРОДУКЦИЯ', 'ПРИВЛЕЧЕННЫЕ ТОВАРЫ'];
  const allProducts = await msGetAll('/entity/product?archived=false');
  const products = allProducts.filter(p => {
    const path = (p.pathName || '').toUpperCase();
    return ALLOWED_CATEGORIES.some(cat => path.startsWith(cat));
  });
  console.log(`Filtered: ${products.length} of ${allProducts.length} products`);

  // 3. Stock — iterate pages, use assortment.meta.href for id
  let stockMap = {};
  let costMap = {};
  try {
    let offset = 0;
    while (true) {
      const stockResp = await msGet(`/report/stock/all?stockMode=all&limit=1000&offset=${offset}`);
      const rows = stockResp.rows || [];
      rows.forEach(r => {
        // В /report/stock/all id товара в meta.href напрямую
        const href = r.meta?.href || '';
        // убираем ?expand=supplier если есть
        const cleanHref = href.split('?')[0];
        const id = cleanHref ? cleanHref.split('/').pop() : r.id;
        if (id) {
          stockMap[id] = (stockMap[id] || 0) + (r.stock || 0);
          // price в отчёте = себестоимость в копейках
          if (r.price > 0) costMap[id] = r.price / 100;
          else if (r.avgCost > 0) costMap[id] = r.avgCost / 100;
        }
      });
      if (rows.length < 1000) break;
      offset += 1000;
    }
    console.log('Stock loaded:', Object.keys(stockMap).length);
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
    // Merge cost from sales into costMap
  Object.keys(salesThis).forEach(id => {
    if (salesThis[id].avgCost) costMap[id] = salesThis[id].avgCost;
  });
  const built = products.map(p => buildProduct(p, stockMap, costMap, salesThis, salesLast, priceTypes));

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

  // GET /api/debug/stock — показывает первые 3 строки отчёта остатков
  if (pathname==='/api/debug/stock' && req.method==='GET') {
    try {
      const r = await msGet('/report/stock/all?stockMode=all&limit=3');
      return sendJSON(res, { rows: r.rows, total: r.meta?.size });
    } catch(e) { return sendErr(res, e.message); }
  }

  // GET /api/debug/product?code=XXXX — найти товар по коду
  if (pathname==='/api/debug/product' && req.method==='GET') {
    try {
      const code = query.code || '';
      const r = await msGet(`/entity/product?search=${encodeURIComponent(code)}&limit=1`);
      const p = r.rows?.[0];
      if (!p) return sendErr(res, 'не найден');
      // get stock for this product
      const sid = p.id;
      const sr = await msGet(`/report/stock/all?stockMode=all&limit=5`);
      return sendJSON(res, { product: { id: p.id, name: p.name, code: p.code }, sampleStockRows: sr.rows, total: sr.meta?.size });
    } catch(e) { return sendErr(res, e.message); }
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
