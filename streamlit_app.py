"""
StockSense - smart inventory for quick-commerce stores (Flask + SQLite).

Setup:   pip install flask
Run:     python stocksense.py seed     # optional demo data
         python stocksense.py          # http://localhost:5000
Alerts:  set ALERT_WEBHOOK_URL (Slack/Teams/any webhook accepting {"text": "..."}) to push alerts.
"""
import json, math, os, random, sqlite3, sys, time, urllib.request
from flask import Flask, g, jsonify, request, render_template_string

DB = os.environ.get("DB_PATH", "inventory.db")
DAY = 86400
app = Flask(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY, name TEXT UNIQUE, reorder INTEGER DEFAULT 10);
CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY, pid INT, batch_no TEXT, qty INT, rem INT, expiry REAL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, ts REAL, pid INT, type TEXT, qty INT, note TEXT);
CREATE TABLE IF NOT EXISTS audits(id INTEGER PRIMARY KEY, ts REAL, pid INT, expected INT, actual INT, drift INT,
                                  anomaly INT, z REAL, reasons TEXT, resolved INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS alert_log(id INTEGER PRIMARY KEY, ts REAL, k TEXT UNIQUE, sev TEXT, msg TEXT);
"""

# ---------------------------------------------------------------- db helpers
def db():
    if "db" not in g:
        g.db = sqlite3.connect(DB)
        g.db.row_factory = sqlite3.Row
        g.db.executescript(SCHEMA)
    return g.db

@app.teardown_appcontext
def _close(_):
    d = g.pop("db", None)
    if d:
        d.close()

def q(sql, args=()):
    return db().execute(sql, args).fetchall()

def one(sql, args=()):
    r = db().execute(sql, args).fetchone()
    return r[0] if r and r[0] is not None else 0

def run(sql, args=()):
    c = db().execute(sql, args)
    db().commit()
    return c.lastrowid

def log_event(pid, typ, qty, note="", ts=None):
    run("INSERT INTO events(ts,pid,type,qty,note) VALUES(?,?,?,?,?)", (ts or time.time(), pid, typ, qty, note))

# ---------------------------------------------- steps 1-3: inventory tracking
def receive(name, batch_no, qty, expiry, ts=None):
    row = q("SELECT id FROM products WHERE name=? COLLATE NOCASE", (name,))
    pid = row[0]["id"] if row else run("INSERT INTO products(name) VALUES(?)", (name,))
    run("INSERT INTO batches(pid,batch_no,qty,rem,expiry) VALUES(?,?,?,?,?)", (pid, batch_no, qty, qty, expiry))
    log_event(pid, "receive", qty, batch_no, ts)
    return pid

def stock(pid):                       # expected stock according to the system
    return one("SELECT SUM(rem) FROM batches WHERE pid=?", (pid,))

def sellable(pid):                    # excludes expired batches
    return one("SELECT SUM(rem) FROM batches WHERE pid=? AND expiry>?", (pid, time.time()))

def fifo(pid, n, include_expired=False):
    """Remove n units, earliest expiry first. Returns units actually removed."""
    sql = "SELECT id,rem FROM batches WHERE pid=? AND rem>0" + ("" if include_expired else " AND expiry>?") + " ORDER BY expiry"
    left = n
    for b in q(sql, (pid,) if include_expired else (pid, time.time())):
        take = min(b["rem"], left)
        run("UPDATE batches SET rem=rem-? WHERE id=?", (take, b["id"]))
        left -= take
        if left == 0:
            break
    return n - left

def place_order(pid, n):
    if n > sellable(pid):
        log_event(pid, "cancel", n, "insufficient stock")
        return False
    fifo(pid, n)
    log_event(pid, "order", n)
    return True

def add_back(pid, n):
    b = q("SELECT id FROM batches WHERE pid=? ORDER BY expiry DESC LIMIT 1", (pid,))
    if b:
        run("UPDATE batches SET rem=rem+? WHERE id=?", (n, b[0]["id"]))

def write_off_expired():
    total = 0
    for b in q("SELECT * FROM batches WHERE rem>0 AND expiry<?", (time.time(),)):
        log_event(b["pid"], "expire", b["rem"], b["batch_no"])
        run("UPDATE batches SET rem=0 WHERE id=?", (b["id"],))
        total += b["rem"]
    return total

# ------------------------------------ steps 4-5: drift + anomaly detection
def analyse(pid, expected, actual):
    drift = actual - expected
    hist = [abs(r["drift"]) for r in q("SELECT drift FROM audits WHERE pid=?", (pid,))]
    mean = sum(hist) / len(hist) if hist else 0
    sd = math.sqrt(sum((h - mean) ** 2 for h in hist) / len(hist)) if len(hist) > 2 else 0
    z = (abs(drift) - mean) / sd if sd else 0
    anomaly = abs(drift) > max(3, 0.08 * expected) or z > 2.5
    reasons = []
    if anomaly:
        avg_order = one("SELECT AVG(qty) FROM events WHERE pid=? AND type='order'", (pid,)) or 1
        n = abs(drift)
        if drift < 0:
            if one("SELECT COUNT(*) FROM batches WHERE pid=? AND rem>0 AND expiry<?", (pid, time.time() + DAY)):
                reasons.append("Expired or near-expiry units removed from shelf but not written off")
            if n >= 0.8 * avg_order:
                reasons.append(f"Unrecorded sale (about {n / avg_order:.1f} typical orders), e.g. offline sale")
            reasons.append("Damage or spoilage not logged" if n < max(3, 0.08 * expected) * 2
                           else "Possible theft or mis-picking; audit this shelf")
        else:
            reasons += ["Return or stock receipt not recorded", "Data-entry error on an inbound quantity"]
        if actual != expected and str(actual)[::-1] == str(expected):
            reasons.insert(0, "Digits look transposed (typing error)")
    return dict(drift=drift, anomaly=int(anomaly), z=round(z, 1), reasons=reasons)

def record_count(pid, actual):
    expected = stock(pid)
    a = analyse(pid, expected, actual)
    aid = run("INSERT INTO audits(ts,pid,expected,actual,drift,anomaly,z,reasons) VALUES(?,?,?,?,?,?,?,?)",
              (time.time(), pid, expected, actual, a["drift"], a["anomaly"], a["z"], json.dumps(a["reasons"])))
    return {"id": aid, "expected": expected, "actual": actual, **a}

def reconcile(aid):
    a = q("SELECT * FROM audits WHERE id=?", (aid,))[0]
    diff = stock(a["pid"]) - a["actual"]
    if diff > 0:
        fifo(a["pid"], diff, include_expired=True)
    elif diff < 0:
        add_back(a["pid"], -diff)
    log_event(a["pid"], "adjust", -diff, "audit reconcile")
    run("UPDATE audits SET resolved=1 WHERE id=?", (aid,))

# ---------------------------------------------------- step 7: stock prediction
def daily_sales(pid, days=14):
    out, now = [0] * days, time.time()
    for r in q("SELECT ts,qty FROM events WHERE pid=? AND type='order' AND ts>?", (pid, now - days * DAY)):
        out[days - 1 - int((now - r["ts"]) // DAY)] += r["qty"]
    return out

def demand(pid):                       # weighted moving average, units/day
    d = daily_sales(pid, 7)
    return 0.6 * sum(d[4:]) / 3 + 0.4 * sum(d) / 7

def days_left(pid):
    dm = demand(pid)
    return sellable(pid) / dm if dm > 0 else None

# ------------------------------------------- steps 6 + 8: expiry and alerts
def compute_alerts():
    A, now = [], time.time()
    for p in q("SELECT * FROM products"):
        pid, name, s, dl = p["id"], p["name"], sellable(p["id"]), days_left(p["id"])
        if s <= p["reorder"]:
            A.append((f"low{pid}-{s}", "warn", f"{name}: low stock ({s} left, reorder level {p['reorder']})"))
        if dl is not None and dl < 2:
            A.append((f"so{pid}-{int(dl * 4)}", "crit", f"{name}: predicted to run out in about {dl:.1f} days"))
        for b in q("SELECT * FROM batches WHERE pid=? AND rem>0", (pid,)):
            left = (b["expiry"] - now) / DAY
            if left < 0:
                A.append((f"ex{b['id']}", "crit", f"{name}: {b['rem']} units expired (batch {b['batch_no']})"))
            elif left < 3:
                A.append((f"ne{b['id']}", "warn", f"{name}: {b['rem']} units expire in {left:.0f} days (batch {b['batch_no']})"))
        a = q("SELECT * FROM audits WHERE pid=? ORDER BY id DESC LIMIT 1", (pid,))
        if a and a[0]["anomaly"] and not a[0]["resolved"]:
            A.append((f"dr{a[0]['id']}", "crit", f"{name}: inventory drift of {a[0]['drift']} units"))
    c = one("SELECT COUNT(*) FROM events WHERE type='cancel'")
    if c:
        A.append((f"cx{c}", "warn", f"{c} order(s) cancelled for lack of stock"))
    return A

def notify(msg):
    print("[ALERT]", msg)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        try:
            req = urllib.request.Request(url, json.dumps({"text": "StockSense: " + msg}).encode(),
                                         {"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            print("webhook failed:", e)

def dispatch(alerts):
    for k, sev, msg in alerts:            # each alert is sent once
        if not q("SELECT 1 FROM alert_log WHERE k=?", (k,)):
            run("INSERT INTO alert_log(ts,k,sev,msg) VALUES(?,?,?,?)", (time.time(), k, sev, msg))
            notify(msg)

# ------------------------------------------------------------------ REST API
def body():
    return request.get_json(force=True, silent=True) or {}

@app.post("/api/receive")
def api_receive():
    b = body()
    exp = time.mktime(time.strptime(b["expiry"], "%Y-%m-%d")) + DAY - 1
    return jsonify(product_id=receive(b["name"], b.get("batch_no", "B" + str(int(time.time()))), int(b["qty"]), exp))

@app.post("/api/order")               # call this from your delivery-app order webhook
def api_order():
    b = body()
    ok = place_order(int(b["product_id"]), int(b["qty"]))
    return jsonify(ok=ok, sellable=sellable(int(b["product_id"]))), (200 if ok else 409)

@app.post("/api/movement")            # type: return | damage
def api_move():
    b, pid, n = body(), int(body()["product_id"]), int(body()["qty"])
    if b["type"] == "return":
        add_back(pid, n); log_event(pid, "return", n)
    elif b["type"] == "damage":
        log_event(pid, "damage", fifo(pid, n, True))
    else:
        return jsonify(error="type must be return or damage"), 400
    return jsonify(ok=True)

@app.post("/api/count")
def api_count():
    b = body()
    return jsonify(record_count(int(b["product_id"]), int(b["actual"])))

@app.post("/api/reconcile/<int:aid>")
def api_reconcile(aid):
    reconcile(aid); return jsonify(ok=True)

@app.post("/api/writeoff")
def api_writeoff():
    return jsonify(written_off=write_off_expired())

@app.get("/api/dashboard")
def api_dashboard():
    A = compute_alerts()
    dispatch(A)
    products = []
    for p in q("SELECT * FROM products"):
        a = q("SELECT * FROM audits WHERE pid=? ORDER BY id DESC LIMIT 1", (p["id"],))
        dl = days_left(p["id"])
        products.append(dict(id=p["id"], name=p["name"], stock=stock(p["id"]), sellable=sellable(p["id"]),
                             counted=a[0]["actual"] if a else None, drift=a[0]["drift"] if a else None,
                             days_left=None if dl is None else round(dl, 1), demand=round(demand(p["id"]), 1),
                             sales14=daily_sales(p["id"]), reorder=p["reorder"]))
    insights = [dict(id=r["id"], product=r["name"], drift=r["drift"], z=r["z"], resolved=r["resolved"],
                     reasons=json.loads(r["reasons"]))
                for r in q("SELECT a.*,p.name FROM audits a JOIN products p ON p.id=a.pid WHERE anomaly=1 ORDER BY a.id DESC LIMIT 5")]
    batches = [dict(product=r["name"], batch=r["batch_no"], rem=r["rem"], days=round((r["expiry"] - time.time()) / DAY, 1))
               for r in q("SELECT b.*,p.name FROM batches b JOIN products p ON p.id=b.pid WHERE rem>0 ORDER BY expiry")]
    return jsonify(products=products, insights=insights, batches=batches,
                   alerts=[dict(sev=s, msg=m) for _, s, m in A],
                   kpis=dict(units=sum(p["stock"] for p in products), products=len(products),
                             critical=sum(1 for a in A if a[1] == "crit"),
                             cancelled=one("SELECT COUNT(*) FROM events WHERE type='cancel'")))

# ----------------------------------------------------------------- dashboard
PAGE = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>StockSense</title>
<style>body{font:15px system-ui;margin:0;background:#f6f7f3;color:#1c2b2a}main{max-width:1000px;margin:auto;padding:16px}
.c{background:#fff;border:1px solid #dfe4dd;border-radius:8px;padding:14px;margin-bottom:14px;overflow-x:auto}
table{width:100%;border-collapse:collapse}td,th{padding:6px 8px;border-bottom:1px solid #eee;text-align:left;white-space:nowrap}
.k{display:flex;gap:10px;flex-wrap:wrap}.k div{flex:1 1 120px;background:#fff;border-left:4px solid #1d7a5a;padding:10px}.k b{font-size:22px;display:block}
.crit{border-left:4px solid #c0392b;padding:6px 8px;margin:4px 0;background:#fbeeee}.warn{border-left:4px solid #b7791f;padding:6px 8px;margin:4px 0;background:#fbf5e8}
input,select,button{font:inherit;padding:7px;margin:2px}button{background:#12403a;color:#fff;border:0;border-radius:6px;cursor:pointer}</style>
<main><h1>StockSense</h1><div class=k id=k></div>
<div class=c><h3>Alerts</h3><div id=al></div></div><div class=c><h3>AI drift insights</h3><div id=in></div></div>
<div class=c><h3>Stock</h3><table id=pt></table></div><div class=c><h3>Batches and expiry</h3><table id=bt></table></div>
<div class=c><h3>Actions</h3>
<input id=rn placeholder="Product"><input id=rq type=number placeholder=Qty><input id=re type=date><button onclick="post('/api/receive',{name:rn.value,qty:rq.value,expiry:re.value})">Receive</button><br>
<select id=sp></select><input id=sq type=number value=1><button onclick="post('/api/order',{product_id:sp.value,qty:sq.value})">Order</button>
<input id=cq type=number placeholder="Counted"><button onclick="post('/api/count',{product_id:sp.value,actual:cq.value})">Count</button>
<button onclick="post('/api/writeoff',{})">Write off expired</button></div></main>
<script>
const $=i=>document.getElementById(i),e=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
async function post(u,b){const r=await fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)});
 if(!r.ok&&r.status!=409)alert('Error');else if(r.status==409)alert('Order cancelled: not enough stock');load()}
async function load(){const d=await(await fetch('/api/dashboard')).json(),v=$('sp').value;
 $('k').innerHTML=Object.entries(d.kpis).map(([k,x])=>`<div><b>${x}</b>${k}</div>`).join('');
 $('al').innerHTML=d.alerts.map(a=>`<div class=${a.sev}>${e(a.msg)}</div>`).join('')||'All clear';
 $('in').innerHTML=d.insights.map(i=>`<div class=crit><b>${e(i.product)}</b>: ${i.drift} units (z ${i.z}) ${i.reasons.map(r=>'<br>&bull; '+e(r)).join('')}
  ${i.resolved?'<br><i>Reconciled</i>':`<br><button onclick="post('/api/reconcile/${i.id}',{})">Correct system stock</button>`}</div>`).join('')||'No anomalies';
 $('pt').innerHTML='<tr><th>Product<th>System<th>Counted<th>Drift<th>Demand/day<th>Days left</tr>'+d.products.map(p=>`<tr><td>${e(p.name)}<td>${p.stock}<td>${p.counted??'-'}<td>${p.drift??'-'}<td>${p.demand}<td>${p.days_left??'-'}</tr>`).join('');
 $('bt').innerHTML='<tr><th>Product<th>Batch<th>Left<th>Expires in (days)</tr>'+d.batches.map(b=>`<tr><td>${e(b.product)}<td>${e(b.batch)}<td>${b.rem}<td>${b.days}</tr>`).join('');
 $('sp').innerHTML=d.products.map(p=>`<option value=${p.id}>${e(p.name)}</option>`).join('');if(v)$('sp').value=v}
load();setInterval(load,30000);
</script>"""

@app.get("/")
def index():
    return render_template_string(PAGE)

# ------------------------------------------------------------------ demo data
def seed():
    with app.app_context():
        db().executescript("DELETE FROM products;DELETE FROM batches;DELETE FROM events;DELETE FROM audits;DELETE FROM alert_log;")
        now = time.time()
        # name, avg daily demand, expiry-days A, expiry-days B, leftover A, leftover B, reorder
        for name, m, ea, eb, ra, rb, ro in [("Amul Milk 500ml", 20, 2, 7, 8, 30, 12), ("Farm Eggs 6pc", 14, 5, 12, 10, 20, 10),
                                            ("Bread Loaf", 10, -1, 3, 6, 15, 10), ("Curd 400g", 8, 20, 25, 12, 15, 8),
                                            ("Lays Chips", 25, 90, 120, 30, 40, 15)]:
            sales = [(d, round(m * random.uniform(0.7, 1.3))) for d in range(14, 0, -1)]
            T = sum(s for _, s in sales)
            qa = round(T * 0.7)
            pid = receive(name, "A1", qa + ra, now + ea * DAY, now - 15 * DAY)
            receive(name, "B1", T - qa + rb, now + eb * DAY, now - 15 * DAY)
            run("UPDATE products SET reorder=? WHERE id=?", (ro, pid))
            for d, s in sales:
                fifo(pid, s, True)
                log_event(pid, "order", s, ts=now - d * DAY + 3600)
            for k, dr in enumerate([1, -1, 0, 1]):      # normal audit history
                run("INSERT INTO audits(ts,pid,expected,actual,drift,anomaly,z,reasons) VALUES(?,?,?,?,?,0,0,'[]')",
                    (now - (8 - k) * DAY, pid, 30, 30 + dr, dr))
            record_count(pid, stock(pid) - (11 if name == "Farm Eggs 6pc" else 0))   # eggs: planted drift
    print("Demo data loaded.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        seed()
    else:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
