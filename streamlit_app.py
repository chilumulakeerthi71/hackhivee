"""
StockSense - Smart inventory for quick-commerce stores.
Streamlit version of the original Flask + SQLite application.

Run locally:
    pip install -r requirements.txt
    streamlit run streamlit_app.py
"""

import json
import math
import os
import random
import sqlite3
import time
import urllib.request
from datetime import datetime, date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

DB = os.environ.get("DB_PATH", "inventory.db")
DAY = 86400

SCHEMA = """
CREATE TABLE IF NOT EXISTS products(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    reorder INTEGER DEFAULT 10
);
CREATE TABLE IF NOT EXISTS batches(
    id INTEGER PRIMARY KEY,
    pid INT,
    batch_no TEXT,
    qty INT,
    rem INT,
    expiry REAL
);
CREATE TABLE IF NOT EXISTS events(
    id INTEGER PRIMARY KEY,
    ts REAL,
    pid INT,
    type TEXT,
    qty INT,
    note TEXT
);
CREATE TABLE IF NOT EXISTS audits(
    id INTEGER PRIMARY KEY,
    ts REAL,
    pid INT,
    expected INT,
    actual INT,
    drift INT,
    anomaly INT,
    z REAL,
    reasons TEXT,
    resolved INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS alert_log(
    id INTEGER PRIMARY KEY,
    ts REAL,
    k TEXT UNIQUE,
    sev TEXT,
    msg TEXT
);
"""


def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    return c


def q(sql, args=()):
    c = conn()
    try:
        return c.execute(sql, args).fetchall()
    finally:
        c.close()


def one(sql, args=()):
    c = conn()
    try:
        r = c.execute(sql, args).fetchone()
        return r[0] if r and r[0] is not None else 0
    finally:
        c.close()


def run(sql, args=()):
    c = conn()
    try:
        cur = c.execute(sql, args)
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


def log_event(pid, typ, qty, note="", ts=None):
    run(
        "INSERT INTO events(ts,pid,type,qty,note) VALUES(?,?,?,?,?)",
        (ts or time.time(), pid, typ, qty, note),
    )


def receive(name, batch_no, qty, expiry, ts=None):
    row = q("SELECT id FROM products WHERE name=? COLLATE NOCASE", (name,))
    pid = row[0]["id"] if row else run(
        "INSERT INTO products(name) VALUES(?)", (name,)
    )
    run(
        "INSERT INTO batches(pid,batch_no,qty,rem,expiry) VALUES(?,?,?,?,?)",
        (pid, batch_no, qty, qty, expiry),
    )
    log_event(pid, "receive", qty, batch_no, ts)
    return pid


def stock(pid):
    return one("SELECT SUM(rem) FROM batches WHERE pid=?", (pid,))


def sellable(pid):
    return one(
        "SELECT SUM(rem) FROM batches WHERE pid=? AND expiry>?",
        (pid, time.time()),
    )


def fifo(pid, n, include_expired=False):
    sql = (
        "SELECT id,rem FROM batches WHERE pid=? AND rem>0"
        + ("" if include_expired else " AND expiry>?")
        + " ORDER BY expiry"
    )
    left = int(n)
    args = (pid,) if include_expired else (pid, time.time())
    for b in q(sql, args):
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
    b = q(
        "SELECT id FROM batches WHERE pid=? ORDER BY expiry DESC LIMIT 1",
        (pid,),
    )
    if b:
        run("UPDATE batches SET rem=rem+? WHERE id=?", (n, b[0]["id"]))
    else:
        run(
            "INSERT INTO batches(pid,batch_no,qty,rem,expiry) VALUES(?,?,?,?,?)",
            (pid, "RETURN", n, n, time.time() + 365 * DAY),
        )


def write_off_expired():
    total = 0
    for b in q(
        "SELECT * FROM batches WHERE rem>0 AND expiry<?",
        (time.time(),),
    ):
        qty = b["rem"]
        log_event(b["pid"], "expire", qty, b["batch_no"])
        run("UPDATE batches SET rem=0 WHERE id=?", (b["id"],))
        total += qty
    return total


def analyse(pid, expected, actual):
    drift = actual - expected
    hist = [
        abs(r["drift"])
        for r in q("SELECT drift FROM audits WHERE pid=?", (pid,))
    ]
    mean = sum(hist) / len(hist) if hist else 0
    sd = (
        math.sqrt(sum((h - mean) ** 2 for h in hist) / len(hist))
        if len(hist) > 2
        else 0
    )
    z = (abs(drift) - mean) / sd if sd else 0
    anomaly = abs(drift) > max(3, 0.08 * expected) or z > 2.5

    reasons = []
    if anomaly:
        avg_order = one(
            "SELECT AVG(qty) FROM events WHERE pid=? AND type='order'",
            (pid,),
        ) or 1
        n = abs(drift)

        if drift < 0:
            if one(
                "SELECT COUNT(*) FROM batches WHERE pid=? AND rem>0 AND expiry<?",
                (pid, time.time() + DAY),
            ):
                reasons.append(
                    "Expired or near-expiry units removed from shelf but not written off"
                )
            if n >= 0.8 * avg_order:
                reasons.append(
                    f"Unrecorded sale (about {n / avg_order:.1f} typical orders), "
                    "e.g. offline sale"
                )
            reasons.append(
                "Damage or spoilage not logged"
                if n < max(3, 0.08 * expected) * 2
                else "Possible theft or mis-picking; audit this shelf"
            )
        else:
            reasons += [
                "Return or stock receipt not recorded",
                "Data-entry error on an inbound quantity",
            ]

        if actual != expected and str(actual)[::-1] == str(expected):
            reasons.insert(0, "Digits look transposed (typing error)")

    return {
        "drift": drift,
        "anomaly": int(anomaly),
        "z": round(z, 1),
        "reasons": reasons,
    }


def record_count(pid, actual):
    expected = stock(pid)
    a = analyse(pid, expected, actual)
    aid = run(
        """
        INSERT INTO audits(ts,pid,expected,actual,drift,anomaly,z,reasons)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            time.time(),
            pid,
            expected,
            actual,
            a["drift"],
            a["anomaly"],
            a["z"],
            json.dumps(a["reasons"]),
        ),
    )
    return {"id": aid, "expected": expected, "actual": actual, **a}


def reconcile(aid):
    rows = q("SELECT * FROM audits WHERE id=?", (aid,))
    if not rows:
        return
    a = rows[0]
    diff = stock(a["pid"]) - a["actual"]
    if diff > 0:
        fifo(a["pid"], diff, include_expired=True)
    elif diff < 0:
        add_back(a["pid"], -diff)
    log_event(a["pid"], "adjust", -diff, "audit reconcile")
    run("UPDATE audits SET resolved=1 WHERE id=?", (aid,))


def daily_sales(pid, days=14):
    out = [0] * days
    now = time.time()
    for r in q(
        "SELECT ts,qty FROM events WHERE pid=? AND type='order' AND ts>?",
        (pid, now - days * DAY),
    ):
        idx = days - 1 - int((now - r["ts"]) // DAY)
        if 0 <= idx < days:
            out[idx] += r["qty"]
    return out


def demand(pid):
    d = daily_sales(pid, 7)
    return 0.6 * sum(d[4:]) / 3 + 0.4 * sum(d) / 7


def days_left(pid):
    dm = demand(pid)
    return sellable(pid) / dm if dm > 0 else None


def compute_alerts():
    alerts = []
    now = time.time()

    for p in q("SELECT * FROM products"):
        pid = p["id"]
        name = p["name"]
        s = sellable(pid)
        dl = days_left(pid)

        if s <= p["reorder"]:
            alerts.append(
                (
                    f"low{pid}-{s}",
                    "warn",
                    f"{name}: low stock ({s} left, reorder level {p['reorder']})",
                )
            )

        if dl is not None and dl < 2:
            alerts.append(
                (
                    f"so{pid}-{int(dl * 4)}",
                    "crit",
                    f"{name}: predicted to run out in about {dl:.1f} days",
                )
            )

        for b in q(
            "SELECT * FROM batches WHERE pid=? AND rem>0",
            (pid,),
        ):
            left = (b["expiry"] - now) / DAY
            if left < 0:
                alerts.append(
                    (
                        f"ex{b['id']}",
                        "crit",
                        f"{name}: {b['rem']} units expired (batch {b['batch_no']})",
                    )
                )
            elif left < 3:
                alerts.append(
                    (
                        f"ne{b['id']}",
                        "warn",
                        f"{name}: {b['rem']} units expire in {left:.0f} days "
                        f"(batch {b['batch_no']})",
                    )
                )

        a = q(
            "SELECT * FROM audits WHERE pid=? ORDER BY id DESC LIMIT 1",
            (pid,),
        )
        if a and a[0]["anomaly"] and not a[0]["resolved"]:
            alerts.append(
                (
                    f"dr{a[0]['id']}",
                    "crit",
                    f"{name}: inventory drift of {a[0]['drift']} units",
                )
            )

    cancelled = one(
        "SELECT COUNT(*) FROM events WHERE type='cancel'"
    )
    if cancelled:
        alerts.append(
            (
                f"cx{cancelled}",
                "warn",
                f"{cancelled} order(s) cancelled for lack of stock",
            )
        )

    return alerts


def notify(msg):
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        req = urllib.request.Request(
            url,
            json.dumps({"text": "StockSense: " + msg}).encode(),
            {"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass


def dispatch(alerts):
    for k, sev, msg in alerts:
        if not q("SELECT 1 FROM alert_log WHERE k=?", (k,)):
            run(
                "INSERT INTO alert_log(ts,k,sev,msg) VALUES(?,?,?,?)",
                (time.time(), k, sev, msg),
            )
            notify(msg)


def dashboard_data():
    alerts = compute_alerts()
    dispatch(alerts)

    products = []
    for p in q("SELECT * FROM products"):
        a = q(
            "SELECT * FROM audits WHERE pid=? ORDER BY id DESC LIMIT 1",
            (p["id"],),
        )
        dl = days_left(p["id"])
        products.append(
            {
                "id": p["id"],
                "name": p["name"],
                "stock": stock(p["id"]),
                "sellable": sellable(p["id"]),
                "counted": a[0]["actual"] if a else None,
                "drift": a[0]["drift"] if a else None,
                "days_left": None if dl is None else round(dl, 1),
                "demand": round(demand(p["id"]), 1),
                "sales14": daily_sales(p["id"]),
                "reorder": p["reorder"],
            }
        )

    insights = [
        {
            "id": r["id"],
            "product": r["name"],
            "drift": r["drift"],
            "z": r["z"],
            "resolved": r["resolved"],
            "reasons": json.loads(r["reasons"]),
        }
        for r in q(
            """
            SELECT a.*,p.name
            FROM audits a
            JOIN products p ON p.id=a.pid
            WHERE anomaly=1
            ORDER BY a.id DESC LIMIT 5
            """
        )
    ]

    batches = [
        {
            "product": r["name"],
            "batch": r["batch_no"],
            "rem": r["rem"],
            "days": round((r["expiry"] - time.time()) / DAY, 1),
        }
        for r in q(
            """
            SELECT b.*,p.name
            FROM batches b
            JOIN products p ON p.id=b.pid
            WHERE rem>0
            ORDER BY expiry
            """
        )
    ]

    return products, insights, batches, alerts


def clear_demo_data():
    c = conn()
    try:
        c.executescript(
            """
            DELETE FROM products;
            DELETE FROM batches;
            DELETE FROM events;
            DELETE FROM audits;
            DELETE FROM alert_log;
            """
        )
        c.commit()
    finally:
        c.close()


def seed():
    clear_demo_data()
    now = time.time()

    demo = [
        ("Amul Milk 500ml", 20, 2, 7, 8, 30, 12),
        ("Farm Eggs 6pc", 14, 5, 12, 10, 20, 10),
        ("Bread Loaf", 10, -1, 3, 6, 15, 10),
        ("Curd 400g", 8, 20, 25, 12, 15, 8),
        ("Lays Chips", 25, 90, 120, 30, 40, 15),
    ]

    for name, m, ea, eb, ra, rb, ro in demo:
        sales = [
            (d, round(m * random.uniform(0.7, 1.3)))
            for d in range(14, 0, -1)
        ]
        total_sales = sum(s for _, s in sales)
        qa = round(total_sales * 0.7)

        pid = receive(
            name,
            "A1",
            qa + ra,
            now + ea * DAY,
            now - 15 * DAY,
        )
        receive(
            name,
            "B1",
            total_sales - qa + rb,
            now + eb * DAY,
            now - 15 * DAY,
        )
        run(
            "UPDATE products SET reorder=? WHERE id=?",
            (ro, pid),
        )

        for d, s in sales:
            fifo(pid, s, True)
            log_event(pid, "order", s, ts=now - d * DAY + 3600)

        for k, dr in enumerate([1, -1, 0, 1]):
            run(
                """
                INSERT INTO audits(
                    ts,pid,expected,actual,drift,anomaly,z,reasons
                )
                VALUES(?,?,?,?,?,0,0,'[]')
                """,
                (now - (8 - k) * DAY, pid, 30, 30 + dr, dr),
            )

        record_count(
            pid,
            stock(pid) - (11 if name == "Farm Eggs 6pc" else 0),
        )


def app_header():
    st.set_page_config(
        page_title="StockSense",
        page_icon="📦",
        layout="wide",
    )
    st.title("📦 StockSense")
    st.caption(
        "Smart inventory intelligence for quick-commerce stores"
    )


def show_dashboard():
    products, insights, batches, alerts = dashboard_data()

    kpis = {
        "Units": sum(p["stock"] for p in products),
        "Products": len(products),
        "Critical": sum(1 for a in alerts if a[1] == "crit"),
        "Cancelled": one(
            "SELECT COUNT(*) FROM events WHERE type='cancel'"
        ),
    }

    cols = st.columns(4)
    for col, (label, value) in zip(cols, kpis.items()):
        col.metric(label, value)

    st.subheader("🚨 Alerts")
    if alerts:
        for _, severity, msg in alerts:
            if severity == "crit":
                st.error(msg)
            else:
                st.warning(msg)
    else:
        st.success("All clear — no active alerts.")

    st.subheader("🤖 AI Drift Insights")
    if insights:
        for item in insights:
            with st.container(border=True):
                st.markdown(
                    f"**{item['product']}** — drift: "
                    f"`{item['drift']}` units, z-score: `{item['z']}`"
                )
                for reason in item["reasons"]:
                    st.write("•", reason)

                if item["resolved"]:
                    st.caption("Reconciled")
                else:
                    if st.button(
                        "Correct system stock",
                        key=f"reconcile_{item['id']}",
                    ):
                        reconcile(item["id"])
                        st.success("Stock reconciled.")
                        st.rerun()
    else:
        st.info("No anomalies detected.")

    st.subheader("📊 Stock")
    if products:
        df = pd.DataFrame(products)
        display_df = df[
            [
                "name",
                "stock",
                "sellable",
                "counted",
                "drift",
                "demand",
                "days_left",
                "reorder",
            ]
        ].rename(
            columns={
                "name": "Product",
                "stock": "System Stock",
                "sellable": "Sellable",
                "counted": "Counted",
                "drift": "Drift",
                "demand": "Demand/day",
                "days_left": "Days Left",
                "reorder": "Reorder Level",
            }
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No products yet. Load demo data or receive stock.")

    st.subheader("⏳ Batches and Expiry")
    if batches:
        st.dataframe(
            pd.DataFrame(batches).rename(
                columns={
                    "product": "Product",
                    "batch": "Batch",
                    "rem": "Units Left",
                    "days": "Expires In (days)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No active batches.")

    if products:
        st.subheader("📈 Demand")
        chart_df = pd.DataFrame(
            [
                {
                    "Product": p["name"],
                    "Demand per day": p["demand"],
                }
                for p in products
            ]
        )
        fig = px.bar(
            chart_df,
            x="Product",
            y="Demand per day",
            title="Estimated daily demand",
        )
        st.plotly_chart(fig, use_container_width=True)


def show_actions():
    st.subheader("Actions")

    products = q("SELECT * FROM products ORDER BY name")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["Receive", "Order", "Count", "Write-off"]
    )

    with tab1:
        with st.form("receive_form"):
            name = st.text_input("Product name")
            batch_no = st.text_input(
                "Batch number",
                value="B" + str(int(time.time())),
            )
            qty = st.number_input(
                "Quantity",
                min_value=1,
                value=10,
                step=1,
            )
            expiry = st.date_input(
                "Expiry date",
                value=date.today() + timedelta(days=7),
            )
            submitted = st.form_submit_button("Receive stock")

            if submitted:
                if not name.strip():
                    st.error("Enter a product name.")
                else:
                    expiry_ts = (
                        datetime.combine(expiry, datetime.min.time()).timestamp()
                        + DAY
                        - 1
                    )
                    pid = receive(
                        name.strip(),
                        batch_no.strip() or "B" + str(int(time.time())),
                        int(qty),
                        expiry_ts,
                    )
                    st.success(
                        f"Stock received successfully. Product ID: {pid}"
                    )
                    st.rerun()

    with tab2:
        if products:
            product_map = {
                f"{p['name']} (ID {p['id']})": p["id"]
                for p in products
            }
            selected = st.selectbox(
                "Product",
                list(product_map.keys()),
                key="order_product",
            )
            qty = st.number_input(
                "Order quantity",
                min_value=1,
                value=1,
                step=1,
                key="order_qty",
            )

            if st.button("Place order"):
                pid = product_map[selected]
                if place_order(pid, int(qty)):
                    st.success("Order placed successfully.")
                else:
                    st.error("Order cancelled: not enough sellable stock.")
                st.rerun()
        else:
            st.info("Add stock first.")

    with tab3:
        if products:
            product_map = {
                f"{p['name']} (ID {p['id']})": p["id"]
                for p in products
            }
            selected = st.selectbox(
                "Product",
                list(product_map.keys()),
                key="count_product",
            )
            actual = st.number_input(
                "Physical counted quantity",
                min_value=0,
                value=0,
                step=1,
            )

            if st.button("Record count"):
                result = record_count(
                    product_map[selected],
                    int(actual),
                )
                if result["anomaly"]:
                    st.error(
                        f"Anomaly detected. Drift: {result['drift']} units."
                    )
                    for reason in result["reasons"]:
                        st.write("•", reason)
                else:
                    st.success(
                        f"Count recorded. Drift: {result['drift']} units."
                    )
                st.rerun()
        else:
            st.info("Add stock first.")

    with tab4:
        if st.button("Write off all expired stock"):
            written = write_off_expired()
            st.success(f"Written off {written} expired units.")
            st.rerun()


def show_event_history():
    st.subheader("Event History")
    rows = q(
        """
        SELECT e.id, e.ts, p.name AS product, e.type, e.qty, e.note
        FROM events e
        LEFT JOIN products p ON p.id=e.pid
        ORDER BY e.id DESC
        LIMIT 100
        """
    )
    if not rows:
        st.info("No events yet.")
        return

    df = pd.DataFrame([dict(r) for r in rows])
    df["time"] = pd.to_datetime(df["ts"], unit="s")
    df = df[["id", "time", "product", "type", "qty", "note"]]
    st.dataframe(df, use_container_width=True, hide_index=True)


def main():
    app_header()

    with st.sidebar:
        st.header("Controls")
        st.caption("Use demo data to test the full workflow.")

        if st.button("🌱 Load demo data", use_container_width=True):
            seed()
            st.success("Demo data loaded.")
            st.rerun()

        if st.button("🗑️ Clear all data", use_container_width=True):
            clear_demo_data()
            st.success("All inventory data cleared.")
            st.rerun()

        st.divider()
        st.write("Database:", DB)

    dashboard_tab, actions_tab, history_tab = st.tabs(
        ["📊 Dashboard", "⚙️ Actions", "📝 History"]
    )

    with dashboard_tab:
        show_dashboard()

    with actions_tab:
        show_actions()

    with history_tab:
        show_event_history()


if __name__ == "__main__":
    main()
