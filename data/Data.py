# Generating compact, realistic GPM PoC dataset (Option A: ~50 rows per CSV, Unstructured depth Option 1: ~10 documents)
import os, random, zipfile, textwrap
from datetime import datetime, timedelta
import pandas as pd

random.seed(2025)
out_dir = "/mnt/data/gpm_poc_compact"
os.makedirs(out_dir, exist_ok=True)

# Time window
start_dt = datetime(2025,1,1)
end_dt = datetime(2025,10,1)
def rand_ts(start=start_dt, end=end_dt):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

# Scenarios mapping
scenarios = [
    {"id":"SCEN1","name":"Equity SSI Late Update","tag":"SSI"},
    {"id":"SCEN2","name":"FI FX Conversion Mismatch","tag":"FX"},
    {"id":"SCEN3","name":"Corporate Action Sync Failure","tag":"CA"},
    {"id":"SCEN4","name":"Interbook Transfer Mapping Error","tag":"IB"},
    {"id":"SCEN5","name":"Nostro Shortfall - Funding Issue","tag":"NS"}
]

# 1) Trades - 50 rows, distributed across scenarios
instruments = ["MSFT.O","AAPL.O","DBR.X","JPM.N","NIKK.N","GOVBOND10Y","EURUSDSPOT","USDJPYSPOT","SPX_OPT","CDS_UK"]
trades = []
for i in range(50):
    sc = scenarios[i % len(scenarios)]
    tdt = rand_ts()
    qty = random.choice([50,100,200,500,1000])
    price = round(random.uniform(5,300),2)
    trades.append({
        "Trade_ID": f"T{9000+i}",
        "Scenario": sc["id"],
        "Trade_Date": tdt.strftime("%Y-%m-%d %H:%M:%S"),
        "Trader": random.choice(["joe.trader","li.chen","maria.g","omar.khan","sara.r"]),
        "Desk": random.choice(["Equities-APAC","FI-EMEA","FX-NA","Derivatives-APAC"]),
        "Instrument": random.choice(instruments),
        "Asset_Class": "",
        "Quantity": qty,
        "Price": price,
        "Currency": random.choice(["USD","EUR","JPY","GBP"]),
        "Counterparty": random.choice([f"CP{str(x).zfill(3)}" for x in range(1,21)]),
        "Book": random.choice([f"Book{n}" for n in range(1,8)]),
        "Notes": ""
    })
# fill Asset_Class based on instrument
for r in trades:
    instr = r["Instrument"]
    if "." in instr or "SPX" in instr:
        r["Asset_Class"] = "Equity"
    elif "GOVBOND" in instr or "DBR" in instr:
        r["Asset_Class"] = "Fixed Income"
    elif "SPOT" in instr or "FX" in instr:
        r["Asset_Class"] = "FX"
    else:
        r["Asset_Class"] = "Derivatives"

df_trades = pd.DataFrame(trades)
df_trades.to_csv(os.path.join(out_dir,"trades.csv"), index=False)

# 2) Positions - one snapshot per trade + 20% corrected snapshots (~60 rows)
positions = []
for idx, row in df_trades.iterrows():
    pos_dt = datetime.strptime(row["Trade_Date"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=6)
    positions.append({
        "Position_ID": f"P{10000+idx}",
        "Trade_ID": row["Trade_ID"],
        "Snapshot": "T+0",
        "Valuation_Date": pos_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Quantity": row["Quantity"],
        "Market_Value": round(row["Quantity"] * row["Price"],2),
        "Book": row["Book"]
    })
    if random.random() < 0.2:
        pos_dt2 = pos_dt + timedelta(days=1)
        corr = row["Quantity"] + random.choice([-int(row["Quantity"]*0.1), int(row["Quantity"]*0.2), 0])
        positions.append({
            "Position_ID": f"P{10000+idx}_1",
            "Trade_ID": row["Trade_ID"],
            "Snapshot": "T+1",
            "Valuation_Date": pos_dt2.strftime("%Y-%m-%d %H:%M:%S"),
            "Quantity": max(0, corr),
            "Market_Value": round(max(0,corr) * row["Price"],2),
            "Book": row["Book"]
        })
df_positions = pd.DataFrame(positions)
df_positions.to_csv(os.path.join(out_dir,"positions.csv"), index=False)

# 3) Settlements - 40 rows subset; inject scenario-specific failures
settlements = []
sample_idxs = random.sample(range(len(df_trades)), 40)
for idx in sample_idxs:
    t = df_trades.iloc[idx]
    settle_dt = datetime.strptime(t["Trade_Date"], "%Y-%m-%d %H:%M:%S") + timedelta(days=random.choice([1,2]))
    fail = False; fail_reason = ""
    if t["Scenario"]=="SCEN1" and random.random()<0.3:
        fail = True; fail_reason = "SSI_Mismatch"
    if t["Scenario"]=="SCEN2" and random.random()<0.2:
        fail = True; fail_reason = "FX_Conversion_Error"
    if t["Scenario"]=="SCEN3" and random.random()<0.15:
        fail = True; fail_reason = "CA_Not_Applied"
    if t["Scenario"]=="SCEN5" and random.random()<0.25:
        fail = True; fail_reason = "Insufficient_Funds"
    status = "Failed" if fail else random.choices(["Confirmed","Pending"], weights=[0.8,0.2])[0]
    settlements.append({
        "Settlement_ID": f"S{20000+idx}",
        "Trade_ID": t["Trade_ID"],
        "Settlement_Date": settle_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Quantity": t["Quantity"] - (0 if status=="Confirmed" else random.choice([0,5,10])),
        "Currency": t["Currency"],
        "Amount": round(t["Quantity"] * t["Price"],2),
        "Settlement_Status": status,
        "Fail_Reason": fail_reason,
        "Custodian": random.choice(["JPM","CITI","BOFA","BNP","HSBC"])
    })
df_settlements = pd.DataFrame(settlements)
df_settlements.to_csv(os.path.join(out_dir,"settlements.csv"), index=False)

# 4) Corporate Actions - 10 rows relevant to SCEN3
cas = []
for i in range(10):
    ca_dt = rand_ts()
    cas.append({
        "CA_ID": f"CA{500+i}",
        "Instrument": random.choice(instruments),
        "CA_Type": random.choice(["Split","Dividend","SpinOff","RightsIssue"]),
        "Effective_Date": ca_dt.strftime("%Y-%m-%d"),
        "Notes": "Feed update required across subledgers"
    })
df_cas = pd.DataFrame(cas)
df_cas.to_csv(os.path.join(out_dir,"corporate_actions.csv"), index=False)

# 5) Breaks - 30 rows, linked to trades/settlements where applicable
breaks = []
for i in range(30):
    t = df_trades.sample(1).iloc[0]
    detected = datetime.strptime(t["Trade_Date"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=random.randint(1,72))
    linked_settle = df_settlements[df_settlements["Trade_ID"]==t["Trade_ID"]]
    settle_id = linked_settle.iloc[0]["Settlement_ID"] if not linked_settle.empty and random.random()<0.7 else ""
    reason = linked_settle.iloc[0]["Fail_Reason"] if (not linked_settle.empty and linked_settle.iloc[0]["Fail_Reason"]) else random.choice(["AutoDetected","StaticDataError","IntegrationError","ToleranceExceeded"])
    breaks.append({
        "Break_ID": f"B{30000+i}",
        "Trade_ID": t["Trade_ID"],
        "Settlement_ID": settle_id,
        "Break_Type": random.choice(["Cash_Break","Quantity_Mismatch","Documentation_Gap","Pricing_Mismatch","SSI_Issue","Feed_Delay","CA_Mismatch"]),
        "Break_Reason": reason,
        "Detected_Date": detected.strftime("%Y-%m-%d %H:%M:%S"),
        "Status": random.choice(["Open","Investigating","Resolved"]),
        "Assigned_To": random.choice(["GPM_Analyst1","GPM_Analyst2","GPM_Analyst3","CustodyOps","StaticDataTeam"]),
        "Severity": random.choice(["High","Medium","Low"])
    })
df_breaks = pd.DataFrame(breaks)
df_breaks.to_csv(os.path.join(out_dir,"breaks.csv"), index=False)

# 6) ITSM tickets - 25 rows, rich descriptions, linked to breaks
itsm = []
systems = ["ReconciliationEngine","MQGateway","SettlementHub","PositionEngine","CAFeed","SSIService","TreasurySystem"]
for i in range(25):
    b = df_breaks.sample(1).iloc[0]
    created = rand_ts()
    itsm.append({
        "Ticket_ID": f"ITSM{7000+i}",
        "Linked_Break": b["Break_ID"],
        "System": random.choice(systems),
        "Priority": random.choice(["Low","Medium","High","Critical"]),
        "Summary": f"{b['Break_Type']} for trade {b['Trade_ID']}",
        "Description": ("Detailed: " + b["Break_Reason"] + ". Steps: Investigate static data, verify custodian confirmation, check MQ queues, apply manual fix if required."),
        "Created_On": created.strftime("%Y-%m-%d %H:%M:%S"),
        "Status": random.choice(["Open","In Progress","Resolved","Pending RCA"]),
        "Assigned_To": random.choice(["Infra Team","Middleware","Custody Team","GPM Support","Treasury Ops"])
    })
df_itsm = pd.DataFrame(itsm)
df_itsm.to_csv(os.path.join(out_dir,"itsm_tickets.csv"), index=False)

# 7) Change tickets - 8 rows (postmortem fixes)
changes = []
for i in range(8):
    created = rand_ts()
    changes.append({
        "Change_ID": f"CHG{900+i}",
        "Change_Date": created.strftime("%Y-%m-%d"),
        "Description": random.choice(["SSI table update","MQ consumer tuning","Reconciliation tolerance change","CA feed parser fix","Schedule adjustment"]),
        "Impact": random.choice(["Low","Medium","High"]),
        "Related_System": random.choice(systems),
        "Status": random.choice(["Planned","Completed","RolledBack"])
    })
df_changes = pd.DataFrame(changes)
df_changes.to_csv(os.path.join(out_dir,"change_tickets.csv"), index=False)

# 8) Audit trail - 50 rows (actions)
audits = []
actions = ["CREATE_BREAK","ASSIGN_ANALYST","UPDATE_POSITION","ESCALATE_TO_IT","RESOLVE_BREAK"]
for i in range(50):
    b = df_breaks.sample(1).iloc[0]
    ts = rand_ts()
    audits.append({
        "Audit_ID": f"AUD{4000+i}",
        "Entity_ID": b["Break_ID"],
        "Action": random.choice(actions),
        "User": random.choice(["GPM_Analyst1","GPM_Analyst2","GPM_Lead","CustodyOps","StaticDataTeam"]),
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "Notes": random.choice(["Logged by automation","Manual update after counterparty confirmation","Escalated following SLA breach","Resolved after SSI update"])
    })
df_audits = pd.DataFrame(audits)
df_audits.to_csv(os.path.join(out_dir,"audit_trail.csv"), index=False)

# 9) Emails - 10 multi-line realistic threads (Option1 depth)
email_threads = []
for i in range(10):
    b = df_breaks.sample(1).iloc[0]
    trade = df_trades[df_trades["Trade_ID"]==b["Trade_ID"]].iloc[0]
    t0 = datetime.strptime(trade["Trade_Date"], "%Y-%m-%d %H:%M:%S")
    subj = f"[Action Required] Settlement Break {b['Break_ID']} for {trade['Trade_ID']}"
    body = (
        f"From: {trade['Trader']}@bank.com\nTo: gpm_ops@bank.com\nCC: staticdata@bank.com,custody@bank.com,it_support@bank.com\nDate: {t0.strftime('%Y-%m-%d %H:%M:%S')}\nSubject: {subj}\n\n"
        f"Team,\n\nWe have a settlement failure for trade {trade['Trade_ID']} ({trade['Instrument']}). Reason: {b['Break_Reason']}. This affects client P&L reporting and may impact T+1 regulatory submission. Please advise corrective action and timeline.\n\nRegards,\n{trade['Trader']}\n\n"
        "-----Forwarded Message-----\n"
        f"From: custody@custodian.com\nTo: gpm_ops@bank.com\nDate: {(t0+timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')}\nSubject: Re: {subj}\n\nCustodian: We see SSI mismatch. Provide updated instructions.\n\n"
        f"Ops Reply: Assigned to {b['Assigned_To']}. ITSM ticket created: ITSM{random.randint(7000,7025)}\n"
    )
    email_threads.append(body)
with open(os.path.join(out_dir,"emails.txt"), "w") as f:
    f.write("\n\n".join(email_threads))

# 10) Chats - 10 threads (Option1 depth)
chat_threads = []
for i in range(10):
    b = df_breaks.sample(1).iloc[0]
    trade = df_trades[df_trades["Trade_ID"]==b["Trade_ID"]].iloc[0]
    t0 = datetime.strptime(trade["Trade_Date"], "%Y-%m-%d %H:%M:%S")
    chat = (
        f"[{(t0+timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}] ops_analyst: Created break {b['Break_ID']} for trade {trade['Trade_ID']}\n"
        f"[{(t0+timedelta(hours=1,minutes=10)).strftime('%Y-%m-%d %H:%M:%S')}] custody_ops: Checking SSI registry for CP {trade['Counterparty']}\n"
        f"[{(t0+timedelta(hours=1,minutes=25)).strftime('%Y-%m-%d %H:%M:%S')}] it_support: Observed MQ lag; creating ticket ITSM{random.randint(7000,7025)}\n"
        f"[{(t0+timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')}] ops_lead: If not resolved within SLA ({b['Severity']}), escalate to Treasury\n"
    )
    chat_threads.append(chat)
with open(os.path.join(out_dir,"chats.txt"), "w") as f:
    f.write("\n\n".join(chat_threads))

# 11) SOP and SLA documents (text)
sop_text = textwrap.dedent("""
SOP: GPM Break Handling - Key Steps
1. Detection: Automated reconciliation at T+0 flags exceptions in ReconciliationEngine.
2. Classification: Break types include Cash_Break, Quantity_Mismatch, SSI_Issue, CA_Mismatch, Others.
3. Assignment: Breaks auto-route to GPM queue; critical ones assigned to GPM_Lead.
4. Resolution: Engage StaticData, CustodyOps, or IT (MQGateway) depending on root cause.
5. Escalation: SLA breaches escalate to Treasury and Compliance; ITSM tickets must include RCA.
""")
with open(os.path.join(out_dir,"sop.txt"), "w") as f:
    f.write(sop_text)

sla_text = textwrap.dedent("""
SLA: GPM Breaks
- High severity: Resolve within 1 business hour.
- Medium severity: Resolve within T+1 business day.
- Low severity: Resolve within T+3 business days.
- ITSM tickets created by GPM must include Root Cause Analysis prior to closure.
""")
with open(os.path.join(out_dir,"sla.txt"), "w") as f:
    f.write(sla_text)

# 12) Relationships CSV (for Neo4j ingestion)
rels = []
for _, t in df_trades.iterrows():
    rels.append({"Source": t["Trade_ID"], "Target": f"P{10000+int(t.name)}", "Type": "HAS_POSITION"})
for _, s in df_settlements.iterrows():
    rels.append({"Source": s["Trade_ID"], "Target": s["Settlement_ID"], "Type": "HAS_SETTLEMENT"})
for _, b in df_breaks.iterrows():
    rels.append({"Source": b["Break_ID"], "Target": b["Trade_ID"], "Type": "BREAK_OF"})
for _, it in df_itsm.iterrows():
    rels.append({"Source": it["Ticket_ID"], "Target": it["Linked_Break"], "Type": "TICKET_FOR_BREAK"})
df_rels = pd.DataFrame(rels)
df_rels.to_csv(os.path.join(out_dir,"relationships.csv"), index=False)

# Zip up the folder
zip_path = "/mnt/data/gpm_poc_compact.zip"
with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for fname in sorted(os.listdir(out_dir)):
        zf.write(os.path.join(out_dir, fname), arcname=fname)

