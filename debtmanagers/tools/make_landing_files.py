"""
Builds the dummy landing CSV files for the Databricks project from the practice-pack data.
   python tools/make_landing_files.py
Reads  ../technical_tasks/data/debt_collection.db and the two messy KPE placement files.
Writes data/landing/<source>/<source>_<YYYY-MM-DD>.csv, three business days:

  2026-11-13  history: every account, payment and contact attempt so far (first full load)
  2026-11-14  daily drop: new KPE placements (messy), new payments, new contacts
  2026-11-15  daily drop: changed/fixed/new KPE placements, payments incl. a LATE one,
              a payment re-sent from yesterday, a payment for an unknown account

All data is fake. Fixed seed, so the files are identical every time.
"""
import csv
import datetime as dt
import os
import random
import sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DB = os.path.join(ROOT, "..", "technical_tasks", "data", "debt_collection.db")
SRC_CSV = os.path.join(ROOT, "..", "technical_tasks", "data")
LANDING = os.path.join(ROOT, "data", "landing")
rng = random.Random(7)

CODES = {1: "KPE", 2: "HTL", 3: "SFN", 4: "ADG"}
COMMISSION = {1: 0.18, 2: 0.22, 3: 0.15, 4: 0.20}
PLACEMENT_HEADER = ["client_code", "client_account_ref", "first_name", "last_name", "date_of_birth",
                    "phone", "email", "balance_owing", "debt_date"]


def write(source, day, header, rows):
    folder = os.path.join(LANDING, source)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{source}_{day}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {os.path.relpath(path, ROOT):<55} {len(rows):>4} rows")


def messy_date(d):
    d = dt.date.fromisoformat(d)
    return rng.choice([d.isoformat(), d.strftime("%d/%m/%Y"), f"{d.day} {d.strftime('%b %Y')}", d.strftime("%Y%m%d")])


def messy_money(x):
    return rng.choice([f"{x:.2f}", f"${x:,.2f}", f"{x:,.2f}", f"{x}"])


def messy_phone(p):  # p like 0211234567 or already messy
    if not p.startswith("02"):
        return p
    return rng.choice([p, f"{p[:3]} {p[3:6]} {p[6:]}", f"+64 {p[1:3]} {p[3:6]} {p[6:]}", f"64-{p[1:3]}-{p[3:6]}-{p[6:]}"])


def acct_ref(client_id, client_ref):
    return f"{CODES[client_id]}-{client_ref.split('-')[1]}"


def main():
    con = sqlite3.connect(SRC_DB)

    # ---- reference data: clients ------------------------------------------------
    clients = [(CODES[i], n, ind, COMMISSION[i], "2025-07-01") for i, n, ind in
               con.execute("SELECT client_id, client_name, industry FROM clients ORDER BY 1")]
    write("clients", "2026-11-13", ["client_code", "client_name", "industry", "commission_rate", "onboarded_date"], clients)

    # ---- placements -----------------------------------------------------------
    hist = []
    for r in con.execute("""SELECT a.client_id, a.client_ref, d.first_name, d.last_name, d.dob, d.phone, d.email,
                                   a.placed_balance, a.placed_date
                            FROM accounts a JOIN debtors d USING (debtor_id) ORDER BY a.account_id"""):
        cid, cref, fn, ln, dob, phone, email, bal, placed = r
        hist.append([CODES[cid], acct_ref(cid, cref), fn, ln, messy_date(dob), messy_phone(phone), email,
                     messy_money(bal), messy_date(placed)])
    write("placements", "2026-11-13", PLACEMENT_HEADER, hist)
    for day in ["2026-11-14", "2026-11-15"]:
        with open(os.path.join(SRC_CSV, f"placements_{day}.csv"), encoding="utf-8") as f:
            rows = list(csv.reader(f))[1:]
        write("placements", day, PLACEMENT_HEADER, rows)

    # ---- bank payments ----------------------------------------------------------
    pay_header = ["bank_txn_id", "value_date", "client_account_ref", "payer_name", "amount", "transaction_type"]
    pays = []
    for pid, aid, pdate, amt, method, cid, cref, fn, ln in con.execute(
            """SELECT p.payment_id, p.account_id, p.payment_date, p.amount, p.method, a.client_id, a.client_ref,
                      d.first_name, d.last_name
               FROM payments p JOIN accounts a USING (account_id) JOIN debtors d USING (debtor_id)
               ORDER BY p.payment_id"""):
        txn = pid if pid != 215 else 11            # 215 is the bank re-sending txn 11: same bank id
        ttype = "DISHONOUR" if amt < 0 else ("DIRECT_DEBIT" if method == "DD" else method.upper())
        pays.append([f"BNZ{txn:08d}", pdate, acct_ref(cid, cref), f"{fn.strip().upper()} {ln.strip().upper()}",
                     f"{amt:.2f}", ttype])
    write("bank_payments", "2026-11-13", pay_header, pays)

    day14 = [["BNZ00000901", "2026-11-14", "KPE-50001", "AROHA NGATA", "100.00", "BANK_TRANSFER"],
             ["BNZ00000902", "2026-11-14", "ADG-718439", "MERE WALKER", "250.00", "CARD"],
             ["BNZ00000903", "2026-11-14", "SFN-110644", "WIREMU TUPOU", "75.00", "DIRECT_DEBIT"],
             ["BNZ00000904", "2026-11-14", "KPE-50003", "MERE PARATA", "88.20", "CARD"]]   # paid off in full
    day15 = [["BNZ00000903", "2026-11-14", "SFN-110644", "WIREMU TUPOU", "75.00", "DIRECT_DEBIT"],  # re-sent from yesterday
             ["BNZ00000905", "2026-10-28", "HTL-947458", "EMMA SMITH", "500.00", "BANK_TRANSFER"],  # LATE: October payment
             ["BNZ00000906", "2026-11-15", "KPE-50002", "LIAM SMITH", "200.00", "BANK_TRANSFER"],
             ["BNZ00000907", "2026-11-15", "KPE-99999", "UNKNOWN PAYER", "40.00", "BANK_TRANSFER"],  # no such account
             ["BNZ00000908", "2026-11-15", "KPE-50012", "MIA WALKER", "75.00", "CARD"]]
    write("bank_payments", "2026-11-14", pay_header, day14)
    write("bank_payments", "2026-11-15", pay_header, day15)

    # Bank control (trailer) file: what the bank says it sent. Used for reconciliation.
    def control(day, rows):
        uniq = {r[0]: float(r[4]) for r in rows}           # the bank counts unique transactions
        write("bank_control", day, ["file_name", "txn_count", "control_total"],
              [[f"bank_payments_{day}.csv", len(uniq), f"{sum(uniq.values()):.2f}"]])
    control("2026-11-13", pays)
    control("2026-11-14", day14)
    control("2026-11-15", day15)

    # ---- contact attempts --------------------------------------------------------
    c_header = ["attempt_id", "client_account_ref", "attempt_ts", "channel", "outcome", "agent_id"]
    contacts = []
    for aid, acc, ts, ch, out, cid, cref in con.execute(
            """SELECT c.attempt_id, c.account_id, c.attempt_ts, c.channel, c.outcome, a.client_id, a.client_ref
               FROM contact_attempts c JOIN accounts a USING (account_id) ORDER BY c.attempt_id"""):
        agent = f"AG{rng.randint(1, 9):02d}" if ch == "call" else ""
        contacts.append([f"DL{aid:07d}", acct_ref(cid, cref), ts, ch, out, agent])
    write("contact_attempts", "2026-11-13", c_header, contacts)
    write("contact_attempts", "2026-11-14", c_header, [
        ["DL0000901", "KPE-50001", "2026-11-14 09:15", "call", "promise_to_pay", "AG03"],
        ["DL0000902", "KPE-50002", "2026-11-14 09:40", "call", "no_answer", "AG03"],
        ["DL0000903", "KPE-50004", "2026-11-14 10:05", "sms", "sent", ""],
        ["DL0000904", "SFN-110644", "2026-11-14 11:30", "call", "right_party_contact", "AG05"]])
    write("contact_attempts", "2026-11-15", c_header, [
        ["DL0000905", "KPE-50002", "2026-11-15 09:10", "call", "promise_to_pay", "AG03"],
        ["DL0000906", "KPE-50012", "2026-11-15 09:30", "email", "sent", ""],
        ["DL0000907", "KPE-50007", "2026-11-15 10:00", "call", "wrong_number", "AG07"],
        ["DL0000904", "SFN-110644", "2026-11-14 11:30", "call", "right_party_contact", "AG05"]])  # re-sent


if __name__ == "__main__":
    print("Writing landing files:")
    main()
