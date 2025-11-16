# load_shipments.py
import os
import sqlite3
import pandas as pd

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "shipments.db")   # adjust if your DB is named differently
CSV0 = os.path.join(BASE_DIR, "spreadsheet_0.csv")  # self-contained products
CSV1 = os.path.join(BASE_DIR, "spreadsheet_1.csv")  # shipment products (one product per row)
CSV2 = os.path.join(BASE_DIR, "spreadsheet_2.csv")  # shipment routes (origin/destination per shipping_identifier)

# === helper ===
def table_exists(conn, table_name):
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,)
    )
    return cur.fetchone() is not None

def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Database file not found at: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Optional: show existing tables
    print("Existing tables:", [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")])

    # === Part 1: Insert spreadsheet_0 into 'products' table ===
    if os.path.exists(CSV0):
        df0 = pd.read_csv(CSV0)
        # Inspect/protect: ensure df0 columns match DB columns (you may need to rename)
        print("CSV0 columns:", df0.columns.tolist())
        # Use pandas to_sql append; replace table name as appropriate
        try:
            df0.to_sql("products", conn, if_exists="append", index=False)
            print(f"Inserted {len(df0)} rows into 'products' table (if columns aligned).")
        except Exception as e:
            print("Error writing spreadsheet_0 to 'products' table:", e)
    else:
        print("spreadsheet_0.csv not found, skipping.")

    # === Part 2: Merge spreadsheet_1 and spreadsheet_2, insert into 'shipments' ===
    if not os.path.exists(CSV1) or not os.path.exists(CSV2):
        print("spreadsheet_1.csv or spreadsheet_2.csv missing. skipping shipments insert.")
        conn.close()
        return

    df1 = pd.read_csv(CSV1)  # example cols: shipping_identifier, product_name, quantity, ...
    df2 = pd.read_csv(CSV2)  # example cols: shipping_identifier, origin, destination, shipment_date, ...

    print("CSV1 cols:", df1.columns.tolist())
    print("CSV2 cols:", df2.columns.tolist())

    # Normalize column names to lower-case for safety
    df1.columns = [c.strip() for c in df1.columns]
    df2.columns = [c.strip() for c in df2.columns]

    merged = pd.merge(df1, df2, on="shipping_identifier", how="left", validate="m:1")
    print(f"Merged rows: {len(merged)}")

    # Validate shipments table exists
    if not table_exists(conn, "shipments"):
        # create a simple shipments table if missing (adjust schema as needed)
        cur.execute("""
        CREATE TABLE shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shipping_identifier TEXT,
            product_name TEXT,
            quantity INTEGER,
            origin TEXT,
            destination TEXT,
            shipment_date TEXT
        )
        """)
        conn.commit()
        print("Created 'shipments' table (auto).")

    # Prepare insert statement
    insert_sql = """
    INSERT INTO shipments (shipping_identifier, product_name, quantity, origin, destination, shipment_date)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    with conn:
        for _, row in merged.iterrows():
            sid = row.get("shipping_identifier")
            pname = row.get("product_name") or row.get("product")
            qty = int(row.get("quantity") or 1)
            origin = row.get("origin")
            destination = row.get("destination")
            shipment_date = row.get("shipment_date") if "shipment_date" in row.index else None

            cur.execute(insert_sql, (sid, pname, qty, origin, destination, shipment_date))
            inserted += 1

    print(f"Inserted {inserted} shipment rows into 'shipments' table.")
    conn.close()

if __name__ == "__main__":
    main()
