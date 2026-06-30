"""OCEL 2.0 SQLite -> Postgres via pm4py."""
import pm4py
import psycopg2
from psycopg2.extras import execute_values
import os

SQLITE_PATH = "logistics.sqlite"
PG_HOST = os.getenv("DB_HOST", "postgres")
PG_DBNAME = os.getenv("DB_NAME", "logistics")
PG_USER = os.getenv("DB_USER", "dtuser")
PG_PASS = os.getenv("DB_PASS", "dtpass123")

print("Lade OCEL 2.0 via pm4py...")
ocel = pm4py.read.read_ocel2_sqlite(SQLITE_PATH)

# >>> DEBUG: echte Spaltennamen zeigen (pm4py-Versionen unterscheiden sich!) <<<
print("Event columns: ", ocel.events.columns.tolist())
print("Object columns:", ocel.objects.columns.tolist())
print("Reln columns:  ", ocel.relations.columns.tolist())
print(f"Events: {len(ocel.events)}, Objects: {len(ocel.objects)}, Relations: {len(ocel.relations)}")

pg = psycopg2.connect(host=PG_HOST, dbname=PG_DBNAME, user=PG_USER, password=PG_PASS)
cur = pg.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS event (
    ocel_id TEXT PRIMARY KEY, ocel_type TEXT, ocel_time TIMESTAMP)""")
cur.execute("""CREATE TABLE IF NOT EXISTS object (
    ocel_id TEXT PRIMARY KEY, ocel_type TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS event_object (
    ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT,
    PRIMARY KEY (ocel_event_id, ocel_object_id))""")

# --- Events (Datetime-Fix: keine Zeitzone, festes Format) ---
ev = ocel.events[["ocel:eid", "ocel:activity", "ocel:timestamp"]].copy()
ev.columns = ["ocel_id", "ocel_type", "ocel_time"]
ev["ocel_time"] = ev["ocel_time"].dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
execute_values(cur,
    "INSERT INTO event (ocel_id, ocel_type, ocel_time) VALUES %s ON CONFLICT DO NOTHING",
    [tuple(r) for r in ev.itertuples(index=False)], page_size=1000)
print(f"{len(ev)} Events.")

# --- Objects ---
ob = ocel.objects[["ocel:oid", "ocel:type"]].copy()
ob.columns = ["ocel_id", "ocel_type"]
execute_values(cur,
    "INSERT INTO object (ocel_id, ocel_type) VALUES %s ON CONFLICT DO NOTHING",
    [tuple(r) for r in ob.itertuples(index=False)], page_size=1000)
print(f"{len(ob)} Objects.")

# --- Relations ---
rel = ocel.relations[["ocel:eid", "ocel:oid", "ocel:qualifier"]].copy()
rel.columns = ["ocel_event_id", "ocel_object_id", "ocel_qualifier"]
execute_values(cur,
    """INSERT INTO event_object (ocel_event_id, ocel_object_id, ocel_qualifier)
       VALUES %s ON CONFLICT DO NOTHING""",
    [tuple(r) for r in rel.itertuples(index=False)], page_size=1000)
print(f"{len(rel)} Relations.")

cur.execute("CREATE INDEX IF NOT EXISTS idx_eo_event ON event_object(ocel_event_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_eo_obj   ON event_object(ocel_object_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_ev_time  ON event(ocel_time)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_ev_type  ON event(ocel_type)")

pg.commit(); cur.close(); pg.close()
print("Fertig.")