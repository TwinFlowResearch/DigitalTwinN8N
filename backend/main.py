# =====================================================================
# Digital Twin Tools – FastAPI-Backend
#
# Stellt die Datenbank- und Domänenlogik des kognitiven Digitalen
# Zwillings als HTTP-Endpunkte bereit. n8n ruft diese Endpunkte als
# Werkzeuge auf, hat selbst aber keinen direkten Datenbankzugriff.
# =====================================================================

import os
import json
import random
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

app = FastAPI(title="Digital Twin Tools")

# CORS für das Frontend (Prototyp: alles erlauben)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================================
# DISRUPTION-KONFIGURATION
# Für jeden Störungstyp: betroffenes Event, zu suchende Ressource,
# erlaubte Aktionen und eine menschenlesbare Beschreibung.
# Neuen Typ hinzufügen: nur hier eintragen, kein n8n-Node nötig.
# =====================================================================
DISRUPTION_CONFIG = {
    "truck_breakdown": {
        "affected_event_type": "drive to terminal",
        "search_object_type":  "Truck",
        "cascade_to_event":    "depart",
        "action_catalog": [
            "book_replacement_truck",
            "reschedule_container",
            "drive_to_terminal"
        ],
        "description": "LKW ausgefallen — Ersatz-LKW suchen oder Schiff umbuchen"
    },
    "staff_absent": {
        "affected_event_type": "create transport document",
        "search_object_type":  None,
        "cascade_to_event":    "depart",
        "action_catalog": [
            "expedite_document_creation",
            "reassign_to_available_staff",
            "delay_booking"
        ],
        "description": "Mitarbeiter fehlt — Dokument verzögert, Buchungsfenster prüfen"
    },
    "no_vehicle": {
        "affected_event_type": "book vehicles",
        "search_object_type":  "Vehicle",
        "cascade_to_event":    "depart",
        "action_catalog": [
            "book_alternative_vehicle",
            "reschedule_shipment",
            "wait_for_next_vehicle"
        ],
        "description": "Kein Fahrzeug verfügbar — nächstes Schiff suchen"
    },
    "forklift_breakdown": {
        "affected_event_type": "bring to loading bay",
        "search_object_type":  "Forklift",
        "cascade_to_event":    "depart",
        "action_catalog": [
            "book_replacement_forklift",
            "manual_loading",
            "delay_loading"
        ],
        "description": "Stapler ausgefallen — Ersatz oder manuelle Verladung"
    },
    "container_damage": {
        "affected_event_type": "pick up empty container",
        "search_object_type":  "Container",
        "cascade_to_event":    "depart",
        "action_catalog": [
            "replace_container",
            "repair_and_delay",
            "use_alternative_container"
        ],
        "description": "Container beschädigt — Ersatz-Container suchen"
    },
    "overweight": {
        "affected_event_type": "weigh",
        "search_object_type":  None,
        "cascade_to_event":    "depart",
        "action_catalog": [
            "redistribute_cargo",
            "remove_excess_items",
            "request_overweight_permit"
        ],
        "description": "Container zu schwer — Ladung umverteilen oder Sondergenehmigung"
    },
    "weather_delay": {
        "affected_event_type": "depart",
        "search_object_type":  "Vehicle",
        "cascade_to_event":    None,
        "action_catalog": [
            "delay_departure",
            "find_alternative_route",
            "reschedule_to_next_weather_window"
        ],
        "description": "Wetter blockiert Abfahrt — nächstes Wetterfenster suchen"
    }
}


# =====================================================================
# DATENBANK-VERBINDUNGSPOOL
# Max. 5 gleichzeitige Verbindungen.
# Verhindert "too many connections" bei 100 automatischen Tests.
# =====================================================================
db_pool = psycopg2.pool.SimpleConnectionPool(
    1, 5,
    host=os.getenv("DB_HOST", "postgres"),
    dbname=os.getenv("DB_NAME", "logistics"),
    user=os.getenv("DB_USER", "dtuser"),
    password=os.getenv("DB_PASS", "dtpass123"),
    cursor_factory=RealDictCursor,
)

@contextmanager
def get_db():
    """
    Leiht eine Verbindung aus dem Pool aus und gibt sie nach Gebrauch
    automatisch wieder zurück, auch wenn innerhalb des with-Blocks
    eine Exception auftritt.
    """
    con = db_pool.getconn()
    try:
        yield con
    finally:
        db_pool.putconn(con)


# =====================================================================
# HILFSFUNKTION: 4-Stufen-OCEL-Graphtraversierung
#
# Rekonstruiert den vollständigen Ereignisablauf einer Customer Order,
# indem dem Objektpfad Customer Order -> Transport Document -> Container
# -> Vehicle gefolgt wird. Eine direkte Suche nach Events der Customer
# Order allein liefert nur wenige Treffer, weil physische Aktivitäten
# (z. B. Drive to Terminal) ausschließlich mit Container und Truck
# verknüpft sind, nicht mit der Order selbst (vgl. Kapitel 6.2).
#
# Details der Implementierung:
#   - DISTINCT ON (ocel_type) bei Container-Events liefert pro
#     Event-Typ nur eine Zeile, auch wenn mehrere Handling Units
#     denselben Container betreffen
#   - Das Zeitfenster grenzt die Ausgabe auf den relevanten Prozess
#     ein und schließt Events anderer, paralleler Aufträge aus
#   - LIMIT 1 auf Container und Vehicle wählt jeweils das erste
#     Objekt entlang des Pfads (Prototyp-Vereinfachung, vgl. Thesis)
# =====================================================================
def get_order_events(cur, order_id: str) -> list:
    sql = """
    WITH

    -- Stufe 1: Transport Document dieser Bestellung
    tds AS (
        SELECT DISTINCT eo.ocel_object_id AS td_id
        FROM event_object eo
        JOIN object o ON o.ocel_id = eo.ocel_object_id
        WHERE o.ocel_type = 'Transport Document'
          AND eo.ocel_event_id IN (
              SELECT ocel_event_id
              FROM event_object
              WHERE ocel_object_id = %(order_id)s
          )
    ),

    -- Zeitfenster: von erstem CO/TD-Event bis 12h nach letztem
    time_window AS (
        SELECT
            MIN(e.ocel_time) - interval '2 hours'  AS start_time,
            MAX(e.ocel_time) + interval '12 hours' AS end_time
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id IN (
            SELECT %(order_id)s
            UNION
            SELECT td_id FROM tds
        )
    ),

    -- Stufe 2: Genau EINEN Container dieser Bestellung
    -- LIMIT 1 wählt einen Container pro Transport Document aus.
    -- Prototyp-Vereinfachung: eine vollständige Implementierung
    -- würde alle Container einer Bestellung parallel verarbeiten.
    containers AS (
        SELECT DISTINCT eo2.ocel_object_id AS cr_id
        FROM event e
        JOIN event_object eo1 ON eo1.ocel_event_id = e.ocel_id
        JOIN event_object eo2 ON eo2.ocel_event_id = e.ocel_id
        JOIN object o ON o.ocel_id = eo2.ocel_object_id
        WHERE eo1.ocel_object_id IN (SELECT td_id FROM tds)
          AND o.ocel_type = 'Container'
        LIMIT 1
    ),

    -- Stufe 3: Fahrzeug (Schiff) das unseren Container geladen hat
    -- LIMIT 1: nur das erste Fahrzeug, das diesen Container lädt
    vehicles AS (
        SELECT DISTINCT eo2.ocel_object_id AS vh_id
        FROM event e
        JOIN event_object eo1 ON eo1.ocel_event_id = e.ocel_id
        JOIN event_object eo2 ON eo2.ocel_event_id = e.ocel_id
        JOIN object o ON o.ocel_id = eo2.ocel_object_id
        WHERE eo1.ocel_object_id IN (SELECT cr_id FROM containers)
          AND o.ocel_type = 'Vehicle'
          AND LOWER(e.ocel_type) LIKE '%%load%%vehicle%%'
        LIMIT 1
    ),

    -- Stufe 4: Alle relevanten Events sammeln
    -- CO-Events: Register Customer Order
    co_events AS (
        SELECT e.ocel_id, e.ocel_type, e.ocel_time, 'CO' AS layer
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id = %(order_id)s
    ),

    -- TD-Events: Create Transport Document, Book Vehicles, ...
    td_events AS (
        SELECT DISTINCT e.ocel_id, e.ocel_type, e.ocel_time, 'TD' AS layer
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id IN (SELECT td_id FROM tds)
    ),

    -- Container-Events: DISTINCT ON verhindert doppelte Load Truck-Einträge.
    -- Pro Event-Typ wird nur der früheste Eintrag behalten.
    container_events AS (
        SELECT DISTINCT ON (e.ocel_type)
            e.ocel_id, e.ocel_type, e.ocel_time, 'Container' AS layer
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id IN (SELECT cr_id FROM containers)
        ORDER BY e.ocel_type, e.ocel_time
    ),

    -- Schiff-Abfahrt: nur das Depart-Event des richtigen Schiffs
    vehicle_events AS (
        SELECT DISTINCT e.ocel_id, e.ocel_type, e.ocel_time, 'Vehicle' AS layer
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        WHERE eo.ocel_object_id IN (SELECT vh_id FROM vehicles)
          AND LOWER(e.ocel_type) LIKE '%%depart%%'
    ),

    -- Alles zusammenführen
    all_relevant AS (
        SELECT * FROM co_events
        UNION
        SELECT * FROM td_events
        UNION
        SELECT * FROM container_events
        UNION
        SELECT * FROM vehicle_events
    )

    -- Finale Ausgabe: mit Zeitfenster filtern und Objekte anreichern
    SELECT
        a.ocel_id,
        a.ocel_type,
        a.ocel_time,
        to_char(a.ocel_time, 'YYYY-MM-DD HH24:MI:SS') AS ts,
        a.layer,
        STRING_AGG(DISTINCT o.ocel_type || ':' || o.ocel_id, ', ')
            AS objects,
        STRING_AGG(DISTINCT o.ocel_id, ', ')
            AS object_ids,
        STRING_AGG(DISTINCT o.ocel_type, ', ')
            AS object_types
    FROM all_relevant a
    JOIN event_object eo ON eo.ocel_event_id = a.ocel_id
    JOIN object o        ON o.ocel_id = eo.ocel_object_id
    CROSS JOIN time_window tw
    WHERE a.ocel_time BETWEEN tw.start_time AND tw.end_time
    GROUP BY a.ocel_id, a.ocel_type, a.ocel_time, a.layer
    ORDER BY a.ocel_time
    """
    cur.execute(sql, {"order_id": order_id})
    return cur.fetchall()


# =====================================================================
# GESUNDHEITSCHECK
# =====================================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# =====================================================================
# TOOL 1: Knowledge-Base-Pre-Check
# Sucht ob dieselbe Störung schon einmal erfolgreich gelöst wurde.
# Treffer = Fast Path, LLM wird übersprungen → Token gespart.
# =====================================================================
@app.get("/tool/kb_check")
def kb_check(disruption_type: str, object_type: str = "Truck"):
    sql = """
        SELECT shadow_id, event_type, action_parameters,
               confidence_score, chain_of_thought, disruption_type
        FROM shadow_events
        WHERE disruption_type = %s
          AND outcome_success = TRUE
          AND confidence_score >= 0.85
        ORDER BY created_at DESC
        LIMIT 1
    """
    with get_db() as con:
        cur = con.cursor()
        cur.execute(sql, (disruption_type,))
        row = cur.fetchone()
        cur.close()
    return {"cache_hit": bool(row), "solution": row}


# =====================================================================
# TOOL 2: Plan einer Bestellung abrufen
# Nutzt die 4-Stufen-Traversierung für einen sauberen, vollständigen
# Fahrplan ohne Duplikate und ohne Event-Flut.
# =====================================================================
@app.get("/tool/get_trace")
def get_trace(order_id: str):
    with get_db() as con:
        cur = con.cursor()
        rows = get_order_events(cur, order_id)
        cur.close()

    if not rows:
        return {
            "order_id": order_id,
            "trace": [],
            "warning": (
                f"Keine Events für '{order_id}' gefunden. "
                "Echte IDs prüfen mit: "
                "SELECT DISTINCT ocel_id FROM object "
                "WHERE ocel_type='Customer Order' LIMIT 10;"
            )
        }

    return {"order_id": order_id, "trace": rows}


# =====================================================================
# TOOL 3: Freie Ressourcen suchen (Ersatz-LKW, Stapler, Container...)
# object_type kommt dynamisch aus der Disruption-Konfiguration.
# =====================================================================
@app.get("/tool/search_alternatives")
def search_alternatives(object_type: str, target_time: str):
    if not object_type or object_type.lower() == "null":
        return {"object_type": object_type, "available": []}

    sql = """
        SELECT o.ocel_id, o.ocel_type
        FROM object o
        WHERE o.ocel_type = %s
          AND o.ocel_id NOT IN (
              SELECT eo.ocel_object_id
              FROM event_object eo
              JOIN event e ON e.ocel_id = eo.ocel_event_id
              WHERE e.ocel_time
                  BETWEEN %s::timestamp - interval '2 hours'
                      AND %s::timestamp + interval '2 hours'
          )
        LIMIT 10
    """
    with get_db() as con:
        cur = con.cursor()
        cur.execute(sql, (object_type, target_time, target_time))
        rows = cur.fetchall()
        cur.close()
    return {"object_type": object_type, "available": rows}


# =====================================================================
# TOOL 4: Nächstes verfügbares Fahrzeug / Schiff
# =====================================================================
@app.get("/tool/search_next_vehicle")
def search_next_vehicle(after_timestamp: str, container_id: str = ""):
    sql = """
        SELECT
            e.ocel_id AS depart_event_id,
            to_char(e.ocel_time, 'YYYY-MM-DD HH24:MI:SS') AS depart_time,
            o.ocel_id AS vehicle_id
        FROM event e
        JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
        JOIN object o        ON o.ocel_id = eo.ocel_object_id
        WHERE LOWER(e.ocel_type) = 'depart'
          AND o.ocel_type = 'Vehicle'
          AND e.ocel_time > %s::timestamp
        ORDER BY e.ocel_time ASC
        LIMIT 5
    """
    with get_db() as con:
        cur = con.cursor()
        cur.execute(sql, (after_timestamp,))
        rows = cur.fetchall()
        cur.close()
    if not rows:
        return {"next_vehicles": [], "recommendation": None}
    return {"next_vehicles": rows, "recommendation": rows[0]}


# =====================================================================
# TOOL 5: Automatische Szenario-Generierung
#
# Der Nutzer tippt nur die Order-ID — alles andere kommt aus der DB:
#   - Störungstyp: zufällig, aber passend zu vorhandenen Events
#   - Objekt-ID: echte ID aus dem betroffenen Event
#   - Deadline: echte Schiffsabfahrtszeit aus dem Fahrplan
#   - Verzögerung: realistisch berechnet (2-8h, Dataset-Grenze beachtet)
#   - Wahrscheinlichkeit: nur 33% der Aufrufe erzeugen überhaupt
#     eine Störung — der Rest ist Normalbetrieb
# =====================================================================
@app.get("/tool/auto_generate_scenario")
def auto_generate_scenario(order_id: str, disruption_type: str = None):

    # --- Wahrscheinlichkeits-Check ---
    # Mit 66% Chance läuft der Prozess normal durch (kein Eingriff nötig).
    # Mit 33% Chance tritt eine Störung auf.
    DISRUPTION_PROBABILITY = 0.33

    if not disruption_type and random.random() > DISRUPTION_PROBABILITY:
        with get_db() as con:
            cur = con.cursor()
            all_events = get_order_events(cur, order_id)
            cur.close()
        return {
            "order_id":       order_id,
            "has_disruption": False,
            "disruption":     None,
            "full_trace":     all_events,
            "disruption_description": "Kein Störungsfall — Prozess läuft normal.",
            "action_catalog": [],
            "search_object_type": None
        }

    with get_db() as con:
        cur = con.cursor()

        # Vollständigen Fahrplan über korrekten Graphpfad holen
        all_events = get_order_events(cur, order_id)

        if not all_events:
            raise HTTPException(
                404,
                f"Keine Events für '{order_id}' gefunden. "
                "Echte IDs: SELECT DISTINCT ocel_id FROM object "
                "WHERE ocel_type='Customer Order' LIMIT 10;"
            )

        # Störungstyp wählen
        # Nur Typen die wirklich ein passendes Event im Fahrplan haben
        if disruption_type and disruption_type in DISRUPTION_CONFIG:
            chosen_type = disruption_type
        else:
            matching_types = []
            for dtype, cfg in DISRUPTION_CONFIG.items():
                affected = cfg["affected_event_type"].lower()
                for ev in all_events:
                    if affected in ev["ocel_type"].lower():
                        matching_types.append(dtype)
                        break
            chosen_type = (
                random.choice(matching_types)
                if matching_types else "truck_breakdown"
            )

        config = DISRUPTION_CONFIG[chosen_type]
        affected_type = config["affected_event_type"].lower()

        # Betroffenes Event im Fahrplan finden
        affected_event = None
        for ev in all_events:
            if affected_type in ev["ocel_type"].lower():
                affected_event = ev
                break

        # Echtes Objekt aus diesem Event lesen
        # Beispiel: truck_breakdown → welcher Truck war wirklich dabei?
        real_object_id = None
        search_type = config["search_object_type"]

        if affected_event and search_type:
            cur.execute("""
                SELECT o.ocel_id, o.ocel_type
                FROM object o
                JOIN event_object eo ON eo.ocel_object_id = o.ocel_id
                WHERE eo.ocel_event_id = %s
                  AND o.ocel_type = %s
                LIMIT 1
            """, (affected_event["ocel_id"], search_type))
            obj = cur.fetchone()
            if obj:
                real_object_id = obj["ocel_id"]

        # Deadline: erstes Depart-Event im Fahrplan
        # (das ist das Zielschiff, nicht irgendein zukünftiges Schiff)
        deadline = None
        for ev in all_events:
            if "depart" in ev["ocel_type"].lower():
                deadline = ev["ts"]
                break

        # Realistische Verzögerung berechnen
        # Ziel: kurz genug um lösbar zu bleiben (max. 8h),
        # lang genug um das nächste Schiff zu verpassen.
        # Dataset-Grenze: letztes Depart-Event als absolutes Maximum.
        delay_hours = 4  # sicherer Fallback

        if affected_event and deadline:
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (
                    %s::timestamp - %s::timestamp
                )) / 3600 AS diff_hours
            """, (deadline, affected_event["ts"]))
            diff = cur.fetchone()
            hours_to_ship = (
                diff["diff_hours"] if diff and diff["diff_hours"] else 8
            )

            # Letztes Depart im gesamten Dataset als harte Grenze
            cur.execute("""
                SELECT to_char(MAX(e.ocel_time), 'YYYY-MM-DD HH24:MI:SS')
                       AS last_depart
                FROM event e
                JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
                JOIN object o        ON o.ocel_id = eo.ocel_object_id
                WHERE LOWER(e.ocel_type) LIKE '%%depart%%'
                  AND o.ocel_type = 'Vehicle'
            """)
            last_row = cur.fetchone()
            last_depart_str = last_row["last_depart"] if last_row else None

            if last_depart_str:
                cur.execute("""
                    SELECT EXTRACT(EPOCH FROM (
                        %s::timestamp - %s::timestamp
                    )) / 3600 AS hours_to_last
                """, (last_depart_str, affected_event["ts"]))
                last_diff = cur.fetchone()
                hours_to_last = (
                    last_diff["hours_to_last"]
                    if last_diff and last_diff["hours_to_last"] else 48
                )
            else:
                hours_to_last = 48

            # Verzögerung: zufällig zwischen 2 und 8 Stunden,
            # niemals mehr als 80% der Zeit zum letzten Dataset-Event.
            max_allowed = min(8, int(float(hours_to_last) * 0.8))
            delay_hours = random.randint(2, max(2, max_allowed))

        # Container-ID der Bestellung ermitteln
        # (Pfad: Customer Order -> Transport Document -> Container)
        cur.execute("""
            WITH tds AS (
                SELECT DISTINCT eo.ocel_object_id AS td_id
                FROM event_object eo
                JOIN object o ON o.ocel_id = eo.ocel_object_id
                WHERE o.ocel_type = 'Transport Document'
                  AND eo.ocel_event_id IN (
                      SELECT ocel_event_id
                      FROM event_object
                      WHERE ocel_object_id = %s
                  )
            )
            SELECT DISTINCT eo2.ocel_object_id AS container_id
            FROM event_object eo1
            JOIN event_object eo2 ON eo2.ocel_event_id = eo1.ocel_event_id
            JOIN object o ON o.ocel_id = eo2.ocel_object_id
            WHERE eo1.ocel_object_id IN (SELECT td_id FROM tds)
              AND o.ocel_type = 'Container'
            LIMIT 1
        """, (order_id,))
        container = cur.fetchone()
        cur.close()

    return {
        "order_id":               order_id,
        "has_disruption":         True,
        "disruption_type":        chosen_type,
        "disruption_description": config["description"],
        "disruption": {
            "type":               chosen_type,
            "object_id":          real_object_id,
            "delay_hours":        delay_hours,
            "event_id":           affected_event["ocel_id"] if affected_event else None,
            "event_time":         affected_event["ts"]       if affected_event else None,
            "container_id":       container["container_id"]  if container      else None,
            "expected_miss_time": deadline
        },
        "action_catalog":         config["action_catalog"],
        "search_object_type":     search_type,
        "full_trace":             all_events
    }


# =====================================================================
# WoT THING DESCRIPTIONS (Stufe 2)
#
# Generiert W3C-WoT-TD-1.1-konforme Objektbeschreibungen automatisch
# aus DISRUPTION_CONFIG. DISRUPTION_CONFIG bleibt die Single Source
# of Truth — diese Funktion übersetzt sie nur ins TD-Format.
#
# Jede TD enthält:
#   - properties: aktueller Zustand des Objekts (aus echter DB)
#   - actions:     erlaubte Aktionen für diesen Objekttyp,
#                   abgeleitet aus DISRUPTION_CONFIG.action_catalog
#   - events:      mögliche Störungsereignisse für diesen Objekttyp
# =====================================================================

# Welche Properties hat welcher Objekttyp (Schema-Definition)
TD_PROPERTY_SCHEMA = {
    "Truck": {
        "status":   {"type": "string", "enum": ["available", "in_use", "broken_down"]},
        "location": {"type": "string", "description": "Letztes bekanntes Event"},
    },
    "Vehicle": {
        "status":      {"type": "string", "enum": ["scheduled", "departed", "delayed"]},
        "depart_time": {"type": "string", "description": "Geplante Abfahrtszeit (YYYY-MM-DD HH:MM:SS)"},
    },
    "Container": {
        "status": {"type": "string", "enum": ["loaded", "empty", "in_transit", "damaged"]},
        "weight": {"type": "string", "description": "Gewichtsstatus"},
    },
    "Forklift": {
        "status": {"type": "string", "enum": ["available", "in_use", "broken_down"]},
    },
}

# Welche Aktion erwartet welche Input-Parameter (für die TD-actions)
TD_ACTION_SCHEMAS = {
    "book_replacement_truck":            {"truck_id": "string"},
    "reschedule_container":              {"vehicle_id": "string", "reason": "string"},
    "drive_to_terminal":                 {"truck_id": "string", "event_timestamp": "string"},
    "expedite_document_creation":        {"event_timestamp": "string"},
    "reassign_to_available_staff":       {"staff_id": "string"},
    "delay_booking":                     {"event_timestamp": "string"},
    "book_alternative_vehicle":          {"vehicle_id": "string"},
    "reschedule_shipment":               {"vehicle_id": "string"},
    "wait_for_next_vehicle":             {"vehicle_id": "string"},
    "book_replacement_forklift":         {"forklift_id": "string"},
    "manual_loading":                    {"event_timestamp": "string"},
    "delay_loading":                     {"event_timestamp": "string"},
    "replace_container":                 {"container_id": "string"},
    "repair_and_delay":                  {"event_timestamp": "string"},
    "use_alternative_container":         {"container_id": "string"},
    "redistribute_cargo":                {"container_id": "string"},
    "remove_excess_items":               {"container_id": "string"},
    "request_overweight_permit":         {"container_id": "string"},
    "delay_departure":                   {"vehicle_id": "string", "event_timestamp": "string"},
    "find_alternative_route":            {"vehicle_id": "string"},
    "reschedule_to_next_weather_window": {"vehicle_id": "string"},
}


def _actions_for_object_type(object_type: str) -> dict:
    """
    Sammelt alle Aktionen aus DISRUPTION_CONFIG, deren search_object_type
    zu diesem Objekttyp passt, und baut daraus TD-actions.
    """
    actions = {}
    for dtype, cfg in DISRUPTION_CONFIG.items():
        if cfg["search_object_type"] != object_type:
            continue
        for action_name in cfg["action_catalog"]:
            if action_name in actions:
                continue
            input_schema = TD_ACTION_SCHEMAS.get(action_name, {})
            actions[action_name] = {
                "title": action_name.replace("_", " ").title(),
                "description": f"Verfügbar bei Störungstyp '{dtype}': {cfg['description']}",
                "input": {
                    "type": "object",
                    "properties": {
                        k: {"type": v} for k, v in input_schema.items()
                    }
                },
                "forms": [{
                    "href": "/tool/write_cascade",
                    "contentType": "application/json",
                    "op": "invokeaction"
                }]
            }
    return actions


def _events_for_object_type(object_type: str) -> dict:
    """Mögliche Störungsereignisse für diesen Objekttyp aus DISRUPTION_CONFIG."""
    events = {}
    for dtype, cfg in DISRUPTION_CONFIG.items():
        if cfg["search_object_type"] != object_type:
            continue
        events[dtype] = {
            "title": dtype.replace("_", " ").title(),
            "description": cfg["description"],
            "data": {"type": "object", "properties": {
                "delay_hours": {"type": "integer"},
                "event_id": {"type": "string"}
            }}
        }
    return events


def build_thing_description(cur, object_id: str, object_type: str) -> dict:
    """
    Baut eine W3C-WoT-TD-1.1-konforme Beschreibung für EIN Objekt.
    properties.* werden mit echten Werten aus der DB gefüllt.
    actions/events werden aus DISRUPTION_CONFIG generiert (siehe oben).
    """
    schema = TD_PROPERTY_SCHEMA.get(object_type, {})
    properties = {}

    if object_type in ("Truck", "Forklift"):
        # Ist das Objekt aktuell in einem Event verwendet?
        cur.execute("""
            SELECT COUNT(*) AS c FROM event_object eo
            JOIN event e ON e.ocel_id = eo.ocel_event_id
            WHERE eo.ocel_object_id = %s
              AND e.ocel_time > NOW() - interval '1000 days'
        """, (object_id,))
        used = cur.fetchone()["c"] > 0
        properties["status"] = {
            **schema.get("status", {}),
            "value": "in_use" if used else "available"
        }
        cur.execute("""
            SELECT e.ocel_type, to_char(e.ocel_time,'YYYY-MM-DD HH24:MI:SS') AS ts
            FROM event e JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
            WHERE eo.ocel_object_id = %s ORDER BY e.ocel_time DESC LIMIT 1
        """, (object_id,))
        last = cur.fetchone()
        properties["location"] = {
            **schema.get("location", {}),
            "value": f"{last['ocel_type']} @ {last['ts']}" if last else "unknown"
        }

    elif object_type == "Vehicle":
        cur.execute("""
            SELECT to_char(e.ocel_time,'YYYY-MM-DD HH24:MI:SS') AS ts
            FROM event e JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
            WHERE eo.ocel_object_id = %s AND e.ocel_type = 'depart'
            ORDER BY e.ocel_time ASC LIMIT 1
        """, (object_id,))
        dep = cur.fetchone()
        properties["depart_time"] = {
            **schema.get("depart_time", {}),
            "value": dep["ts"] if dep else "unknown"
        }
        properties["status"] = {
            **schema.get("status", {}),
            "value": "scheduled" if dep else "unknown"
        }

    elif object_type == "Container":
        cur.execute("""
            SELECT e.ocel_type FROM event e
            JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
            WHERE eo.ocel_object_id = %s
            ORDER BY e.ocel_time DESC LIMIT 1
        """, (object_id,))
        last = cur.fetchone()
        properties["status"] = {
            **schema.get("status", {}),
            "value": last["ocel_type"] if last else "unknown"
        }

    return {
        "@context": "https://www.w3.org/2022/wot/td/v1.1",
        "id": f"urn:dt:{object_type.lower()}:{object_id}",
        "title": f"{object_type} {object_id}",
        "@type": object_type,
        "properties": properties,
        "actions": _actions_for_object_type(object_type),
        "events": _events_for_object_type(object_type),
    }


@app.get("/tool/get_thing_descriptions")
def get_thing_descriptions(order_id: str, disruption_object_id: str = None,
                            disruption_object_type: str = None,
                            search_object_type: str = None,
                            available_ids: str = ""):
    """
    Liefert WoT Thing Descriptions für alle für diese Störung relevanten
    Objekte:
      1. Das betroffene Objekt (z.B. tr2 bei truck_breakdown)
      2. Alle verfügbaren Alternativen (z.B. tr5, tr8 aus search_alternatives)

    available_ids: kommagetrennte Liste, z.B. "tr5,tr8"
    """
    tds = []
    with get_db() as con:
        cur = con.cursor()

        if disruption_object_id and disruption_object_type:
            tds.append(build_thing_description(
                cur, disruption_object_id, disruption_object_type))

        if available_ids and search_object_type:
            for oid in available_ids.split(","):
                oid = oid.strip()
                if oid:
                    tds.append(build_thing_description(
                        cur, oid, search_object_type))

        cur.close()

    return {"order_id": order_id, "thing_descriptions": tds}


# =====================================================================
# TOOL 6: Kaskade in Shadow Log schreiben
# Schreibt mehrere Events auf einmal (für Kaskadeneffekte).
# =====================================================================
class CascadeEvent(BaseModel):
    """Eine einzelne geplante oder überschriebene Aktion innerhalb einer Kaskade."""
    shadow_id: str
    event_type: str
    event_timestamp: str
    overrides_event_id: str | None = None  # gesetzt, wenn dieses Event ein Baseline-Event ersetzt
    action_parameters: dict | None = None
    is_new_event: bool = False  # True, wenn kein Baseline-Pendant existiert (z. B. Umbuchung)

class CascadePayload(BaseModel):
    """
    Vollständige Anfrage des Agenten-Workflows an /tool/write_cascade:
    Metadaten zum Testlauf (Evaluationsstufe, Modell, Kosten,
    Compliance-Ergebnis) plus die Liste der zu schreibenden Events.
    """
    order_id: str
    trigger_event_id: str | None = None
    confidence_score: float
    chain_of_thought: str
    test_id: str | None = None
    experiment_stage: int | None = None
    disruption_type: str | None = None
    llm_model: str | None = None
    tokens_prompt: int = 0
    tokens_completion: int = 0
    api_cost_usd: float = 0.0
    latency_ms: int = 0
    compliance_approved: bool = True
    compliance_rejection_count: int = 0
    hitl_required: bool = False
    hitl_approved_by: str | None = None
    events: list[CascadeEvent]

@app.post("/tool/write_cascade")
def write_cascade(payload: CascadePayload):
    sql = """
        INSERT INTO shadow_events (
            shadow_id, event_type, event_timestamp, order_id,
            trigger_event_id, overrides_event_id, is_new_event,
            action_parameters, confidence_score, chain_of_thought,
            test_id, experiment_stage, disruption_type, llm_model,
            tokens_prompt, tokens_completion, api_cost_usd, latency_ms,
            compliance_approved, compliance_rejection_count,
            hitl_required, hitl_approved_by
        ) VALUES (
            %(shadow_id)s, %(event_type)s, %(event_timestamp)s, %(order_id)s,
            %(trigger_event_id)s, %(overrides_event_id)s, %(is_new_event)s,
            %(action_parameters)s, %(confidence_score)s, %(chain_of_thought)s,
            %(test_id)s, %(experiment_stage)s, %(disruption_type)s, %(llm_model)s,
            %(tokens_prompt)s, %(tokens_completion)s, %(api_cost_usd)s,
            %(latency_ms)s, %(compliance_approved)s,
            %(compliance_rejection_count)s, %(hitl_required)s,
            %(hitl_approved_by)s
        ) ON CONFLICT (shadow_id) DO NOTHING
    """
    # Felder, die für alle Events der Kaskade identisch sind
    # (z. B. confidence_score, chain_of_thought, Modellinfos)
    shared = payload.model_dump(exclude={"events"})
    written = []
    with get_db() as con:
        cur = con.cursor()
        try:
            # Jedes Event der Kaskade einzeln einfügen; die gemeinsamen
            # Metadaten aus shared werden dabei für jede Zeile wiederverwendet.
            for ev in payload.events:
                row = {**shared, **ev.model_dump()}
                row["action_parameters"] = json.dumps(row["action_parameters"])
                cur.execute(sql, row)
                written.append(ev.shadow_id)
            con.commit()
        except Exception as e:
            con.rollback()
            raise HTTPException(500, str(e))
        finally:
            cur.close()
    return {"status": "written", "count": len(written), "shadow_ids": written}


# =====================================================================
# TOOL 7: Outcome manuell setzen
# Erlaubt das nachträgliche, manuelle Markieren eines Laufs als
# erfolgreich oder gescheitert, z. B. für Fälle außerhalb der
# automatisierten Evaluation oder zur Korrektur von Einzelfällen.
# =====================================================================
class OutcomeUpdate(BaseModel):
    order_id: str
    outcome_success: bool
    outcome_note: str | None = None

@app.post("/tool/update_outcome")
def update_outcome(p: OutcomeUpdate):
    """Setzt das Ergebnis eines Laufs manuell, statt es automatisch zu berechnen."""
    sql = """
        UPDATE shadow_events
        SET outcome_success      = %(outcome_success)s,
            outcome_note         = %(outcome_note)s,
            outcome_evaluated_at = NOW()
        WHERE order_id = %(order_id)s
    """
    with get_db() as con:
        cur = con.cursor()
        try:
            cur.execute(sql, p.model_dump())
            affected = cur.rowcount
            con.commit()
        except Exception as e:
            con.rollback()
            raise HTTPException(500, str(e))
        finally:
            cur.close()
    return {"status": "updated", "rows": affected}


# =====================================================================
# TOOL 8: Outcome automatisch bestimmen
# Vergleicht letzten KI-Zeitstempel mit Schiffsabfahrt.
# KI-Zeitstempel vor Abfahrt = Erfolg.
# Für die 100 automatischen Tests — kein manuelles Nachtragen.
# =====================================================================
@app.post("/tool/auto_evaluate_outcome")
def auto_evaluate_outcome(order_id: str = ""):
    with get_db() as con:
        cur = con.cursor()

        # Holt den spätesten Zeitstempel der KI-Aktionen dieser Bestellung
        cur.execute("""
            SELECT MAX(event_timestamp) AS last_ai
            FROM shadow_events
            WHERE order_id = %s
        """, (order_id,))
        last_ai = cur.fetchone()["last_ai"]

        # Holt die echte Abfahrtszeit des Schiffs, das für DIESE Bestellung (order_id) geplant ist
        cur.execute("""
            WITH tds AS (
                SELECT DISTINCT eo.ocel_object_id AS td_id
                FROM event_object eo
                JOIN object o ON o.ocel_id = eo.ocel_object_id
                WHERE o.ocel_type = 'Transport Document'
                  AND eo.ocel_event_id IN (
                      SELECT ocel_event_id FROM event_object WHERE ocel_object_id = %s
                  )
            ),
            containers AS (
                SELECT DISTINCT eo2.ocel_object_id AS cr_id
                FROM event e
                JOIN event_object eo1 ON eo1.ocel_event_id = e.ocel_id
                JOIN event_object eo2 ON eo2.ocel_event_id = e.ocel_id
                JOIN object o ON o.ocel_id = eo2.ocel_object_id
                WHERE eo1.ocel_object_id IN (SELECT td_id FROM tds)
                  AND o.ocel_type = 'Container'
                LIMIT 1
            ),
            vehicles AS (
                SELECT DISTINCT eo2.ocel_object_id AS vh_id
                FROM event e
                JOIN event_object eo1 ON eo1.ocel_event_id = e.ocel_id
                JOIN event_object eo2 ON eo2.ocel_event_id = e.ocel_id
                JOIN object o ON o.ocel_id = eo2.ocel_object_id
                WHERE eo1.ocel_object_id IN (SELECT cr_id FROM containers)
                  AND o.ocel_type = 'Vehicle'
                  AND LOWER(e.ocel_type) LIKE '%%load%%vehicle%%'
                LIMIT 1
            )
            SELECT MIN(e.ocel_time) AS depart
            FROM event e
            JOIN event_object eo ON eo.ocel_event_id = e.ocel_id
            WHERE eo.ocel_object_id IN (SELECT vh_id FROM vehicles)
              AND LOWER(e.ocel_type) = 'depart'
        """, (order_id,))
        depart = cur.fetchone()["depart"]

        # Erfolgskriterium: Der letzte vom Agenten geplante Zeitstempel
        # muss vor oder genau zur tatsächlichen Schiffsabfahrt liegen.
        # Liegt er danach, hätte die Maßnahme das Schiff nicht mehr
        # erreicht und der Lauf gilt als gescheitert.
        success = (
            last_ai is not None
            and depart is not None
            and last_ai <= depart
        )
        note = f"auto: last_ai={last_ai}, depart={depart}"

        cur.execute("""
            UPDATE shadow_events
            SET outcome_success      = %s,
                outcome_note         = %s,
                outcome_evaluated_at = NOW()
            WHERE order_id = %s
        """, (success, note, order_id))
        con.commit()
        cur.close()

    return {"order_id": order_id, "outcome_success": success, "note": note}


# =====================================================================
# FRONTEND-ENDPOINTS
# =====================================================================

@app.get("/frontend/orders")
def list_orders(limit: int = 24):
    """
    Liste aller Customer Orders mit Status für das Container-Grid.
    Status-Logik:
      disrupted  -> es gibt shadow_events ohne outcome
      ai_solved  -> es gibt shadow_events mit outcome_success = TRUE
      ai_failed  -> es gibt shadow_events mit outcome_success = FALSE
      normal     -> keine shadow_events
    """
    with get_db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT o.ocel_id AS order_id,
                   COALESCE(s.status, 'normal') AS status,
                   COALESCE(s.cnt, 0) AS ai_actions
            FROM object o
            LEFT JOIN (
                SELECT order_id,
                       COUNT(*) AS cnt,
                       CASE
                         WHEN BOOL_OR(outcome_success IS NULL) THEN 'disrupted'
                         WHEN BOOL_AND(outcome_success) THEN 'ai_solved'
                         ELSE 'ai_failed'
                       END AS status
                FROM shadow_events
                GROUP BY order_id
            ) s ON s.order_id = o.ocel_id
            WHERE o.ocel_type = 'Customer Order'
            ORDER BY o.ocel_id
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
    return {"orders": rows}


@app.get("/frontend/order_detail")
def order_detail(order_id: str):
    """
    Vollständige Detail-Ansicht einer Bestellung:
    - baseline: Original-Fahrplan (Graphtraversierung)
    - shadow: alle KI-Aktionen aus shadow_events
    """
    with get_db() as con:
        cur = con.cursor()
        baseline = get_order_events(cur, order_id)
        cur.execute("""
            SELECT shadow_id, event_type,
                   to_char(event_timestamp,'YYYY-MM-DD HH24:MI:SS') AS ts,
                   overrides_event_id, is_new_event, action_parameters,
                   confidence_score, chain_of_thought, disruption_type,
                   compliance_rejection_count, hitl_required,
                   outcome_success,
                   to_char(created_at,'YYYY-MM-DD HH24:MI:SS') AS created
            FROM shadow_events
            WHERE order_id = %s
            ORDER BY created_at ASC, event_timestamp ASC
        """, (order_id,))
        shadow = cur.fetchall()
        cur.close()
    return {"order_id": order_id, "baseline": baseline, "shadow": shadow}


@app.get("/frontend/kb_stats")
def kb_stats():
    """
    Knowledge-Base-Statistik für das Lern-Panel:
    - pro Störungstyp: Anzahl Läufe, Erfolge, Ø Confidence
    - Fast-Path-Anteil über die Zeit
    """
    with get_db() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT disruption_type,
                   COUNT(*) AS runs,
                   SUM(CASE WHEN outcome_success THEN 1 ELSE 0 END) AS successes,
                   ROUND(AVG(confidence_score)::NUMERIC, 2) AS avg_confidence,
                   SUM(CASE WHEN chain_of_thought LIKE 'FAST PATH%%'
                       THEN 1 ELSE 0 END) AS cache_hits
            FROM shadow_events
            WHERE disruption_type IS NOT NULL
            GROUP BY disruption_type
            ORDER BY runs DESC
        """)
        by_type = cur.fetchall()
        cur.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN chain_of_thought LIKE 'FAST PATH%%'
                       THEN 1 ELSE 0 END) AS total_cache_hits
            FROM shadow_events
        """)
        totals = cur.fetchone()
        cur.close()
    return {"by_type": by_type, "totals": totals}