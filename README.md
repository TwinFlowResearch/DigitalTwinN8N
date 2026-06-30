# Digitaler Zwilling mit n8n und LLM-Agenten — KI-gestützte Entscheidungsfindung in der Logistik

Implementierung eines Digitalen Zwillings mit n8n und einem KI-Agenten zur
automatischen Erkennung, Diagnose und Behebung von Prozessabweichungen in
einem Logistiksystem. Entstanden im Rahmen der Bachelorarbeit an der
Universität Ulm.

## Architektur

Das System besteht aus vier Docker-Containern in einem gemeinsamen Netzwerk:

| Container      | Technologie   | Port |
| -------------- | ------------- | ---- |
| Datenbank      | PostgreSQL 16 | 5433 |
| DB-Admin       | pgAdmin 4     | 5050 |
| Orchestrierung | n8n           | 5678 |
| Backend        | FastAPI       | 8000 |

Die Datenschicht enthält das OCEL-2.0-Baseline-Log und den Shadow-Log. Der
Tool-Layer (FastAPI) stellt alle Datenbankzugriffe als HTTP-Endpunkte bereit.
Die Orchestrierungsschicht (n8n) koordiniert zwei KI-Agenten: einen
Dispatcher-Agenten, der Handlungsempfehlungen ableitet, und einen
Compliance-Agenten, der diese vor der Ausführung prüft.

## Voraussetzungen

- Docker Desktop
- Ein LLM-API-Schlüssel
- Der OCEL 2.0 Logistics Simulation Datensatz der RWTH Aachen (SQLite-Format)

## Setup

1. Repository klonen:

   ```
   git clone https://github.com/TwinFlowResearch/DigitalTwinN8N.git
   cd DigitalTwinN8N

   ```

2. Datensatz bereitstellen: `data/logistic.sqlite` muss vorhanden sein.
   Falls die Datei nicht im Repository enthalten ist, lade den
   OCEL 2.0 Logistics Simulation Datensatz der RWTH Aachen herunter und
   lege ihn unter diesem Pfad ab.

3. Container starten:

   ```
   docker-compose up -d
   ```

   Beim ersten Start führt PostgreSQL automatisch die Skripte aus
   `init-db/` aus (`01_shadow_log.sql`, `02_view.sql`) und legt damit
   die Shadow-Log-Tabelle sowie den Delta-View `v_effective_events` an.

4. OCEL-Datensatz in die Datenbank importieren:

   ```
   docker exec -it dt-backend python data/migrate_pm4py.py
   ```

   Das Skript liest `data/logistic.sqlite` über `pm4py` ein und überträgt
   die Daten in die Tabellen `event`, `object` und `event_object`.

5. n8n-Workflows importieren: n8n unter `http://localhost:5678` öffnen,
   die vier JSON-Dateien aus `n8n-workflows/` über _Import from File_
   einspielen, und alle Credential-Felder (OpenRouter-API-Schlüssel)
   manuell neu setzen, da n8n-Exporte keine Zugangsdaten enthalten.

6. Workflow `Digital-Twin-Main` aktivieren bzw. publishen, damit der
   Webhook unter `http://localhost:5678/webhook/disruption` erreichbar ist.

## Verwendung

**Manuell testen**, über den Chat-Trigger in n8n: Order-ID eingeben, z. B.
`co1`, optional gefolgt von einem Störungstyp wie `truck_breakdown`. Fehlt
der Störungstyp, wählt das Backend automatisch einen passenden aus den
vorhandenen Events der Bestellung.

**Automatisiert testen**, über den Webhook:

```
curl -X POST http://localhost:5678/webhook/disruption \
  -H "Content-Type: application/json" \
  -d '{"order_id": "co1", "disruption_type": "truck_breakdown"}'
```

**Evaluation durchführen**: Den Workflow `Evaluation-Runner` in n8n öffnen,
die gewünschte `experiment_stage` (1 bis 4) einstellen und ausführen. Der
Workflow ruft `Digital-Twin-Main-Evaluation` automatisiert für mehrere
Kombinationen aus Order-ID und Störungstyp auf.

## Projektstruktur

```
.
├── docker-compose.yml
├── backend/
│   ├── Dockerfile
│   ├── main.py              # FastAPI-Backend mit allen Tool-Endpunkten
│   └── requirements.txt
├── data/
│   ├── logistic.sqlite      # OCEL-2.0-Datensatz (RWTH Aachen)
│   └── migrate_pm4py.py     # Importiert den Datensatz nach PostgreSQL
├── init-db/
│   ├── 01_shadow_log.sql    # Schema der Shadow-Log-Tabelle
│   └── 02_view.sql          # Delta-View v_effective_events
├── n8n-workflows/
│   ├── Digital-Twin-Main.json
│   ├── Agent-Loop.json
│   ├── Digital-Twin-Main-Evaluation.json
│   └── Evaluation-Runner.json

```

## API-Endpunkte (Auszug)

| Endpunkt                       | Zweck                                                                 |
| ------------------------------ | --------------------------------------------------------------------- |
| `/tool/kb_check`               | Prüft, ob für einen Störungstyp bereits eine bewährte Lösung vorliegt |
| `/tool/get_trace`              | Liefert den vollständigen Ereignisablauf einer Bestellung             |
| `/tool/search_alternatives`    | Sucht freie Ersatzressourcen für einen Störungstyp                    |
| `/tool/search_next_vehicle`    | Sucht die nächste verfügbare Schiffsabfahrt                           |
| `/tool/auto_generate_scenario` | Generiert ein Störungsszenario aus echten Datenbankdaten              |
| `/tool/get_thing_descriptions` | Liefert WoT Thing Descriptions betroffener Objekte                    |
| `/tool/write_cascade`          | Schreibt geplante KI-Events in den Shadow-Log                         |
| `/tool/auto_evaluate_outcome`  | Bewertet automatisch, ob ein Testlauf erfolgreich war                 |

Die vollständige Dokumentation aller Endpunkte ist unter
`http://localhost:8000/docs` verfügbar, sobald das Backend läuft
(automatisch generierte OpenAPI-Dokumentation).

## Hinweise

- Die Container kommunizieren ausschließlich über das Docker-Netzwerk
  `dt-network` und adressieren sich über Container-Namen
  (z. B. `http://dt-backend:8000`), nicht über `localhost`.
- Der Konfidenzwert-basierte Routing-Mechanismus (vollautomatisch,
  Benachrichtigung, HITL) ist in `main.py` sowie im Workflow
  `Digital-Twin-Main` (Node `Confidence-Switch`) implementiert.

## Lizenz und Datensatz

Der verwendete OCEL-2.0-Datensatz stammt von der RWTH Aachen und ist nicht
Teil dieses Repositories. (https://www.ocel-standard.org/event-logs/simulations/logistics/)
