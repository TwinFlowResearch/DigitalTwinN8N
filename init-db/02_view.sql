CREATE OR REPLACE VIEW v_effective_events AS
SELECT e.ocel_id AS event_id, e.ocel_type AS event_type, e.ocel_time AS event_timestamp,
       'BASELINE' AS source, NULL::TEXT AS shadow_id,
       NULL::REAL AS confidence_score, NULL::TEXT AS chain_of_thought
FROM event e
WHERE e.ocel_id NOT IN (
    SELECT overrides_event_id FROM shadow_events WHERE overrides_event_id IS NOT NULL)
UNION ALL
SELECT s.overrides_event_id, s.event_type, s.event_timestamp,
       'AI_OVERRIDE', s.shadow_id, s.confidence_score, s.chain_of_thought
FROM shadow_events s WHERE s.overrides_event_id IS NOT NULL
UNION ALL
SELECT s.shadow_id, s.event_type, s.event_timestamp,
       'AI_NEW', s.shadow_id, s.confidence_score, s.chain_of_thought
FROM shadow_events s WHERE s.is_new_event = TRUE;