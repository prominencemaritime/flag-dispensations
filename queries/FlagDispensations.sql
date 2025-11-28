/*
event_type='Passage Plan'
et.id=37
es.name='for-review'
es.id=3
schedule_frequency_hours=0.5
lookback_days=1
*/
SELECT
	et.id AS event_type_id,
	et.name AS event_type_name,
	v.email AS vsl_email,
	v.id AS vessel_id,
	v.name AS vessel_name,
	e.id AS event_id,
	e.name AS event_name,
	e.created_at,
	ed.synced_at,
	es.name AS status,
	es.id AS status_id
FROM
	events e
LEFT JOIN vessels v ON v.id = e.vessel_id
LEFT JOIN event_details ed ON ed.event_id = e.id
LEFT JOIN event_statuses es ON es.id = ed.status_id
LEFT JOIN event_types et ON et.id = e.type_id
WHERE
	et.id = 37  --passage-plan
	AND es.id = 3   --for-review
	AND LOWER(e.name) NOT LIKE '%test%'
	AND LOWER(e.name) NOT LIKE '%vessel%'
	AND ed.synced_at >= NOW() - INTERVAL '1 day' * :lookback_days
	AND e.deleted_at IS NULL
ORDER BY
	ed.synced_at ASC;
