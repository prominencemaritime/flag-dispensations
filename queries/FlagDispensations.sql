/*
job_entities.type = 'flag-extension-dispensation'
schedule frequency=1h
lookback=1d
job_status=for_approval
*/
SELECT
	v.email AS vsl_email,
	jv.vessel_id AS vessel_id,
	v.name AS vessel,
	jv.job_id as job_id,
	ji.name as importance,
	je.title AS title,
	fedt.name as dispensation_type,
	d.name AS department,
	je.due_date AS due_date,
	jvfe.requested_on AS requested_on,
	je.created_at AS created_at,
	js.name AS status
FROM 
	job_entities je 
LEFT JOIN
	job_importances ji
	ON ji.id = je.importance_id
LEFT JOIN
	departments d
	ON d.id = je.main_department_id
LEFT JOIN
	ports p
	ON p.id = je.port_id
LEFT JOIN
	job_statuses js
	ON js.id = je.status_id
LEFT JOIN
	job_vessel_flag_extensions jvfe
	ON jvfe.job_id = je.id
LEFT JOIN
	flag_extension_and_dispensation_types fedt
	ON fedt.id = jvfe.type_id
LEFT JOIN
	job_vessels jv
	ON jv.job_id = je.id
LEFT JOIN
	vessels v
	ON v.id = jv.vessel_id
WHERE
	je.type = 'flag-extension-dispensation'
	AND je.deleted_at IS NULL
	AND je.archived_at IS NULL
	AND v.active = 'true'
	AND je.created_at >= NOW() - INTERVAL '1 day' * :lookback_days -- 1
	AND js.label = :job_status;  -- 'for_approval'
