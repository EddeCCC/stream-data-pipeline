SELECT *
FROM {{ ref('mart_changes_over_time') }}
WHERE make_timestamp(time) > now()
