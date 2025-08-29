WITH changes_per_minute AS (
    SELECT
        changes.timestamp,
        changes.change_type,
        COUNT(*) AS count
    FROM {{ ref('int_recent_changes') }} as changes
    GROUP BY changes.timestamp, changes.change_type
)

SELECT *
FROM changes_per_minute
ORDER BY timestamp
