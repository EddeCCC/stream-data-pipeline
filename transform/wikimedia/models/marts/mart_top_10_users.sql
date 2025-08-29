WITH user_edit_counts AS (
    SELECT
        users.user_id,
        users.username,
        COUNT(changes.user_id) AS edit_count
    FROM {{ ref('int_recent_changes') }} AS changes
    INNER JOIN {{ ref('int_users') }} AS users
      ON changes.user_id = users.user_id
    GROUP BY users.user_id, users.username
)

SELECT *
FROM user_edit_counts
ORDER BY edit_count DESC
LIMIT 10
