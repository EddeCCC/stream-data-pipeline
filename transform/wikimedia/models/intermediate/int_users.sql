WITH distinct_users AS (
    SELECT DISTINCT
        username,
        is_bot
    FROM {{ ref('stg_recent_changes') }}
    WHERE username IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER () AS user_id,
    username,
    is_bot
FROM distinct_users
