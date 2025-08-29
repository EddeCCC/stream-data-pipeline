WITH counts AS (
    SELECT
        SUM(CASE WHEN is_bot = FALSE THEN 1 ELSE 0 END) AS human_edits,
        SUM(CASE WHEN is_bot = TRUE THEN 1 ELSE 0 END) AS bot_edits
    FROM {{ ref('int_recent_changes') }}
)

SELECT
    CAST(human_edits as INTEGER) as human_edits,
    CAST(bot_edits as INTEGER) as bot_edits,
    CASE
        WHEN bot_edits = 0 THEN 0
        ELSE CAST(human_edits AS DOUBLE) / bot_edits
    END AS human_bot_ratio
FROM counts
