SELECT
    CAST(id AS BIGINT) as change_id,
    CAST(trim(user) AS VARCHAR) AS username,
    CAST(bot AS BOOLEAN) AS is_bot,
    CAST(trim(type) AS VARCHAR) AS change_type,
    CAST(trim(title) AS VARCHAR) as title,
    CAST(trim(title_url) AS VARCHAR) AS url,
    CAST(trim(comment) AS VARCHAR) as comment,
    CAST(timestamp AS BIGINT) as timestamp,
    CAST(date AS DATE) as date,
    CAST(time AS TIME) as time
FROM {{ source('wikimedia', 'recent_changes_raw') }}
