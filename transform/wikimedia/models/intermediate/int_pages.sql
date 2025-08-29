WITH distinct_pages AS (
    SELECT DISTINCT
        title,
        url
    FROM {{ ref('stg_recent_changes') }}
    WHERE title IS NOT NULL
      AND url IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER () AS page_id,
    title,
    url
FROM distinct_pages
