WITH page_edit_counts AS (
    SELECT
        pages.page_id,
        pages.title,
        COUNT(changes.change_id) AS edit_count
    FROM {{ ref('int_recent_changes') }} AS changes
    INNER JOIN {{ ref('int_pages') }} AS pages
      ON changes.page_id = pages.page_id
    GROUP BY pages.page_id, pages.title
)

SELECT *
FROM page_edit_counts
ORDER BY edit_count DESC
LIMIT 10
