SELECT
    changes.change_id,
    users.user_id,
    users.is_bot,
    pages.page_id,
    changes.change_type,
    changes.comment,
    changes.date,
    changes.time,
    changes.timestamp
FROM {{ ref('stg_recent_changes') }} changes
LEFT JOIN {{ ref('int_users') }} users
    ON changes.username = users.username
LEFT JOIN {{ ref('int_pages') }} pages
    ON changes.url = pages.url
