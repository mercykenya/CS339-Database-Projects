SELECT primary_title
FROM titles
WHERE type = 'movie' AND title_id IN (
    SELECT title_id
    FROM akas
    GROUP BY title_id
    HAVING COUNT(DISTINCT language) = (
        SELECT MAX(num_languages)
        FROM (
            SELECT COUNT(DISTINCT language) AS num_languages
            FROM akas
            GROUP BY title_id
        ) t
    )
)
ORDER BY primary_title;
