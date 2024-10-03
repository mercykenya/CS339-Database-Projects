SELECT COUNT(*)
FROM(
    SELECT title_id
    FROM ratings
    WHERE rating = 10.0 AND votes >= 100
)AS subquery;
