SELECT premiered AS year, ROUND(AVG(rating), 2) AS avg_rating
FROM titles
JOIN ratings ON titles.title_id = ratings.title_id
WHERE type = 'movie' AND year IS NOT NULL AND rating IS NOT NULL
GROUP BY year
HAVING avg_rating > (SELECT ROUND(AVG(rating), 2) FROM ratings WHERE title_id IN (SELECT title_id FROM titles WHERE type = 'movie'))
ORDER BY year ASC;