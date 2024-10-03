SELECT COUNT(DISTINCT titles.title_id) AS num_movies
FROM crew
JOIN titles ON crew.title_id = titles.title_id
WHERE crew.category = 'director' AND titles.type = 'movie'
GROUP BY crew.person_id
ORDER BY num_movies DESC
LIMIT 1;


