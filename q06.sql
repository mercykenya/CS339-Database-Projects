SELECT primary_title, rating, votes
FROM titles
JOIN ratings USING (title_id)
WHERE type = 'movie' AND premiered = 2022
ORDER BY rating DESC, votes DESC, primary_title ASC
LIMIT 10;
