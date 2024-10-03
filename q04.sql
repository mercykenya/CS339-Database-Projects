SELECT primary_title, premiered, characters
FROM titles
JOIN (
  SELECT title_id, characters
  FROM crew
  WHERE person_id = (
    SELECT person_id
    FROM people
    WHERE name = 'Tom Cruise'
  )
) AS filtered_crew USING(title_id)
WHERE type = 'movie'
ORDER BY premiered, primary_title;
