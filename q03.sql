SELECT COUNT(DISTINCT title_id)
FROM titles
WHERE type != 'movie'
  AND genres LIKE '%Sci-Fi%'
  AND genres LIKE '%Western%';
