SELECT
  p2.name,
  p1.name,
  COUNT(*) AS appearance_count
FROM
  crew c1
  JOIN crew c2 ON c1.title_id = c2.title_id AND c1.category IN ('actor', 'actress') AND c2.category IN ('actor', 'actress') AND c1.person_id < c2.person_id
  JOIN people p1 ON c1.person_id = p1.person_id
  JOIN people p2 ON c2.person_id = p2.person_id
  JOIN titles t ON c1.title_id = t.title_id
GROUP BY
  p1.name,
  p2.name
HAVING
  appearance_count > 0
ORDER BY
  appearance_count DESC,
  p1.name ASC,
  p2.name ASC
LIMIT 1;
