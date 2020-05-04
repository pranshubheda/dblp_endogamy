MATCH (p:Person)-[q:HasAuthored]-(r:Paper)
WITH r, count(p) as no_of_authors
WHERE no_of_authors > 2
RETURN r, no_of_authors
LIMIT 100