MATCH (p:Person)-[q:HasAuthored]-(r:Paper)
RETURN p, q, r
LIMIT 100