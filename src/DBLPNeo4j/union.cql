WITH [25753072,26675644] as author_ids
MATCH (p:Person)-[q:HasAuthored]-(r:Paper)
where Id(p) in author_ids
RETURN count(DISTINCT r) ;