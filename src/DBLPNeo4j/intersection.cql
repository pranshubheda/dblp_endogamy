WITH [25753072,26675644] as author_ids
MATCH (p:Person)-[q:HasAuthored]-(r:Paper)
where Id(p) in author_ids
with r, size(author_ids) as author_count, count(DISTINCT p) as count
WHERE author_count = count
RETURN count(r);