WITH [26573863, 26533382, 27089281, 25000038] as author_ids
MATCH (p:Person)-[q:HasAuthored]-(r:Paper)
where Id(p) in author_ids
with r, size(author_ids) as author_count, count(DISTINCT p) as count
WHERE author_count = count
RETURN count(r);