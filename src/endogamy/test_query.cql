MATCH (r:Paper) 
WHERE ID(r) = $id   
RETURN r.year as year;