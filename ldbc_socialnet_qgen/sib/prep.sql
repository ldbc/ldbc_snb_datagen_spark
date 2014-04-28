xml_set_ns_decl ('snvoc', 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/', 2);
xml_set_ns_decl ('sn', 'http://www.ldbc.eu/ldbc_socialnet/1.0/data/', 2);
xml_set_ns_decl ('dbpedia-owl', 'http://dbpedia.org/ontology/', 2);


create procedure path_str (in path any)
{
  declare str any;
  declare inx int;
  str := '';
  foreach (any  st  in path) do
    str := str || sprintf (' %d->%d (%d) ', st[0], coalesce (st[1], 0), coalesce (st[2], 0));
  return str;
}

create procedure c_weight (in p1 bigint, in p2 bigint)
{
  vectored;
  if (p1 is null or p2 is null)
     return 0;
  return 0.5 + 
  	  (select count (*) from post ps1, post ps2
	   where ps1.ps_creatorid = p1 and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = p2 and ps2.ps_replyof is null) +
	  (select count (*) from post ps1, post ps2
	   where ps1.ps_creatorid = p2 and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = p1 and ps2.ps_replyof is null) +
	  (select 0.5 * count (*) from post c1, post c2
	   where c1.ps_creatorid = p1 and c1.ps_replyof = c2.ps_postid and c2.ps_creatorid = p2 and c2.ps_replyof is not null) +
	  (select 0.5 * count (*) from post c1, post c2
	   where c1.ps_creatorid = p2 and c1.ps_replyof = c2.ps_postid and c2.ps_creatorid = p1 and c2.ps_replyof is not null);
}

create procedure path_str_sparql (in path any)
{
  declare str any;
  declare inx int;
  str := '';
  foreach (any  st  in path) do
    str := str || sprintf (' %d->%d (%d) ', cast (substring(st[0], 48, 20) as int), coalesce(cast (substring(st[1], 48, 20) as int), 0), coalesce (st[2], 0));
  return str;
}

create procedure c_weight_sparql1 (in p1 varchar, in p2 varchar)
{
  vectored;
  if (p1 is null or p2 is null)
     return 0;
  return 0.5 + 
  	 ( sparql select count(*) from <sib> where {?post1 snvoc:hasCreator ?:p1. ?post1 snvoc:replyOf ?post2. ?post2 snvoc:hasCreator ?:p2. ?post2 a snvoc:Post} ) +
  	 ( sparql select count(*) from <sib> where {?post1 snvoc:hasCreator ?:p2. ?post1 snvoc:replyOf ?post2. ?post2 snvoc:hasCreator ?:p1. ?post2 a snvoc:Post} ) +
  	 ( sparql select 0.5 * count(*) from <sib> where {?post1 snvoc:hasCreator ?:p1. ?post1 snvoc:replyOf ?post2. ?post2 snvoc:hasCreator ?:p2. ?post2 a snvoc:Comment} ) +
  	 ( sparql select 0.5 * count(*) from <sib> where {?post1 snvoc:hasCreator ?:p2. ?post1 snvoc:replyOf ?post2. ?post2 snvoc:hasCreator ?:p1. ?post2 a snvoc:Comment} );
}

create procedure c_weight_sparql (in p1 varchar, in p2 varchar)
{
  vectored;
  if (p1 is null or p2 is null)
     return 0;
  return 0.5 + 
    (SELECT COUNT (*)
     FROM RDF_QUAD AS r0
       INNER JOIN DB.DBA.RDF_QUAD AS r1
       ON ( r0.S  = r1.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r2
       ON ( r1.O  = r2.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r3
       ON ( r1.O  = r3.S 
       	  AND  r2.S  = r3.S )
     WHERE  r0.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r1.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf')
       AND  r2.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r3.P = __i2idn ( 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
       AND  r3.O = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post')
       AND  r0.O = __i2idn (p1) AND r2.O = __i2idn (p2)) +
    (SELECT COUNT (*)
     FROM RDF_QUAD AS r0
       INNER JOIN DB.DBA.RDF_QUAD AS r1
       ON ( r0.S  = r1.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r2
       ON ( r1.O  = r2.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r3
       ON ( r1.O  = r3.S 
       	  AND  r2.S  = r3.S )
     WHERE  r0.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r1.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf')
       AND  r2.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r3.P = __i2idn ( 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
       AND  r3.O = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post')
       AND  r0.O = __i2idn (p2) AND r2.O = __i2idn (p1)) +
    (SELECT 0.5 * COUNT (*)
     FROM RDF_QUAD AS r0
       INNER JOIN DB.DBA.RDF_QUAD AS r1
       ON ( r0.S  = r1.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r2
       ON ( r1.O  = r2.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r3
       ON ( r1.O  = r3.S 
       	  AND  r2.S  = r3.S )
     WHERE  r0.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r1.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf')
       AND  r2.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r3.P = __i2idn ( 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
       AND  r3.O = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment')
       AND  r0.O = __i2idn (p1) AND r2.O = __i2idn (p2)) +
    (SELECT 0.5 * COUNT (*)
     FROM RDF_QUAD AS r0
       INNER JOIN DB.DBA.RDF_QUAD AS r1
       ON ( r0.S  = r1.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r2
       ON ( r1.O  = r2.S )
       INNER JOIN DB.DBA.RDF_QUAD AS r3
       ON ( r1.O  = r3.S 
       	  AND  r2.S  = r3.S )
     WHERE  r0.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r1.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf')
       AND  r2.P = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator')
       AND  r3.P = __i2idn ( 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
       AND  r3.O = __i2idn ( 'http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment')
       AND  r0.O = __i2idn (p2) AND r2.O = __i2idn (p1));
}
