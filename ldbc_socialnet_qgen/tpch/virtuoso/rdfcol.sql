

-- convert from row wise to column-wise rdf.


-- 
drop index rdf_quad_pogs;
drop index rdf_quad_sp;
drop index rdf_quad_op;
drop index rdf_quad_gs;

alter table rdf_quad rename rq_rows;

create table RDF_QUAD (
  G IRI_ID_8,
  S IRI_ID_8,
  P IRI_ID_8,
  O any,
  primary key (P, S, O, G) column
  )
alter index RDF_QUAD on RDF_QUAD partition (S int (0hexffff00))
create column index RDF_QUAD_POGS on RDF_QUAD (P, O, G, S) partition (O varchar (-1, 0hexffff))
;
create distinct no primary key ref column index RDF_QUAD_SP on RDF_QUAD (S, P) partition (S int (0hexffff00))
create distinct no primary key ref column index RDF_QUAD_GS on RDF_QUAD (G, S) partition (S int (0hexffff00))
create distinct no primary key ref column index RDF_QUAD_OP on RDF_QUAD (O, P) partition (O varchar (-1, 0hexffff))
;

insert into rdf_quad (g,s,p,o) select g,s,p,o from rq_rows;
