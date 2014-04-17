sparql select ?type count(*)
from <sib>
where {
    ?post a snvoc:Post.
    ?post snvoc:hasTag ?tag .
    ?tag a ?type .
    filter (?type != <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Tag>)
}
group by ?type
order by desc 2
limit 50
;