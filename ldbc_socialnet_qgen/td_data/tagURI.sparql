sparql select ?tag count(*)
from <sib>
where {
   ?post a snvoc:Post.
   ?post snvoc:hasTag ?tag .
}
group by ?tag
order by desc 2
limit 100
;