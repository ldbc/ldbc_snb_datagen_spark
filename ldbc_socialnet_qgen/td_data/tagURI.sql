sparql select distinct(?tag)
from <sib>
where {
   ?post a snvoc:Post.
   ?post snvoc:hasTag ?tag .
};