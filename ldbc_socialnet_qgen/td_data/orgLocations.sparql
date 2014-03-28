sparql select ?loc count(*)
from <sib> 
where { 
   ?w snvoc:hasOrganisation ?org .
   ?org snvoc:isLocatedIn ?loc.
}
group by ?loc
order by desc 2
limit 100;