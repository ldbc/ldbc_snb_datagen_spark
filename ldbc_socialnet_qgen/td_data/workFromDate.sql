sparql select concat(min(xsd:int(?date)), '\n', max(xsd:int(?date)))
from <sib>
where {
   ?w snvoc:workFrom ?date .
};