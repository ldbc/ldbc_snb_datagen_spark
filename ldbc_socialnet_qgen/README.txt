
BIBM Enhancements for version 0.7.7
- sql driver and sql loader accepts options -user and -password.

BIBM Enhancements for version 0.7.6
- qualification comparison of dates now ignores time part.

BIBM Enhancements for version 0.7.3
- helper scripts to run testdriver added (directory 'scripts').

BIBM Enhancements for version 0.7.2
1. bsbm/ir queries rewritten
2. -dry-run option implemented

BIBM Enhancements for version 0.7.1
- implementation of TPCH refresh functions added

BIBM Enhancements for version 0.7
- BSBM/IR patch2 integrated
- bsbm/ir queries rewritten to use Selectivity parameter
- tpch/sparql* queries rewritten: tag "tpcd:" changed to "rdfh:"
 
BIBM Enhancements for version 0.6
- csv converter (com.openlinksw.util.csv2ttl.Main) can also load data into SPARQL store via http endpoint.
This mode turns on when -u <http update endpoint> is supplied.
To support this mode, convertion schema includes property "update_header".
