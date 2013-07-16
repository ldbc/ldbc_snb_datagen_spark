#!/bin/sh

. ${0%/*}/test_fn.sh
scalestampfile=scale.loaded
RUNSQL $bindir/bsbm_load.sql
EXEC_SCL "DB.DBA.RDF_OBJ_FT_RULE_ADD (null, null, 'All');"
EXEC_SCL "DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ ();"

echo "text indexing finished at `date` ">> $scalestampfile