
RUNSQL() {
sqlfile=$1
$isql -S $PORT dba dba  PROMPT=OFF VERBOSE=OFF ERRORS=STDOUT < $sqlfile
status=$?
if [ $status -ne 0 ]
then 
  echo loading $sqlfile failed with code: $status
  exit $status
fi;
}

EXEC_SCL() {
cmd=$1
echo $cmd
$isql -S $PORT dba dba "exec=$cmd"
status=$?
if [ $status -ne 0 ]
then 
  echo $cmd failed: $status
  exit $status
fi;
}

LOAD_RDFH_GZ() {
ddir=$1
graph=$2

$isql -S $PORT dba dba  <<INPUT
-- No text index 
cl_text_index (0);

ld_dir ('$ddir', '%.gz', '$graph');

-- Record CPU time 
select getrusage ()[0] + getrusage ()[1];

rdf_loader_run () &
rdf_loader_run () &
rdf_loader_run () &
rdf_loader_run () &

rdf_loader_run () &
rdf_loader_run () &
rdf_loader_run () &
rdf_loader_run () &

wait_for_children;
checkpoint;

-- Record CPU time
select getrusage ()[0] + getrusage ()[1];

-- count the quads.  The count is not timed as part of the load.
sparql select count (*) from <$graph> { ?s ?p ?o};

INPUT

status=$?
if [ $status -ne 0 ]
then 
  echo "loading failed: $status"
  echo "failed: $status" >> $scalestampfile
  exit $status
fi;
}

CHECKPOINT() {
  EXEC_SCL "checkpoint;"
}