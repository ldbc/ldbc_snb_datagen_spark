create procedure bsbm_load_1 (in fname varchar)
{
  whenever sqlstate '*' goto err;
  dbg_obj_princ ('Starting loading ', fname);
  DB.DBA.TTLP (file_open (fname), 'http://bsbm/', 'http://bsbm/', 255, 0, 0);
  dbg_obj_princ ('Completed ', fname);
  return;
err:
  dbg_obj_princ ('Error loading ', fname, ': ', __SQL_STATE, ' ', __SQL_MESSAGE);
}
;

create procedure bsbm_load (in dir varchar)
{
  declare arr, aq any;
  arr := sys_dirlist (dir, 1);
  log_enable (2, 1);
  aq := async_queue (8);
  foreach (varchar f in arr) do
    {
      if (f like '*.ttl')
        {
          aq_request (aq, 'DB.DBA.bsbm_load_1', vector (dir || '/' || f));
        }
    }
  aq_wait_all (aq);
  exec ('checkpoint');
}
;

create function DB.DBA.RDF_OBJ_FT_RULE_ADD
  (in rule_g varchar, in rule_p varchar, in reason varchar) returns integer

;
--bsbm_load ('dataset');