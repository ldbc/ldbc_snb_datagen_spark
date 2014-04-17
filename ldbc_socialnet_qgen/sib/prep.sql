create procedure path_str (in path any)
{
  declare str any;
  declare inx int;
  str := '';
  foreach (any  st  in path) do
    str := str || sprintf (' %d->%d (%d) ', st[0], coalesce (st[1], 0), coalesce (st[2], 0));
  return str;
}

create procedure c_weight (in p1 bigint, in p2 bigint)
{
  vectored;
  if (p1 is null or p2 is null)
     return 0;
  return 0.5 + 
  	  (select count (*) from post ps1, post ps2
	   where ps1.ps_creatorid = p1 and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = p2 and ps2.ps_replyof is null) +
	  (select count (*) from post ps1, post ps2
	   where ps1.ps_creatorid = p2 and ps1.ps_replyof = ps2.ps_postid and ps2.ps_creatorid = p1 and ps2.ps_replyof is null) +
	  (select 0.5 * count (*) from post c1, post c2
	   where c1.ps_creatorid = p1 and c1.ps_replyof = c2.ps_postid and c2.ps_creatorid = p2 and c2.ps_replyof is not null) +
	  (select 0.5 * count (*) from post c1, post c2
	   where c1.ps_creatorid = p2 and c1.ps_replyof = c2.ps_postid and c2.ps_creatorid = p1 and c2.ps_replyof is not null);
}
