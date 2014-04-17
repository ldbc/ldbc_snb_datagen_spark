select top 100 t_url, count(*)
from post, post_tag, tag
where
	ps_postid = pst_postid and
	pst_tagid = t_tagid
group by 1
order by 2 desc
;