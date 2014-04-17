select top 50 tc_url, count(*)
from post, post_tag, tag_tagclass, tagclass
where
	ps_postid = pst_postid and
	pst_tagid = ttc_tagid and
	ttc_tagclassid = tc_tagclassid
group by 1
order by 2 desc
;