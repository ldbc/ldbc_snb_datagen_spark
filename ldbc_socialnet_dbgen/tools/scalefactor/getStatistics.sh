RCOMMANT=/usr/bin/R
#May need to install additional package such as data.table, igraph, bit64
#install.packages("data.table")

DATAOUTPUTDIR=/export/scratch2/duc/work/LDBC/ldbc_socialnet_bm/ldbc_socialnet_dbgen/datasetFolder/social_network

echo "Number of comments per users"
$RCOMMANT --slave -f minmaxmean_comment.R --args $DATAOUTPUTDIR/comment_hasCreator_person_*.csv

echo "Number of posts per users"
$RCOMMANT --slave -f minmaxmean_post.R --args $DATAOUTPUTDIR/post_hasCreator_person_*.csv

echo "Number of friends per users"
$RCOMMANT --slave -f minmaxmean_friendships.R --args $DATAOUTPUTDIR/person_knows_person_*.csv

echo "Number of likes per users"
$RCOMMANT --slave -f minmaxmean_likes.R --args $DATAOUTPUTDIR/*likes*.csv

echo "Network cluster coefficient"							       
R --slave -f transitivity.R --args $DATAOUTPUTDIR/person_knows_person_*.csv
