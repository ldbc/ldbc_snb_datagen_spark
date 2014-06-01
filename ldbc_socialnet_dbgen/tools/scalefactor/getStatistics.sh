#RCOMMAND=/usr/bin/R
RCOMMAND=R
#May need to install additional package such as data.table, igraph, bit64
#install.packages("data.table")

DATAOUTPUTDIR=/scratch/duc/ldbc/ldbc_socialnet_bm/ldbc_socialnet_dbgen/datasetFolder/s1/social_network

echo "Number of comments per users"
$RCOMMAND --slave -f minmaxmean_comment.R --args $DATAOUTPUTDIR/comment_hasCreator_person_*.csv 

echo "Number of posts per users"
$RCOMMAND --slave -f minmaxmean_post.R --args $DATAOUTPUTDIR/post_hasCreator_person_*.csv 

echo "Number of friends per users"
$RCOMMAND --slave -f minmaxmean_friendships.R --args $DATAOUTPUTDIR/person_knows_person_*.csv 

echo "Number of likes per users"
$RCOMMAND --slave -f minmaxmean_likes.R --args $DATAOUTPUTDIR/*likes*.csv 

echo "Network cluster coefficient"							       
R --slave -f transitivity.R --args $DATAOUTPUTDIR/person_knows_person_*.csv 
