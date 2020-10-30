#!/bin/bash

s=`date +%s`

DIR=$LDBC_SNB_DATAGEN_HOME/out/social_network/dynamic/

person=$DIR/person_0_0.csv
person_trimmed=$DIR/person_0_0_trimmed.csv
forum=$DIR/forum_0_0.csv
forum_trimmed=$DIR/forum_0_0_trimmed.csv
post=$DIR/post_0_0.csv
post_trimmed=$DIR/post_0_0_trimmed.csv
comment=$DIR/comment_0_0.csv
comment_trimmed=$DIR/comment_0_0_trimmed.csv
person_likes_comment=$DIR/person_likes_comment_0_0.csv
person_likes_comment_trimmed=$DIR/person_likes_comment_0_0_trimmed.csv
person_likes_post=$DIR/person_likes_post_0_0.csv
person_likes_post_trimmed=$DIR/person_likes_post_0_0_trimmed.csv
person_knows_person=$DIR/person_knows_person_0_0.csv
person_knows_person_trimmed=$DIR/person_knows_person_0_0_trimmed.csv
forum_hasMember_person=$DIR/forum_hasMember_person_0_0.csv
forum_hasMember_person_trimmed=$DIR/forum_hasMember_person_0_0_trimmed.csv


if [ "$1" == "t" ]; then 
  echo "trimming files..."
  st=`date +%s`
  # keep creationDate, deletionDate, explicitlyDeleted, [forum_type]
  cut -d'|' -f1-3 $person > $person_trimmed
  cut -d'|' -f1-3,6 $forum > $forum_trimmed
  cut -d'|' -f1-3 $post > $post_trimmed
  cut -d'|' -f1-3 $comment > $comment_trimmed
  cut -d'|' -f1-3 $person_likes_comment > $person_likes_comment_trimmed
  cut -d'|' -f1-3 $person_likes_post > $person_likes_post_trimmed
  cut -d'|' -f1-3 $person_knows_person > $person_knows_person_trimmed
  cut -d'|' -f1-3,6 $forum_hasMember_person > $forum_hasMember_person_trimmed
  et=`date +%s`
  rt=$((et-st))
  echo "files trimmed in $rt secs!"
else
  echo "skip trimming files"
fi

if [ "$2" == "t" ]; then 
  sa=`date +%s`
  echo "computing batches..."
  Rscript ./tuning/analysis.R
  ea=`date +%s`
  ra=$((ea-sa))
  echo "computed in $ra secs!"
else
  echo "skip analysis"
fi
e=`date +%s`
r=$((e-s)) 
echo "time taken: $r secs!"