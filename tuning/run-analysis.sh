#!/bin/bash

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
  # keep creationDate, deletionDate, explicitlyDeleted
  cut -d'|' -f1-3 $person >> $person >> $person_trimmed
  cut -d'|' -f1-3 $forum >> $forum_trimmed
  cut -d'|' -f1-3 $post >> $post_trimmed
  cut -d'|' -f1-3 $comment >> $comment_trimmed
  cut -d'|' -f1-3 $person_likes_comment >> $person_likes_comment_trimmed
  cut -d'|' -f1-3 $person_likes_post >> $person_likes_post_trimmed
  cut -d'|' -f1-3 $person_knows_person >> $person_knows_person
  cut -d'|' -f1-3 $forum_hasMember_person >> $forum_hasMember_person_trimmed
else
  echo "skip trimming"
fi

if [ "$2" == "a" ]; then 
  # run analysis 
  Rscript analysis.R
else
  echo "skip analysis"
fi
