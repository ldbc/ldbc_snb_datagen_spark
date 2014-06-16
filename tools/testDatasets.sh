#!/bin/bash

if [ $# -ne 4 ]
then
   echo "Arguments not correctly supplied"
   echo "Usage: sh testDatasets <num_reducers1> <dir1> <num_reducers2> <dir2>"
   exit
fi

FILES="comment forum_containerOf_post person_email_emailaddress  person_studyAt_organisation post_isLocatedIn_place comment_hasCreator_person forum_hasMember_person           person_hasInterest_tag person_workAt_organisation tag comment_hasTag_tag forum_hasModerator_person person_isLocatedIn_place place tagclass comment_isLocatedIn_place forum_hasTag_tag person_knows_person place_isPartOf_place tagclass_isSubclassOf_tagclass comment_replyOf_comment organisation person_likes_comment post tag_hasType_tagclass comment_replyOf_post organisation_isLocatedIn_place person_likes_post post_hasCreator_person forum person person_speaks_language post_hasTag_tag"

NUM_REDUCERS_1=$1
DIR_1=$2
NUM_REDUCERS_2=$3
DIR_2=$4

for file in $FILES
do
   echo "CHECKING FILE $file"
   rm -f .auxFile1
   rm -f .auxFile2
   # creating auxiliary file 1
   for i in $(seq 0 $(($NUM_REDUCERS_1-1)))
   do
       tail -n +2 $DIR_1/${file}_${i}.csv >> .auxFile1
       sort .auxFile1 > .auxFile1Sorted
   done

   # creating auxiliary file 2 
   for i in $(seq 0 $(($NUM_REDUCERS_2-1)))
   do
       tail -n +2 $DIR_2/${file}_${i}.csv >> .auxFile2
       sort .auxFile2 > .auxFile2Sorted
   done

   # computing checksums
   a=$(md5sum .auxFile1Sorted | awk '{print $1}')
   b=$(md5sum .auxFile2Sorted | awk '{print $1}')
    
   if [ "$a"=="$b" ] 
   then
       echo ${file} are equal 
       echo ${a} 
       echo ${b} 
   else
       echo ERROR!!!!! ${file} are different 
       echo ${a} 
       echo ${b} 
       exit
   fi
   echo "---------------------"
done
echo GREAT!!!!! the two datasets are the same! 
rm -f .auxFileSorted1
rm -f .auxFileSorted2
rm -f .auxFile1
rm -f .auxFile2


