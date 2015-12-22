#!/bin/bash
DEFAULT_HADOOP_HOME=/home/user//hadoop-2.6.0.stand #change to your hadoop folder
DEFAULT_LDBC_SNB_DATAGEN_HOME=/home/user/ldbc_snb_datagen_0.2 #change to your ldbc_socialnet_dbgen folder
PARAM_GENERATION=1 #param generation

# allow overriding configuration from outside via environment variables
# i.e. you can do
#     HADOOP_HOME=/foo/bar LDBC_SNB_DATAGEN_HOME=/baz/quux ./run.sh
# instead of changing the contents of this file
HADOOP_HOME=${HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
LDBC_SNB_DATAGEN_HOME=${LDBC_SNB_DATAGEN_HOME:-$DEFAULT_LDBC_SNB_DATAGEN_HOME}

export HADOOP_HOME
export LDBC_SNB_DATAGEN_HOME

mvn clean
mvn -DskipTests assembly:assembly 

cp $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen.jar $LDBC_SNB_DATAGEN_HOME/

java -cp $LDBC_SNB_DATAGEN_HOME/ldbc_snb_datagen.jar org.apache.hadoop.util.RunJar $LDBC_SNB_DATAGEN_HOME/ldbc_snb_datagen.jar  $LDBC_SNB_DATAGEN_HOME/test_params.ini


### TEST SCRIPTS ####

ENTITIES="person post comment forum  place tag tagclass organisation "
RELATIONS="person_knows_person organisation_isLocatedIn_place place_isPartOf_place tagclass_isSubclassOf_tagclass tag_hasType_tagclass person_studyAt_organisation person_hasInterest_tag person_workAt_organisation person_isLocatedIn_place forum_hasTag_tag forum_hasModerator_person forum_hasMember_person forum_containerOf_post comment_hasCreator_person comment_hasTag_tag comment_isLocatedIn_place comment_replyOf_comment comment_replyOf_post post_hasCreator_person post_hasTag_tag post_isLocatedIn_place person_likes_comment person_likes_post"
ATTRIBUTES="person_speaks_language person_email_emailaddress  "

FILES="$ENTITIES $RELATIONS "

for file in $ENTITIES
do
  echo "TESTING FILE: $file"
  python2 ./test/validateIdUniqueness.py ./test_data/social_network/${file}_?_?.csv
done

for file in $RELATIONS
do
  echo "TESTING FILE: $file"
  python2 ./test/validatePairUniqueness.py 0 1 ./test_data/social_network/${file}_?_?.csv
done

echo "TESTING KNOWS SUBGRAPH INTEGRITY"
python2 ./test/validateKnowsGraph.py ./test_data/social_network

echo "TESTING STUDYAT SUBGRAPH INTEGRITY"
python2 ./test/validateStudyAt.py ./test_data/social_network/

echo "TESTING UPDATE STREAMS"
for file in `ls ./test_data/social_network/updateStream_*`
do
  python2 ./test/validateUpdateStream.py $file
done
