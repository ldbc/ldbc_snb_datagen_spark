#!/bin/bash

set -e

[[ `md5sum social_network/static/organisation_0_0.csv                 | cut -d' ' -f1` == 'ebe578bcc856dc7bfea164b4fa194cbc' ]]
[[ `md5sum social_network/static/place_0_0.csv                        | cut -d' ' -f1` == 'a4f5c04146330d3a11b0441522cd6d74' ]]
[[ `md5sum social_network/static/tag_0_0.csv                          | cut -d' ' -f1` == '2ecf6319e808d1cdd45d917ddaab31e1' ]]
[[ `md5sum social_network/static/tagclass_0_0.csv                     | cut -d' ' -f1` == '2a9c260bd69680a82a6369b62e846c71' ]]

[[ `md5sum social_network/dynamic/comment_0_0.csv                     | cut -d' ' -f1` == '8fc7a251596079be643a5cd6b3163b5d' ]]
[[ `md5sum social_network/dynamic/comment_hasTag_tag_0_0.csv          | cut -d' ' -f1` == 'f434548899ea8218ef2e351988d0f117' ]]
[[ `md5sum social_network/dynamic/forum_0_0.csv                       | cut -d' ' -f1` == 'ed77ae23647dbb76ebc9915af5e7e34e' ]]
[[ `md5sum social_network/dynamic/forum_hasMember_person_0_0.csv      | cut -d' ' -f1` == 'b9b5327212d6e12734da6d9e7340b62b' ]]
[[ `md5sum social_network/dynamic/forum_hasTag_tag_0_0.csv            | cut -d' ' -f1` == 'de076e0181c59885b72755f20e73b0e3' ]]
[[ `md5sum social_network/dynamic/person_0_0.csv                      | cut -d' ' -f1` == '23e5a120ddd42abeaa541b8be7758334' ]]
[[ `md5sum social_network/dynamic/person_hasInterest_tag_0_0.csv      | cut -d' ' -f1` == '42e0e4d51cd4fb56b18cd20834216118' ]]
[[ `md5sum social_network/dynamic/person_knows_person_0_0.csv         | cut -d' ' -f1` == 'e8ae7a263e040f9b2b475ebc1e788fe9' ]]
[[ `md5sum social_network/dynamic/person_likes_comment_0_0.csv        | cut -d' ' -f1` == '6d9412934d7e9882fc4154ec74ecf952' ]]
[[ `md5sum social_network/dynamic/person_likes_post_0_0.csv           | cut -d' ' -f1` == 'e0d460d75f5e60b32e0bb544d2d83951' ]]
[[ `md5sum social_network/dynamic/person_studyAt_organisation_0_0.csv | cut -d' ' -f1` == 'cb2313acb51295ebe7fee6176467d2f5' ]]
[[ `md5sum social_network/dynamic/person_workAt_organisation_0_0.csv  | cut -d' ' -f1` == 'ef5a7e95d9870e5715e7f9f4706ee349' ]]
[[ `md5sum social_network/dynamic/post_0_0.csv                        | cut -d' ' -f1` == '11a2d5e2d26fbd60631b15a100d46eda' ]]
[[ `md5sum social_network/dynamic/post_hasTag_tag_0_0.csv             | cut -d' ' -f1` == '1be77b94b9559f4a17a04b996df36638' ]]

[[ `md5sum social_network/updateStream_0_0_forum.csv                  | cut -d' ' -f1` == '7e00243f68a8171974eabe4ac37df86b' ]]
[[ `md5sum social_network/updateStream_0_0_person.csv                 | cut -d' ' -f1` == '2e1f44e6d48112a9fd87092206153b57' ]]
