#!/bin/bash

set -e

[[ `md5sum social_network/social_network_activity_0_0.ttl | cut -d' ' -f1` == '52a5356ecc757a8e3e5bce2c8ea79557' ]]
[[ `md5sum social_network/social_network_person_0_0.ttl   | cut -d' ' -f1` == '30973a3dc339617773651b425f7441e4' ]]
[[ `md5sum social_network/social_network_static_0_0.ttl   | cut -d' ' -f1` == '3c4f4120a2ea1e101cf7d72fbfe30c48' ]]

[[ `md5sum social_network/updateStream_0_0_forum.csv      | cut -d' ' -f1` == '7e00243f68a8171974eabe4ac37df86b' ]]
[[ `md5sum social_network/updateStream_0_0_person.csv     | cut -d' ' -f1` == '2e1f44e6d48112a9fd87092206153b57' ]]
