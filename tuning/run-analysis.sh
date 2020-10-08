#!/bin/bash

# keep operation timestamp and type 
# TODO: trim arbitrary number of updateStream_*_*_*.csv files
base=$LDBC_SNB_DATAGEN_HOME/social_network/updateStream_0_0_person.csv
trimmed=$LDBC_SNB_DATAGEN_HOME/social_network/updateStream_0_0_person_trimmed.csv
cut -d'|' -f1,3 $base >> $trimmed

# run analysis 
Rscript analysis.R
