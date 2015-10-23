#!/bin/sh
connector_dir=/Users/alexaverbuch/IdeaProjects/ldbc-load-generator/ldbc_snb_workload_interactive_neo4j
validation_set_dir=${connector_dir}/data-import/src/test/resources/validation_sets/business_intelligence/neo4j
data_gen_dir=/Users/alexaverbuch/IdeaProjects/ldbc_snb_datagen
parameters_dir=${data_gen_dir}/substitution_parameters

# --- DELETE ---

rm ${validation_set_dir}/params.ini
rm ${validation_set_dir}/validation_params.csv
rm ${validation_set_dir}/substitution_parameters/*
rm ${validation_set_dir}/social_network/*

# --- COPY ---

cp ${data_gen_dir}/params.ini ${validation_set_dir}/
 
cp ${parameters_dir}/q1* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q2* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q3* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q4* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q5* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q6* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q7* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q8* ${validation_set_dir}/substitution_parameters/
cp ${parameters_dir}/q9* ${validation_set_dir}/substitution_parameters/

cp /Users/alexaverbuch/hadoopTempDir/output/social_network/* ${validation_set_dir}/social_network/