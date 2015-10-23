#!/bin/sh
connector_dir=/Users/alexaverbuch/IdeaProjects/ldbc-load-generator/ldbc_snb_workload_interactive_neo4j
validation_set_dir=${connector_dir}/data-import/src/test/resources/validation_sets/business_intelligence/neo4j

# --- DELETE ---

rm ${validation_set_dir}/merge/social_network/*

# --- COPY ---

cp /Users/alexaverbuch/hadoopTempDir/output/social_network/* ${validation_set_dir}/merge/social_network/