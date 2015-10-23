#!/bin/sh
parameters_dir=/Users/alexaverbuch/IdeaProjects/ldbc_snb_datagen/substitution_parameters
driver_dir=/Users/alexaverbuch/IdeaProjects/ldbc-load-generator/ldbc_snb_workload_interactive_neo4j/ldbc_driver/src/test/resources/snb/bi/

mv ${parameters_dir}/q1* ${driver_dir}
mv ${parameters_dir}/q2* ${driver_dir}
mv ${parameters_dir}/q3* ${driver_dir}
mv ${parameters_dir}/q4* ${driver_dir}
mv ${parameters_dir}/q5* ${driver_dir}
mv ${parameters_dir}/q6* ${driver_dir}
mv ${parameters_dir}/q7* ${driver_dir}
mv ${parameters_dir}/q8* ${driver_dir}
mv ${parameters_dir}/q9* ${driver_dir}