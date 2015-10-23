#!/bin/sh
data_gen_dir=/Users/alexaverbuch/IdeaProjects/ldbc_snb_datagen
scripts_dir=${data_gen_dir}/scripts

rm ${data_gen_dir}/params.ini
cp ${scripts_dir}/params_regular.ini ${data_gen_dir}/params.ini
bash ${data_gen_dir}/run.sh
bash ${scripts_dir}/copy_data_and_params_to_validation_set.sh 
rm ${data_gen_dir}/params.ini
cp ${scripts_dir}/params_merge.ini ${data_gen_dir}/params.ini
bash ${data_gen_dir}/run.sh
bash ${scripts_dir}/copy_merge_data_to_validation_set.sh
rm ${data_gen_dir}/params.ini
cp ${scripts_dir}/params_regular.ini ${data_gen_dir}/params.ini