#!/bin/bash

./regen-python.sh && cd ../substitution_parameters && for i in *.txt; do echo $i: ; diff -u ../substitution_parameters_old/$i $i;  done && cd ../paramgenerator/
