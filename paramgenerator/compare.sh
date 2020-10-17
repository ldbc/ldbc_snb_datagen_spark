#!/bin/bash

./regen-python.sh && cd ../substitution_parameters && for i in *.txt; do echo $i: ; diff -u $i ../substitution_parameters_old/$i;  done && cd ../paramgenerator/
