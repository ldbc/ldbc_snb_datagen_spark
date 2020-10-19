#!/bin/bash

./generateparams.py ../out_old/build/ ../substitution_parameters && \
	./generateparamsbi.py ../out_old/build/ ../substitution_parameters
