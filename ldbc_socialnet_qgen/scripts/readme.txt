This directory contain sample scripts to run BIBM test driver against Virtuoso database.

./bibmgfg.sh - configuration file to set paths to software
./bin - scripts to generate data, load data into database, and to run tests.
The scripts expect that a number of environment variables are set.
Part of that variables are set in the bibmgfg.sh, and rest of them in specific config.sh
files in database directories.

./bsbm, ./tpch, ./rdfh - specific database directories. Usually contain following files:
./config.sh - configuration file to set specific configuration variables.
./gen.sh - launcher script to generate dataset
./load.sh - launcher script to load dataset
./run*.sh - launcher scrits to run tests

Recommended usage is: copy bibmgfg.sh, and one of specific script directories.
Directory 'bin' need not be copied. Edit configuration file bibmgfg.sh to reflect actual locations of 
the software used. Edit config.sh to reflect database port, scale, and desirable dataset location.
Run consequently scripts gen.sh, load.sh, and run*.sh. 
