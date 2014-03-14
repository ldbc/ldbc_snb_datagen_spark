#!/bin/bash


if [ $# -eq 0 ]
then
      echo "Usage:"
      echo "sh degreeAnalysis.sh <person_knows_person> <outputfile>"
      exit
fi

if [ -z $1 ]
then
      echo "Missing person_knows_peson file"
      echo "Usage:"
      echo "sh degreeAnalysis.sh <person_knows_person> <outputfile>"
      exit
fi

if [ -z $2 ]
then
      echo "Missing output file"
      echo "Usage:"
      echo "sh degreeAnalysis.sh <person_knows_person> <output file>"
      exit
fi

rm -f .degreeAnalysis.tmp
python extractDegrees.py $1 .degreeAnalysis.tmp 
R --no-save --args .degreeAnalysis.tmp $2 degree < cumulative.R 
pstopdf $2
rm .degreeAnalysis.tmp
