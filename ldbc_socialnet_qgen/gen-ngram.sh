#! /bin/bash

## INPUT: 2 arguments:
### first argument: number of term in the gram to generate
### second argument: number of n-gram to generate in total
## OUTPUT: the generated n-gram dictionary is named "ngram.gen.out"

UNIGRAM=1gram.uniq
ngram=""
rm $1gram.gen.out

declare -a seen

nlines=`wc -l $UNIGRAM | cut -f 1 -d ' '`
for i in `seq 1 $2`; do
	for j in `seq 1 $1`; do                                 # Create a n-gram term
	        nb=$((RANDOM % nlines))                         # randomly choose a unigram term
		while [[ ${seen[$nb]} ]]; do                    # Choose another term if this one has already been picked
			nb=$((RANDOM % nlines))
		done
		seen[$nb]=1                                     # Put the unigram term into a list so that it won't be picked again
	        ngram="$ngram `head -n $nb tmp | tail -n 1`"
	done
	echo $ngram >> $1gram.gen.out
	ngram=""
done
