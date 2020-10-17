#!/bin/bash
dir=$1

if [ -z "$dir" ] ; then
	echo "usage: No path specified"
	exit -1
fi

echo "Converting files in $dir"

(
	cd $dir
	for file in q*
	do
		if [[ ! "$file" = query* ]] ; then
			newname=${file/q/bi_}
			echo "Converting $file --> $newname"
			mv $file $newname
		fi
	done

	for file in q*
	do
		if [[ "$file" = query_* ]] ; then
			newname=${file/query_/interactive_}
			echo "Converting $file --> $newname"
			mv $file $newname
		fi
	done
)