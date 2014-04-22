for f in *.sql; do isql 1206 < ${f%%.*}.sql | head -n -3 | tail -n +9 > ${f%%.*}.txt; done
