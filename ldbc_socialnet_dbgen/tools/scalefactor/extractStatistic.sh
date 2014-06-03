echo "\\begin{tabular}{|c||r|r|r|r|}"
./getStatistics.sh > tmp.txt 2>&1
#grep STATISTICS tmp.txt
grep hline tmp.txt
rm tmp.txt
echo "\\hline"
echo "\\end{tabular}"
