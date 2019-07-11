# bash
grep "$1-[0-9]\{2\}-[0-9]\{2\}" ./prices/pp-complete.csv > ./prices/year$1.csv 
echo ID,PRICE,,POSTCODE,TYPE > ./prices/year$1_ALL.csv
grep -E "LONDON|ESSEX|HERTFORD|BUCK|BERKS|SURREY" ./prices/year$1.csv >> ./prices/year$1_ALL.csv



