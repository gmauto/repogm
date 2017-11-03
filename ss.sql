

spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ori;
select distinct makes,series,model
from label_order
" > mark_tmp
cat /home/ori/general/bin/conf/CHE | awk -F "\t" '{print $1"\t"$2"\t"$3"}' | sort -u >> mark_tmp
cat mark_tmp | sort |uniq -u > mark_new


