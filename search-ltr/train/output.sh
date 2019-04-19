#output
echo 0 na q >> featmap.txt
echo 1 price q >> featmap.txt
echo 2 sellcnt q >> featmap.txt
echo 3 evlcnt q >> featmap.txt
echo 4 collectcnt q >> featmap.txt 
echo 5 addCartCnt q >> featmap.txt 
echo 6 ctr q >> featmap.txt
echo 7 uid_ctr q >> featmap.txt
echo 8 refund_rate q >> featmap.txt
echo 9 cate_ctr q >> featmap.txt
echo 10 cate_weight q >> featmap.txt
echo 11 titleSim q >> featmap.txt
echo 12 cateSim q >> featmap.txt
type="i"
var1="categoryId"
for index in {13..1093}
do
   id=$((${index}-13)) 
   var2=${var1}=${id}
   echo ${index} ${var2} ${type} >> featmap.txt
done
var3="queryId"
for index in {1094..2094}
do
        id=$((${index}-1094))
	var4=${var3}=${id}
	echo ${index} ${var4} ${type} >> featmap.txt
done

