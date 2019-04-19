#output
echo 0 na q >> featmap1.txt
echo 1 hitattrs q  >> featmap1.txt
echo 2 hitpreCategoryList q >> featmap1.txt
echo 3 price q  >> featmap1.txt
echo 4 difftm q >> featmap1.txt
echo 5 sellcnt q >> featmap1.txt
echo 6 evlcnt q  >> featmap1.txt
echo 7 collectcnt q >> featmap1.txt 
echo 8 add_cart_count q  >> featmap1.txt 
echo 9 hittitle q >> featmap1.txt
echo 10 queryid q  >> featmap1.txt
echo 11 ctr q >> featmap1.txt
echo 12 uid_ctr q >> featmap1.txt
echo 13 category_ctr q  >> featmap1.txt
var3="category"
type="i"
for index in {14..882}
do 
   var4=${var3}${index}
   echo ${index} ${var4} ${type} >> featmap1.txt
done

