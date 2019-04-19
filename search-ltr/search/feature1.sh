#output
filename=featureset.tmp
indexname=index
var3="category"
for index in {14..882}
do 
   var4=${var3}${index}
   id=$((${index}-14))
   echo "{" >> ${filename}
   echo "    \"name\": \"${var4}\"," >> ${filename}
   echo "    \"template_language\": \"mustache\","  >> ${filename}
   echo "    \"template\": {" >> ${filename}
   echo "    \"match\": {" >> ${filename}
   echo "        \"categoryId\": ${id}"  >> ${filename}
   echo "        }" >> ${filename}
   echo "    }" >> ${filename}
   echo "}," >> ${filename}
done
