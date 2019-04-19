1. 执行feature1.sh生成离散化的ltr特征
2. 其它非离散化的特征可以通过文本编辑器生成，将1，2两部分组合在一起产生最终的ltr特征。
3. 在kibana中执行 put _ltr生成ltr插件需要的索引
4. 执行uploadfeature.sh上传featureset到es
5. 执行uploadmodel.sh上传ltr模型到es

