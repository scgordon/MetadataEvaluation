#Data.sh applies the rubric for a dialect to a metadata. This creates the data.csv that
#a RAD.xlsx functions on. It requires three arguments. The first is the organization, the second is the metadata the third 
#is the dialect. GetData.sh is the batch script for Data.sh
cd ../
MetadataEvaluation=$(pwd)
cd ../Crosswalks 
CrosswalkHome=$(pwd) 

# Configuration environment variables
#export PYTHON3=${PYTHON3:-/anaconda/bin/python}

java net.sf.saxon.Transform \
-s:$MetadataEvaluation/metadata/dummy.xml \
-xsl:$CrosswalkHome/Evaluator/KnownNodes.xsl \
-o:$MetadataEvaluation/raw/$1/$2_dataKnown.csv \
recordSetPath=$MetadataEvaluation/metadata/$1/$2/EML/xml \

java net.sf.saxon.Transform \
-s:$MetadataEvaluation/metadata/dummy.xml \
-xsl:$CrosswalkHome/Evaluator/AllNodes.xsl \
-o:$MetadataEvaluation/raw/$1/$2_dataAll.csv \
recordSetPath=$MetadataEvaluation/metadata/$1/$2/EML/xml \

python $MetadataEvaluation/scripts/GetDataTables.py