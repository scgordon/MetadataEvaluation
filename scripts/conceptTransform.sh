#!/bin/sh
# xmlTransform.sh applies the rubric for a dialect to a metadata. 
# This creates an evaluated.csv that has a row for each element 
# or attribute that has text content. 
# It requires three arguments. The first is the organization, the second is the collection of metadata the third 
#is the dialect. 
scripts=$(pwd)
cd ../
MetadataEvaluation=$(pwd)
cd ../Crosswalks 
CrosswalkHome=$(pwd) 

java net.sf.saxon.Transform \
-s:$MetadataEvaluation/scripts/dummy.xml \
-xsl:$CrosswalkHome/Evaluator/KnownNodes.xsl \
-o:$MetadataEvaluation/data/$1/$2_$3_ConceptEvaluated.csv \
recordSetPath=$MetadataEvaluation/metadata/$1/$2/$3/xml \

gzip -f $MetadataEvaluation/data/$1/$2_$3_ConceptEvaluated.csv
cd $scripts
