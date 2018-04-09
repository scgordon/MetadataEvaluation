#Data.sh applies the rubric for a dialect to a metadata. This creates the data.csv that
#a RAD.xlsx functions on. It requires three arguments. The first is the organization, the second is the metadata the third 
#is the dialect. GetData.sh is the batch script for Data.sh
cd ../
MetadataEvaluation=$(pwd)
cd ../Crosswalks 
CrosswalkHome=$(pwd) 

# Configuration environment variables
export PYTHON3=${PYTHON3:-/anaconda/bin/python}

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

    $PYTHON3 <<CODE
import pandas as pd
import os


    #Create a Recommendations Analysis data product
def ConceptCounts(EvaluatedMetadataDF, Organization, Collection, Dialect):
    RAD='$MetadataEvaluation/data/'+Organization+'/'+Collection+'_'+Dialect+'_RAD.csv'
    #dialectOccurrenceDF = pd.read_csv('../table/dialectContains.csv')
    #dialectOccurrenceDF=dialectOccurrenceDF['MetadataDialect']=='Dialect'
    group_name = EvaluatedMetadataDF.groupby(['Collection','Record', 'Concept'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceMatrix.columns.names = ['']
    pd.options.display.float_format = '{:,.0f}'.format
    #pd.concat([occurrenceMatrix,dialectOccurrenceDF], axis=0, ignore_index=True)
    occurrenceMatrix.to_csv(RAD, mode = 'w', index=False)
    return(occurrenceMatrix)
def XpathCounts(EvaluatedMetadataDF, Organization, Collection, Dialect):
    Xpath='$MetadataEvaluation/data/'+Organization+'/'+Collection+'_'+Dialect+'_XpathCounts.csv'
    group_name = EvaluatedMetadataDF.groupby(['Collection','Record', 'XPath'], as_index=False)
    Xpathdf=group_name.size().unstack().reset_index()
    Xpathdf=Xpathdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    Xpathdf.to_csv(Xpath, mode = 'w', index=False)
    return(Xpathdf)    
    #create a QuickE data product
def QuickEDataProduct(EvaluatedMetadataDF, Organization, Collection, Dialect):
    QuickE='$MetadataEvaluation/data/'+Organization+'/'+Collection+'_'+Dialect+'_QuickE.csv'
    group_name = EvaluatedMetadataDF.groupby(['XPath', 'Record'], as_index=False)
    QuickEdf=group_name.size().unstack().reset_index()
    QuickEdf=QuickEdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    QuickEdf.to_csv(QuickE, mode = 'w', index=False)
    return(QuickEdf)

    #concept occurrence data product
def conceptOccurrence(EvaluatedMetadataDF, Organization, Collection, Dialect):
    Occurrence='$MetadataEvaluation/data/'+Organization+'/'+Collection+'_'+Dialect+'_Occurrence.csv'
    group_name = EvaluatedMetadataDF.groupby(['Record', 'Concept'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceSum=occurrenceMatrix.sum()
    occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(1, 'Collection', Collection+'_'+Dialect)
    result.insert(4, 'CollectionOccurrence%', Collection+'_'+Dialect)
    result.insert(4, 'AverageOccurrencePerRecord', Collection+'_'+Dialect)
    result.columns = ['Concept', 'Collection', 'ConceptCount', 'RecordCount', 'AverageOccurrencePerRecord', 'CollectionOccurrence%' ]
    NumberOfRecords = result.at[0, 'ConceptCount'].count('.xml')
    result['CollectionOccurrence%'] = result['RecordCount']/NumberOfRecords
    result['CollectionOccurrence%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in result['CollectionOccurrence%']], index = result.index)
    result.at[0, 'ConceptCount'] = NumberOfRecords
    result.at[0, 'Concept'] = 'Number of Records'
    result['AverageOccurrencePerRecord'] = result['ConceptCount']/NumberOfRecords
    result['AverageOccurrencePerRecord'] = result['AverageOccurrencePerRecord'].astype(float)
    result[["ConceptCount","RecordCount"]] = result[["ConceptCount","RecordCount"]].astype(int)
    result['AverageOccurrencePerRecord'] = pd.Series(["{0:.2f}".format(val) for val in result['AverageOccurrencePerRecord']], index = result.index)
    result.to_csv(Occurrence, mode = 'w', index=False)
    return(result)
    #xpath occurrence data product
def xpathOccurrence(EvaluatedMetadataDF, Organization, Collection, Dialect):
    XpathOccurrence='$MetadataEvaluation/data/'+Organization+'/'+Collection+'_'+Dialect+'_XPathOccurrence.csv'
    group_name = EvaluatedMetadataDF.groupby(['Record', 'XPath'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceSum=occurrenceMatrix.sum()
    occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(1, 'Collection', Collection+'_'+Dialect)
    result.insert(4, 'CollectionOccurrence%', Collection+'_'+Dialect)
    result.insert(4, 'AverageOccurrencePerRecord', Collection+'_'+Dialect)
    result.columns = ['XPath', 'Collection', 'XPathCount', 'RecordCount', 'AverageOccurrencePerRecord', 'CollectionOccurrence%' ]
    NumberOfRecords = result.at[0, 'XPathCount'].count('.xml')
    result['CollectionOccurrence%'] = result['RecordCount']/NumberOfRecords
    result['CollectionOccurrence%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in result['CollectionOccurrence%']], index = result.index)
    result.at[0, 'XPathCount'] = NumberOfRecords
    result.at[0, 'XPath'] = 'Number of Records'
    result['AverageOccurrencePerRecord'] = result['XPathCount']/NumberOfRecords
    result['AverageOccurrencePerRecord'] = result['AverageOccurrencePerRecord'].astype(float)
    result[["XPathCount","RecordCount"]] = result[["XPathCount","RecordCount"]].astype(int)
    result['AverageOccurrencePerRecord'] = pd.Series(["{0:.2f}".format(val) for val in result['AverageOccurrencePerRecord']], index = result.index)
    result.to_csv(XpathOccurrence, mode = 'w', index=False)
    return(result)


df_known = pd.read_csv('$MetadataEvaluation/raw/$1/$2_dataKnown.csv')
df_all = pd.read_csv('$MetadataEvaluation/raw/$1/$2_dataAll.csv')
df_unknown = df_all[(~df_all.XPath.isin(df_known.XPath))]
df = pd.concat([df_known, df_unknown], axis=0)
df.drop_duplicates(inplace=True)
df.fillna('Unknown', inplace=True)
EvaluatedMetadataDF=df.replace({'Dialect': {'Unknown': '$3'}})

#('Unknown', df.at[1,"Dialect"])


os.makedirs('$MetadataEvaluation/data/$1', exist_ok=True)
EvaluatedMetadataDF.to_csv('$MetadataEvaluation/data/$1/$2_EML_Evaluated.csv.gz',
          index=False,
          compression='gzip', columns=['Collection', 'Dialect', 'Record', 'Concept', 'Content', 'XPath'])
ConceptCounts(EvaluatedMetadataDF, '$1', '$2', '$3')
XpathCounts(EvaluatedMetadataDF, '$1', '$2', '$3')
QuickEDataProduct(EvaluatedMetadataDF, '$1', '$2', '$3')
conceptOccurrence(EvaluatedMetadataDF, '$1', '$2', '$3')
xpathOccurrence(EvaluatedMetadataDF, '$1', '$2', '$3')

CODE


#rm -rvf $MetadataEvaluation/raw/$1