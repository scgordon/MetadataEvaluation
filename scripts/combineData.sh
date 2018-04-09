# This python script is invoked through a shell with no arguments: 
# /data/bedi/MetadataProcessing/bin/combineCollections.sh
# It takes every directory in /data/bedi/data and creates collection comparisons
# in different tables saved as csv to /data/bedi/data/Combined
#
export PYTHON3=${PYTHON3:-/anaconda/bin/python}

$PYTHON3 <<CODE
import glob
from contextlib import contextmanager
import os
import pandas as pd

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

def CombineConceptOccurrence(CollectionComparisons, DataDestination):
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    #CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='Concept', columns='Collection', values='CollectionOccurrence%')
    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF
#Using concept occurrence data products, combine them and produce a record count table with collections for columns and concepts for rows
def CombineConceptCounts(CollectionComparisons, DataDestination):
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    RecordCountCombinedPivotDF = CombinedDF.pivot(index='Concept', columns='Collection', values='RecordCount')
    pd.options.display.float_format = '{:,.0f}'.format
    RecordCountCombinedPivotDF=RecordCountCombinedPivotDF.fillna(0)
    RecordCountCombinedPivotDF.columns.names = ['']
    RecordCountCombinedPivotDF=RecordCountCombinedPivotDF.reset_index()
    RecordCountCombinedPivotDF.to_csv(DataDestination, mode = 'w', index=False)
    return RecordCountCombinedPivotDF

#Using xpath occurrence data products, combine them and produce a collection occurrence% table with collections for columns and concepts for rows
def CombineXPathOccurrence(CollectionComparisons, DataDestination):
    
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    #CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='XPath', columns='Collection', values='CollectionOccurrence%')
    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF

def CombineAverageXPathOccurrencePerRecord(CollectionComparisons, DataDestination):
    
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    #CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='XPath', columns='Collection', values='AverageOccurrencePerRecord')
    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF

#Using xpath occurrence data products, combine them and produce a record count table with collections for columns and concepts for rows
def CombineXPathCounts(CollectionComparisons, DataDestination):
    #os.makedirs('../data/Combined', exist_ok=True)
    XPathCountCombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons), axis=0, ignore_index=True)
    XPathCountCombinedDF=XPathCountCombinedDF.fillna(0)
    XPathCountCombinedDF.columns.names = ['']

    # get a list of columns
    cols = list(XPathCountCombinedDF)
    
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index('Record')))
    # use ix to reorder
    CombinedXPathCountsDF = XPathCountCombinedDF.loc[:, cols]
    cols2 = list(CombinedXPathCountsDF)
    # move the column to head of list using index, pop and insert
    cols2.insert(0, cols2.pop(cols.index('Collection')))
    # use ix to reorder
    CombinedXPathCountsDF = CombinedXPathCountsDF.loc[:, cols2]
    CombinedXPathCountsDF

    CombinedXPathCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return CombinedXPathCountsDF
 #Using xpath occurrence data products, combine them and produce a collection occurrence% table with collections for columns and concepts for rows
def CombineEvaluatedMetadata(CollectionComparisons, DataDestination):
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
   
    CombinedDF.to_csv(DataDestination, mode = 'w',compression='gzip', index=False)
    return CombinedDF   

DirectoryChoice='/Users/scgordon/MetadataEvaluation/data/'+'$1'+'/'
        
with cd(DirectoryChoice):
            
    EvaluatedList=glob.glob('*_Evaluated.csv.gz')
    mystring=DirectoryChoice
    DataDestination='/Users/scgordon/MetadataEvaluation/data/CombinedData/'+'$1'+'_EvaluatedContent.csv.gz'
    EvaluatedList.sort()
    EvaluatedList=[mystring + s for s in EvaluatedList]
    CombineEvaluatedMetadata(EvaluatedList, DataDestination)

    OccurrenceList=glob.glob('*_Occurrence.csv')
    DataDestination='/Users/scgordon/MetadataEvaluation/data/CombinedData/'+'$1'+'_ConceptOccurrence.csv'
    OccurrenceList.sort()
    OccurrenceList=[mystring + s for s in OccurrenceList]
    CombineConceptOccurrence(OccurrenceList, DataDestination)
    
    #RADList=glob.glob('*_RAD.csv')
    #mystring='../../data/'+'$1'+'/'
    #DataDestination='../../CombinedData/'+'$1'+'_ConceptCounts.csv'
    #RADList.sort()
    #RADList=[mystring + s for s in RADList]
    #CombineConceptCounts(RADList, DataDestination)
    
    #QuickEList=glob.glob('*_QuickE.csv')
    #mystring='../../data/'+'$1'+'/'
    #DataDestination='../../CombinedData/'+'$1'+'_QuickE.csv'
    #QuickEList.sort()
    #QuickEList=[mystring + s for s in QuickEList]
    #CombineXPathCounts(QuickEList, DataDestination)
    
    XPathCountsList=glob.glob('*_XpathCounts.csv')
    DataDestination='/Users/scgordon/MetadataEvaluation/data/CombinedData/'+'$1'+'_XpathCounts.csv'
    XPathCountsList.sort()
    XPathCountsList=[mystring + s for s in XPathCountsList]
    CombineXPathCounts(XPathCountsList, DataDestination)
    
    XPathOccurrenceList=glob.glob('*_XPathOccurrence.csv')
    DataDestination='/Users/scgordon/MetadataEvaluation/data/CombinedData/'+'$1'+'_XPathOccurrence.csv'
    XPathOccurrenceList.sort()
    XPathOccurrenceList=[mystring + s for s in XPathOccurrenceList]
    CombineXPathOccurrence(XPathOccurrenceList, DataDestination)

    CombineAverageXPath=glob.glob('*_XPathOccurrence.csv')
    DataDestination='/Users/scgordon/MetadataEvaluation/data/CombinedData/'+'$1'+'_AverageXPathOccurrencePerRecord.csv'
    CombineAverageXPath.sort()
    CombineAverageXPath=[mystring + s for s in CombineAverageXPath]
    CombineAverageXPathOccurrencePerRecord(CombineAverageXPath, DataDestination)

CODE