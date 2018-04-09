import glob
from contextlib import contextmanager
import os
import pandas as pd
import metadataEvaluation

@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)
       
for subdir, dirs, files in os.walk('../data'):
    for dir in dirs:
        with cd('../data/'+dir):
            #print(os.getcwd())
            EvaluatedList=glob.glob('*_Evaluated.csv.gz')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_EvaluatedContent.csv.gz'
            EvaluatedList.sort()
            EvaluatedList=[mystring + s for s in EvaluatedList]
            metadataEvaluation.CombineEvaluatedMetadata(EvaluatedList, DataDestination)

            OccurrenceList=glob.glob('*_Occurrence.csv')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_ConceptOccurrence.csv'
            OccurrenceList.sort()
            OccurrenceList=[mystring + s for s in OccurrenceList]
            metadataEvaluation.CombineConceptOccurrence(OccurrenceList, DataDestination)
            
            RADList=glob.glob('*_Occurrence.csv')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_AverageConceptOccurrencePerRecord.csv'
            RADList.sort()
            RADList=[mystring + s for s in RADList]
            metadataEvaluation.CombineAverageConceptOccurrencePerRecord(RADList, DataDestination)
            
            #QuickEList=glob.glob('*_EML_QuickE.csv')
            #mystring='../../data/'+dir+'/'
            #DataDestination='../../CombinedData/'+dir+'_QuickE.csv'
            #QuickEList.sort()
            #QuickEList=[mystring + s for s in QuickEList]
            #CombineXPathCounts(QuickEList, DataDestination)
            
            XPathCountsList=glob.glob('*_XpathCounts.csv')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_XpathCounts.csv'
            XPathCountsList.sort()
            XPathCountsList=[mystring + s for s in XPathCountsList]
            metadataEvaluation.CombineXPathCounts(XPathCountsList, DataDestination)
            
            XPathOccurrenceList=glob.glob('*_XPathOccurrence.csv')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_XPathOccurrence.csv'
            XPathOccurrenceList.sort()
            XPathOccurrenceList=[mystring + s for s in XPathOccurrenceList]
            metadataEvaluation.CombineXPathOccurrence(XPathOccurrenceList, DataDestination)

            avgXPathOccurrencePerRecordList=glob.glob('*_XPathOccurrence.csv')
            mystring='../../data/'+dir+'/'
            DataDestination='../../CombinedData/'+dir+'_AverageXPathOccurrencePerRecord.csv'
            avgXPathOccurrencePerRecordList.sort()
            avgXPathOccurrencePerRecordList=[mystring + s for s in avgXPathOccurrencePerRecordList]
            metadataEvaluation.CombineAverageXPathOccurrencePerRecord(avgXPathOccurrencePerRecordList, DataDestination)


