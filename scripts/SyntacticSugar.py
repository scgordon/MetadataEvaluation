import metadataEvaluation
import pandas as pd
import csv
import glob
from contextlib import contextmanager
import os
#create a way to easily move between directories
@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)
#create a way to go through all of the metadata we want to evaluate
def LTERsitesWorkflow(Organization, Collection, Dialect):
#build string variables from variables used in calling the function
	MetadataLocation='../metadata/'+Organization+'/'+Collection+'/'+Dialect+'/xml'
	DataDestination='../data/'+Organization+'/'+Collection+'_'+Dialect+'_XpathOccurrence.csv'
#run function to create csv of selected metadata. CSV only contains results from allNodes.xsl so each row is an element or attribute that has text content in a record for the collection
	metadataEvaluation.localAllNodesEval(MetadataLocation, Organization, Collection, Dialect)
#create DF variable 
	EvaluatedMetadataDF = pd.read_csv('../data/'+Organization+'/'+Collection+'_'+Dialect+'_XpathEvaluated.csv.gz')
	metadataEvaluation.xpathOccurrence(EvaluatedMetadataDF, Organization, Collection, Dialect, DataDestination)

def CombineOrganizationData(Organization):
#identify data to combine
    DirectoryChoice='../data/'+Organization+'/'
#change to chosen directory            
    with cd(DirectoryChoice):
#identify all files of a specific type (occurrence.csv)
        XPathOccurrenceList=glob.glob('*_XpathOccurrence.csv')
#where to put it        
        DataDestination='../'+Organization+'/'+Organization+'_xpathOccurrence.csv'
        XPathOccurrenceList.sort()
#location of all files to combine in a list        
        #XPathOccurrenceList=[DirectoryChoice + s for s in XPathOccurrenceList]
        #get occurrence percentages for each collection
        metadataEvaluation.CombineXPathOccurrence(XPathOccurrenceList, DataDestination)
#identify all files of a specific type (occurrence.csv)
        avgXPathOccurrencePerRecordList=glob.glob('*_XpathOccurrence.csv')
#where to put it                
        DataDestination2='../'+Organization+'/'+Organization+'_AVGxpathOccurrence.csv'
        avgXPathOccurrencePerRecordList.sort()
#location of all files to combine in a list        
        #avgXPathOccurrencePerRecordList=[DirectoryChoice + s for s in avgXPathOccurrencePerRecordList]
        #get occurence per average record
        metadataEvaluation.CombineAverageXPathOccurrencePerRecord(avgXPathOccurrencePerRecordList, DataDestination2)

        metadataEvaluation.OrganizationSpreadsheet(Organization,DataDestination,DataDestination2)
#run workflow for a specific set of metadata collections
#files that drive the script
ListofCollections='./ListofCollections.csv'
ListofOrganizations="./ListOfOrganizations.csv"

#with open(ListofCollections, "r") as f:
 #   reader = csv.reader(f, delimiter=",")
  #  for row in enumerate(reader):
   #     LTERsitesWorkflow(*row[1])
#run combine workflow for a set of organizations

with open(ListofOrganizations, 'r') as f2:
    reader2 = csv.reader(f2, delimiter=',')
    for row in enumerate(reader2):
        CombineOrganizationData(*row[1])

