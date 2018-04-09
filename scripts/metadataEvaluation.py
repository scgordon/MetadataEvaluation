#Written by Sean Gordon, Aleksandar Jelenek, and Ted Habermann. 
#Based on the NOAA rubrics Dr Habermann created, and his work 
#conceptualizing the documentation language so that rubrics using 
#recommendations from other earth science communities can be applied
#to multiple metadata dialects as a part of the USGeo BEDI and NSF DIBBs projects.
#This python module as an outcome of DIBBs allows a user to initiate an evaluation of
#valid XML. If it is not a metadata standard that has not been ingested as a 
#documentation language dialect in AllCrosswalks.xml, this XML can be evaluated 
#using the XPath dataframe functions, but will get lumped together 
#as the concept, "Unknown", in any of the concepts based evaluation. 
#Other metadata standards can be conceptualized and added to the Concepts Evaluator
#Then the module can be rebuilt and the recommendations analysis functions can be run
import pandas as pd
import csv
import os
from os import walk
import shutil
import requests
import io
import subprocess
import xlsxwriter
#variables needed to save data 
def localAllNodesEval(MetadataLocation, Organization, Collection, Dialect):
    subprocess.call(["./xmlTransform.sh", Organization, Collection, Dialect])


def XMLeval(MetadataLocation, Organization, Collection, Dialect):
    #eventually replaced with lxml functions
    MetadataDestination=os.path.join('./zip/',Organization,Collection,Dialect,'xml')
    os.makedirs(MetadataDestination, exist_ok=True)
    #os.makedirs(os.path.join('../data',Organization), exist_ok=True)
    src_files = os.listdir(MetadataLocation)
    for file_name in src_files:
        full_file_name = os.path.join(MetadataLocation, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, MetadataDestination)
    shutil.make_archive('./upload/metadata', 'zip', './zip/')


    # Send metadata package, read the response into a dataframe
    url = 'http://metadig.nceas.ucsb.edu/metadata/evaluator'
    files = {'zipxml': open('./upload/metadata.zip', 'rb')}
    r = requests.post(url, files=files, headers={"Accept-Encoding": "gzip"})
    r.raise_for_status()
    EvaluatedMetadataDF = pd.read_csv(io.StringIO(r.text), quotechar='"')

    #Change directories, delete upload directory and zip. Delete copied metadata.

    shutil.rmtree('./upload')

    shutil.rmtree('./zip/')
    
    return(EvaluatedMetadataDF)

#def ExcelRAD(EvaluatedMetadataDF,DataDestination)
#def AddDialectDefinition(***)
#def AddDialect(***)
#def AddRecommendation(****)
#def 
#def JSONeval(****)
#def QualitativeRecommendationsAnalysis(dataframe, RecTag)
#def QuantitativeRecommendationsAnalysis(dataframe, RecTag)
#creates all data products. Likely useful to break up into different functions in the module
def EvaluatedDatatable(EvaluatedMetadataDF, DataDestination):
    EvaluatedMetadataDF.to_csv(DataDestination,
          index=False,
          compression='gzip', columns=['Collection', 'Dialect', 'Record', 'Concept', 'Content', 'XPath'])
    
    return(EvaluatedMetadataDF)

def simpleXPathISO(EvaluatedMetadataDF):
    #Create a simplified XPath output
       #add a column for declaring the collection and dialect to uniquely identify data in combined data products
    #EvaluatedMetadataDF.insert(3, 'Collection', Collection+'_'+Dialect)
    #SimplifiedEvaluated='../data/'+Organization+'/'+Collection+'_'+Dialect+'_EvaluatedSimplified.csv.gz'

    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('/gco:CharacterString', '')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('/[a-z]+:+?', '/')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('/@[a-z]+:+?', '/@')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('/[A-Z]+_[A-Za-z]+/?', '/')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('//', '/')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.rstrip('//')
    #EvaluatedSimplifiedMetadataDF.to_csv(SimplifiedEvaluated, mode = 'w', compression='gzip', index=False)
    return(EvaluatedMetadataDF)

def simpleXPathRe3data(EvaluatedMetadataDF):
#   Create a simplified XPath output for any dataFrame with an XPath column
#   replacements below used to simplify the re3data xPaths.
#   Important to consider a test on root so that one simpleXPath function can get #   used on any data table
#
#   examples
#   /r3d:re3data/r3d:repository/r3d:providerType
#   /r3d:re3data/r3d:repository/r3d:keyword
#   /r3d:re3data/r3d:repository/r3d:institution/r3d:institutionName
#
#   becomes
#   providerType
#   keyword
#   institutionName

    
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('/r3d:re3data/r3d:repository/r3d:', '')
    EvaluatedMetadataDF['XPath']=EvaluatedMetadataDF['XPath'].str.replace('r3d:','')
    
    return(EvaluatedMetadataDF)

    #Create a Recommendations Analysis data product
def ConceptCounts(EvaluatedMetadataDF, Organization, Collection, Dialect, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    RAD='../data/'+Organization+'/'+Collection+'_'+Dialect+'_RAD.csv'
    dialectOccurrenceDF = pd.read_csv('../table/dialectContains.csv')
    dialectOccurrenceDF=dialectOccurrenceDF['MetadataDialect']=='Dialect'
    group_name = EvaluatedMetadataDF.groupby(['Collection','Record', 'Concept'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceMatrix.columns.names = ['']
    pd.options.display.float_format = '{:,.0f}'.format
    pd.concat([occurrenceMatrix,dialectOccurrenceDF], axis=0, ignore_index=True)
    occurrenceMatrix.to_csv(DataDestination, mode = 'w', index=False)
    return(occurrenceMatrix)
def XpathCounts(EvaluatedMetadataDF, Organization, Collection, Dialect, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    Xpath='../data/'+Organization+'/'+Collection+'_'+Dialect+'XpathCounts.csv'
    group_name = EvaluatedMetadataDF.groupby(['Collection','Record', 'XPath'], as_index=False)
    Xpathdf=group_name.size().unstack().reset_index()
    Xpathdf=Xpathdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    Xpathdf.to_csv(DataDestination, mode = 'w', index=False)
    return(Xpathdf)    
    #create a QuickE data product
def QuickEDataProduct(EvaluatedMetadataDF, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    group_name = EvaluatedMetadataDF.groupby(['XPath', 'Record'], as_index=False)
    QuickEdf=group_name.size().unstack().reset_index()
    QuickEdf=QuickEdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    QuickEdf.to_csv(DataDestination, mode = 'w', index=False)
    return(QuickEdf)

    #concept occurrence data product
def conceptOccurrence(EvaluatedMetadataDF, Organization, Collection, Dialect, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
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
    result.to_csv(DataDestination, mode = 'w', index=False)
    return(result)
    #xpath occurrence data product
def xpathOccurrence(EvaluatedMetadataDF, Organization, Collection, Dialect, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    group_name = EvaluatedMetadataDF.groupby(['Record', 'XPath'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceSum=occurrenceMatrix.sum()
    occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(1, 'Collection', Organization+'_'+Collection+'_'+Dialect)
    result.insert(4, 'CollectionOccurrence%', Collection+'_'+Dialect)
    result.insert(4, 'AverageOccurrencePerRecord', Collection+'_'+Dialect)
    result.columns = ['XPath', 'Collection', 'XPathCount', 'RecordCount', 'AverageOccurrencePerRecord', 'CollectionOccurrence%' ]
    NumberOfRecords = result.at[0, 'XPathCount'].count('.xml')
    result['CollectionOccurrence%'] = result['RecordCount']/NumberOfRecords
    
    #result['CollectionOccurrence%'] = pd.Series(["{0:.2f}%".format(val * 100) for val in result['CollectionOccurrence%']], index = result.index)
    result.at[0, 'XPathCount'] = NumberOfRecords
    result.at[0, 'XPath'] = 'Number of Records'
    
    result.at[0, 'CollectionOccurrence%'] = NumberOfRecords
    result['AverageOccurrencePerRecord'] = result['XPathCount']/NumberOfRecords
    result[['AverageOccurrencePerRecord','CollectionOccurrence%']] = result[['AverageOccurrencePerRecord','CollectionOccurrence%']].astype(float)
    result[["XPathCount","RecordCount"]] = result[["XPathCount","RecordCount"]].astype(int)
    result['AverageOccurrencePerRecord'] = pd.Series(["{0:.2f}".format(val) for val in result['AverageOccurrencePerRecord']], index = result.index)
    result.at[0, 'AverageOccurrencePerRecord'] = NumberOfRecords
    result.to_csv(DataDestination, mode = 'w', index=False)
    return(result)

#Using concept occurrence data products, combine them and produce a collection occurrence% table with collections for columns and concepts for rows
def CombineConceptOccurrence(CollectionComparisons, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='Concept', columns='Collection', values='CollectionOccurrence%')
    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF
#Using concept occurrence data products, combine them and produce a record count table with collections for columns and concepts for rows
def CombineConceptCounts(CollectionComparisons, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)    
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
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    #CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='XPath', columns='Collection', values='CollectionOccurrence%')
    

    #pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()
#    ConceptCountsDF.loc[-1] = ConceptCountsDF.head()  # doubledown on header
    #ConceptCountsDF.tail()  # adding the tail in place of original header
  #  ConceptCountsDF.index = ConceptCountsDF.index + 1  # shifting index
   # ConceptCountsDF.sort_index(inplace=True) 
    #ConceptCountsDF[:-1]
    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF
#Using xpath occurrence data products, combine them and produce a record count table with collections for columns and concepts for rows
def CombineXPathCounts(CollectionComparisons, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
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
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
   
    CombinedDF.to_csv(DataDestination, mode = 'w',compression='gzip', index=False)
    return CombinedDF   

#Using concept occurrence data products, combine them and produce a record count table with collections for columns and concepts for rows
def CombineAverageConceptOccurrencePerRecord(CollectionComparisons, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    RecordCountCombinedPivotDF = CombinedDF.pivot(index='Concept', columns='Collection', values='AverageOccurrencePerRecord')
    pd.options.display.float_format = '{:,.0f}'.format
    RecordCountCombinedPivotDF=RecordCountCombinedPivotDF.fillna(0)
    RecordCountCombinedPivotDF.columns.names = ['']
    RecordCountCombinedPivotDF=RecordCountCombinedPivotDF.reset_index()
    RecordCountCombinedPivotDF.to_csv(DataDestination, mode = 'w', index=False)
    return RecordCountCombinedPivotDF
def CombineAverageXPathOccurrencePerRecord(CollectionComparisons, DataDestination):
    DataDestinationDirectory=DataDestination[:DataDestination.rfind('/')+1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons)) 
    #CombinedDF.to_csv(DataDestination, mode = 'w', index=False)
    CombinedPivotDF = CombinedDF.pivot(index='XPath', columns='Collection', values='AverageOccurrencePerRecord')
    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF=CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF=ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode = 'w', index=False)
    return ConceptCountsDF   

def OrganizationSpreadsheet(Organization,xpathOccurrence,AVGxpathOccurrence):
    #create spreadsheet for an organization 
    workbook = xlsxwriter.Workbook(Organization+'_Report.xlsx', {'strings_to_numbers': True})
    cell_format11 = workbook.add_format()
    cell_format11.set_num_format('0.00%')
    cell_format04 = workbook.add_format()
    cell_format04.set_num_format('0.00')
    worksheet = workbook.add_worksheet('OccurrencesAnalysis')
    worksheet.set_column('A:A', 70)
    worksheet.write('A2', 'Number of Records')
    worksheet.write('A3', 'Number of Elements / Attributes')
    worksheet.write('A4', 'Coverage w/r to Repository (CR): number of elements / total number of elements')
    worksheet.write('A5', 'Average Occurrence Rate')
    worksheet.write('A6', 'Repository Completeness: Number of elements /  number of elements in most complete collection in repository')
    worksheet.write('A7', 'Homogeneity: Number >= 1 / Total Number of elements in the collection')
    worksheet.write('A8', 'Partial Elements: Number < 0 and < 1')
    
    ws = workbook.add_worksheet('xpathOccurrence')
    ws.set_column('A:A', 50)
    Reader = csv.reader(open(xpathOccurrence, 'r'), delimiter=',',quotechar='"')
    row_count = 0
    
    for row in Reader:
        for col in range(len(row)):
            ws.write(row_count,col,row[col], cell_format11)
        row_count +=1
    
    ws = workbook.add_worksheet('AVGxpathOccurrence')
    ws.set_column('A:A', 50)
    Reader = csv.reader(open(AVGxpathOccurrence, 'r'), delimiter=',',quotechar='"')
    row_count = 0
    
    for row in Reader:
        for col in range(len(row)):
            ws.write(row_count,col,row[col],cell_format04)
            cell = xlsxwriter.utility.xl_rowcol_to_cell(0, col)
            #cell2 = xlsxwriter.utility.xl_rowcol_to_cell(2, col)
            #colRange = xlsxwriter.utility.xl_range(1,col+1,4500,col+1)
            #colRange2 = xlsxwriter.utility.xl_range(2,1,2,len(row)-1)
            formula = '=xpathOccurrence!'+'%s' % cell
            worksheet.write(0,col,formula)
        for col in range(len(row)-1):  
            cell = xlsxwriter.utility.xl_rowcol_to_cell(0, col)
            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(2, col+1)
            colRange = xlsxwriter.utility.xl_range(1,col+1,4500,col+1)
            colRange2 = xlsxwriter.utility.xl_range(2,1,2,len(row)-1)
            formula2 = '=COUNTIF(xpathOccurrence!'+colRange+',">"&0)'
            worksheet.write(2,col+1,formula2)

            formula3 = '='+cell2+'/COUNTA(xpathOccurrence!'+colRange+')'
            worksheet.write(3,col+1,formula3, cell_format11)

            formula4 = '=SUM(xpathOccurrence!'+colRange+')/'+'%s' % cell2
            worksheet.write(4,col+1,formula4, cell_format11)

            formula5 = '='+'%s' % cell2 +'/MAX('+colRange2+')'
            worksheet.write(5,col+1,formula5, cell_format11)

            formula6 = '=COUNTIF(xpathOccurrence!'+colRange+',">="&1)/'+'%s' % cell2
            worksheet.write(6,col+1,formula6, cell_format11)

            formula7 = '=COUNTIFS(xpathOccurrence!'+colRange+',">"&0,xpathOccurrence!'+colRange+',"<"&1)/'+'%s' % cell2
            worksheet.write(7,col+1,formula7, cell_format11)

            formula1 = '=VLOOKUP("Number of Records",AVGxpathOccurrence!1:1048576,'+str(col+2)+')'
            worksheet.write(1,col+1,formula1)
        row_count +=1
    #######################################################################
    #
    # Create a new scatter chart.
    ws3 = workbook.add_worksheet('2005ComparedWith2006')
    chart1 = workbook.add_chart({'type': 'scatter'})

    # Configure the first series.
    chart1.add_series({
        'name': '=xpathOccurrence!$B$1',
        'categories': '=xpathOccurrence!$A$2:$A$487',
        'values': '=xpathOccurrence!$B$2:$B$487',
    })

    # Configure second series. Note use of alternative syntax to define ranges.
    chart1.add_series({
        'name': '=xpathOccurrence!$B$1',
        'categories': '=xpathOccurrence!$A$2:$A$487',
        'values': '=xpathOccurrence!$C$2:$C$487',
    })

    # Add a chart title and some axis labels.
    chart1.set_title ({'name': 'comparison of AND completeness percentage, from 2005, 2006'})
    chart1.set_x_axis({'name': '2005 Completeness %'})
    chart1.set_y_axis({'name': '2006 Completeness %'})

    # Set an Excel chart style.
    chart1.set_style(11)

    # Insert the chart into the worksheet (with an offset).
    ws3.insert_chart('D2', chart1, {'x_offset': 25, 'y_offset': 10})

    #######################################################################
    workbook.close()      