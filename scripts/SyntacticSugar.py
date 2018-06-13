import pandas as pd
import csv
import glob
from contextlib import contextmanager
import os
import contextlib

import metadataEvaluation


def MDeval(Organization, Collection, Dialect):
    ''' create a way to go through all of the metadata we want to evaluate
    and transform it into a csv for each site/year combination that exists.
    Two analyses are run, one looks for just Documentation Concepts that have
    been identified. The other identifies each node, element or attribute,
    in the xml that has a text value. The conceptual csv is then reduced to
    just the concepts in the LTER recommendation for Completeness.
    '''
    MetadataLocation = (
        '../metadata/' + Organization + '/' +
        Collection + '/' + Dialect + '/xml'
    )

    metadataEvaluation.localAllNodesEval(
        MetadataLocation, Organization, Collection, Dialect
    )

    metadataEvaluation.localKnownNodesEval(
        MetadataLocation, Organization, Collection, Dialect
    )
    EvaluatedMetadataDF = pd.read_csv(
        '../data/' + Organization + '/' + Collection +
        '_' + Dialect + '_ConceptEvaluated.csv.gz'
    )
    DataDestination = (
        '../data/' + Organization + '/' + Collection +
        '_' + Dialect + '_ConceptEvaluated.csv.gz'
    )
    EvaluatedMetadataDF2 = (
        EvaluatedMetadataDF.loc[EvaluatedMetadataDF['Concept'].isin([
            'Resource Identifier', 'Resource Title',
            'Author / Originator', 'Metadata Contact', 'Contributor Name',
            'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
            'Keyword', 'Resource Distribution', 'Spatial Extent',
            'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
            'Resource Use Constraints', 'Process Step', 'Project Description',
            'Entity Type Definition', 'Attribute Definition',
            'Resource Access Constraints', 'Resource Format', 'Attribute List',
            'Attribute Constraints', 'Resource Quality Description'
        ])])

    metadataEvaluation.EvaluatedDatatable(
        EvaluatedMetadataDF2, DataDestination
    )

@contextmanager
def cd(newdir):
    # create a way to easily move between directories
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


def createSiteReport(Organization, Collection, Dialect):
    '''This function takes the evaluated metadata in csv and runs analysis.
    These analyses are combined for each site and reports are generated in
    Excel. It is then uploaded to the user's Google Drive upon authorization
    and converted to a google sheet with a shareable link for anyone to view
    the results.
    '''
    # build string variables from variables used in calling the function

    DataDestination = (
        '../data/' + Organization + '/' + Collection +
        '_' + Dialect + '_XpathOccurrence.csv'
    )
    EvaluatedXpaths = (
        '../data/' + Organization +
        '/' + Collection + '_' + Dialect +
        '_XpathEvaluated.csv.gz')
    # read in evaluated xpaths, create xpath occurrence data
    EvaluatedMetadataDF = pd.read_csv(EvaluatedXpaths)

    metadataEvaluation.xpathOccurrence(
        EvaluatedMetadataDF, Organization,
        Collection, Dialect, DataDestination
    )

    DataDestination4 = (
        '../data/' + Organization + '/' +
        Collection + '_' + Dialect + '_XpathCounts.csv'
    )

    metadataEvaluation.XpathCounts(
        EvaluatedMetadataDF, Organization,
        Collection, Dialect, DataDestination4
    )

    # create concept occurrence
    EvaluatedConcepts = (
        '../data/' + Organization + '/' + Collection +
        '_' + Dialect + '_ConceptEvaluated.csv.gz'
    )
    EvaluatedMetadataDF2 = pd.read_csv(EvaluatedConcepts)

    DataDestination2 = (
        '../data/' + Organization + '/' +
        Collection + '_' + Dialect + '_ConceptOccurrence.csv'
    )

    DataDestination3 = (
        '../data/' + Organization + '/' +
        Collection + '_' + Dialect + '_ConceptCounts.csv'
    )

    metadataEvaluation.conceptOccurrence(
        EvaluatedMetadataDF2, Organization,
        Collection, Dialect, DataDestination2
    )
    ConceptOccurrenceDF = pd.read_csv(DataDestination2, index_col=0)
    # change order of rows to be meaningful for recommendation
    ConceptOccurrenceDF = ConceptOccurrenceDF.reindex(
        ['Number of Records',
         'Resource Identifier', 'Resource Title',
         'Author / Originator', 'Metadata Contact', 'Contributor Name',
         'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
         'Keyword', 'Resource Distribution', 'Spatial Extent',
         'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
         'Resource Use Constraints', 'Process Step', 'Project Description',
         'Entity Type Definition', 'Attribute Definition',
         'Resource Access Constraints', 'Resource Format', 'Attribute List',
         'Attribute Constraints', 'Resource Quality Description']
    )
    ''' fill blank spaces with the collection columns value of a
    concept that is always present in an EML record
    '''
    collectionFill = ConceptOccurrenceDF.at[
        'Resource Identifier', 'Collection'
    ]
    values = {
        'Collection': collectionFill, 'ConceptCount': 0, 'RecordCount': 0,
        'AverageOccurrencePerRecord': 0.00, 'CollectionOccurrence%': 0.00
    }

    ConceptOccurrenceDF = ConceptOccurrenceDF.fillna(value=values)
    ConceptOccurrenceDF.to_csv(DataDestination2, mode='w')

    metadataEvaluation.conceptCounts(
        EvaluatedMetadataDF2, Organization,
        Collection, Dialect, DataDestination3
    )
    # order columns to reflect recommendation order
    occurrenceMatrix = pd.read_csv(DataDestination3)
    occurrenceMatrix = (occurrenceMatrix[[
        'Collection', 'Record',
        'Resource Identifier', 'Resource Title',
        'Author / Originator', 'Metadata Contact', 'Contributor Name',
        'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
        'Keyword', 'Resource Distribution', 'Spatial Extent',
        'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
        'Resource Use Constraints', 'Process Step', 'Project Description',
        'Entity Type Definition', 'Attribute Definition',
        'Resource Access Constraints', 'Resource Format', 'Attribute List',
        'Attribute Constraints', 'Resource Quality Description'
    ]])
    occurrenceMatrix.to_csv(DataDestination3, mode='w', index=False)

    ReportLocation = (
        '../reports/' + Organization + '/' + Organization +
        '_' + Collection + '_' + Dialect + '_Report.xlsx'
    )

    metadataEvaluation.collectionSpreadsheet(
        Organization, Collection, Dialect,
        EvaluatedConcepts, EvaluatedXpaths,
        DataDestination, DataDestination4,
        DataDestination2, DataDestination3, ReportLocation
    )

    # metadataEvaluation.WriteGoogleSheets(ReportLocation)


def CombineOrganizationData(Organization):
    '''Used to combine all sites through time data in the data directory
    into one report. This report removes counts sheets to remain under the
    size limit for Google Sheets conversion.
    '''
    # identify data to combine
    DirectoryChoice = '../data/' + Organization + '/'
    # change to chosen directory
    with cd(DirectoryChoice):
        # identify all files of a specific type (occurrence.csv)
        XPathOccurrenceList = glob.glob('*_XpathOccurrence.csv')
        ConceptOccurrenceList = glob.glob('*_ConceptOccurrence.csv')
        # where to put it
        DataDestination = (
            '../' + Organization + '/' +
            Organization + '_xpathOccurrence.csv'
        )
        XPathOccurrenceList.sort()
        DataDestination3 = (
            '../' + Organization + '/' +
            Organization + '_conceptOccurrence.csv'
        )
        ConceptOccurrenceList.sort()

        # get occurrence percentages for each collection
        metadataEvaluation.CombineXPathOccurrence(
            XPathOccurrenceList, DataDestination
        )
        metadataEvaluation.CombineConceptOccurrence(
            ConceptOccurrenceList, DataDestination3
        )
        ConceptOccurrenceDF = pd.read_csv(DataDestination3, index_col=0)
        # change order of rows to be meaningful for recommendation
        ConceptOccurrenceDF = ConceptOccurrenceDF.reindex(
            ['Resource Identifier', 'Resource Title',
             'Author / Originator', 'Metadata Contact', 'Contributor Name',
             'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
             'Keyword', 'Resource Distribution', 'Spatial Extent',
             'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
             'Resource Use Constraints', 'Process Step', 'Project Description',
             'Entity Type Definition', 'Attribute Definition',
             'Resource Access Constraints', 'Resource Format',
             'Attribute List', 'Attribute Constraints',
             'Resource Quality Description']
        )
        ''' fill blank spaces with the collection columns value of a
        concept that is always present in an EML record
        '''
        ConceptOccurrenceDF = ConceptOccurrenceDF.fillna(0.00)
        ConceptOccurrenceDF.to_csv(DataDestination3, mode='w')
        # identify all files of a specific type (Counts.csv)
        XPathCountsList = glob.glob('*_XpathCounts.csv')
        ConceptCountsList = glob.glob('*_ConceptCounts.csv')
        # where to put it
        DataDestination6 = (
            '../' + Organization + '/' +
            Organization + '_xpathCounts.csv'
        )
        XPathCountsList.sort()
        DataDestination5 = (
            '../' + Organization + '/' +
            Organization + '_conceptCounts.csv'
        )
        ConceptCountsList.sort()

        # get occurrence percentages for each collection
        metadataEvaluation.CombineXPathCounts(
            XPathCountsList, DataDestination6
        )
        metadataEvaluation.CombineConceptCounts(
            ConceptCountsList, DataDestination5
        )
        occurrenceMatrix = pd.read_csv(DataDestination5)
        occurrenceMatrix = (occurrenceMatrix[[
            'Collection', 'Record',
            'Resource Identifier', 'Resource Title',
            'Author / Originator', 'Metadata Contact', 'Contributor Name',
            'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
            'Keyword', 'Resource Distribution', 'Spatial Extent',
            'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
            'Resource Use Constraints', 'Process Step', 'Project Description',
            'Entity Type Definition', 'Attribute Definition',
            'Resource Access Constraints', 'Resource Format', 'Attribute List',
            'Attribute Constraints', 'Resource Quality Description'
        ]])
        occurrenceMatrix.to_csv(DataDestination5, mode='w', index=False)
        # identify all files of a specific type (occurrence.csv)
        avgXPathOccurrencePerRecordList = glob.glob('*_XpathOccurrence.csv')
        # where to put it
        DataDestination2 = (
            '../' + Organization + '/' + Organization +
            '_AVGxpathOccurrence.csv'
        )
        avgXPathOccurrencePerRecordList.sort()
        avgConceptOccurrencePerRecordList = glob.glob(
            '*_ConceptOccurrence.csv'
        )
        # where to put it
        DataDestination4 = (
            '../' + Organization + '/' + Organization +
            '_AVGconceptOccurrence.csv'
        )
        avgConceptOccurrencePerRecordList.sort()

        # get occurence per average record
        metadataEvaluation.CombineAverageXPathOccurrencePerRecord(
            avgXPathOccurrencePerRecordList, DataDestination2
        )
        metadataEvaluation.CombineAverageConceptOccurrencePerRecord(
            avgConceptOccurrencePerRecordList, DataDestination4
        )

        ConceptOccurrenceDF = pd.read_csv(DataDestination4, index_col=0)
        ConceptOccurrenceDF = ConceptOccurrenceDF.reindex(
            ['Resource Identifier', 'Resource Title',
             'Author / Originator', 'Metadata Contact', 'Contributor Name',
             'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
             'Keyword', 'Resource Distribution', 'Spatial Extent',
             'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
             'Resource Use Constraints', 'Process Step', 'Project Description',
             'Entity Type Definition', 'Attribute Definition',
             'Resource Access Constraints', 'Resource Format',
             'Attribute List', 'Attribute Constraints',
             'Resource Quality Description']
        )
        ''' fill blank spaces with the collection columns value of a
        concept that is always present in an EML record
        '''
        ConceptOccurrenceDF = ConceptOccurrenceDF.fillna(0.00)
        ConceptOccurrenceDF.to_csv(DataDestination4, mode='w')

        metadataEvaluation.OrganizationSpreadsheet(
            Organization, DataDestination, DataDestination2,
            DataDestination3, DataDestination4,
            ConceptCounts=DataDestination5, xpathCounts=DataDestination6
        )
        os.chdir('../../scripts')
        SpreadsheetLocation = (
            '../reports/' + Organization + '/' +
            Organization + '_Report.xlsx'
        )
        metadataEvaluation.WriteGoogleSheets(SpreadsheetLocation)


def CombineLTER(DirectoryChoice):
    DirectoryChoice = ('../' + DirectoryChoice + '/')
    # change to chosen directory
    with cd(DirectoryChoice):
        # identify all files of a specific type (occurrence.csv)
        XPathOccurrenceList = glob.glob(
            '**/*_XpathOccurrence.csv', recursive=True
        )
        ConceptOccurrenceList = glob.glob(
            '**/*_ConceptOccurrence.csv', recursive=True
        )
        # where to put it
        DataDestination = (
            './LTER/LTER_xpathOccurrence.csv'
        )
        XPathOccurrenceList.sort()
        DataDestination3 = (
            './LTER/LTER_conceptOccurrence.csv'
        )
        ConceptOccurrenceList.sort()
        # get occurrence percentages for each collection
        metadataEvaluation.CombineXPathOccurrence(
            XPathOccurrenceList, DataDestination
        )
        metadataEvaluation.CombineConceptOccurrence(
            ConceptOccurrenceList, DataDestination3
        )
        ConceptOccurrenceDF = pd.read_csv(DataDestination3, index_col=0)
        # change order of rows to be meaningful for recommendation
        ConceptOccurrenceDF = ConceptOccurrenceDF.reindex(
            ['Resource Identifier', 'Resource Title',
             'Author / Originator', 'Metadata Contact', 'Contributor Name',
             'Publisher', 'Publication Date', 'Resource Contact', 'Abstract',
             'Keyword', 'Resource Distribution', 'Spatial Extent',
             'Taxonomic Extent', 'Temporal Extent', 'Maintenance',
             'Resource Use Constraints', 'Process Step', 'Project Description',
             'Entity Type Definition', 'Attribute Definition',
             'Resource Access Constraints', 'Resource Format',
             'Attribute List', 'Attribute Constraints',
             'Resource Quality Description']
        )
        ''' fill blank spaces with the collection columns value of a
        concept that is always present in an EML record
        '''
        ConceptOccurrenceDF = ConceptOccurrenceDF.fillna(0.00)
        ConceptOccurrenceDF.to_csv(DataDestination3, mode='w')
        # identify all files of a specific type (occurrence.csv)
        avgXPathOccurrencePerRecordList = glob.glob(
            '**/*_XpathOccurrence.csv', recursive=True
        )
        # where to put it
        DataDestination2 = (
            './LTER/LTER_AVGxpathOccurrence.csv'
        )
        avgXPathOccurrencePerRecordList.sort()
        avgConceptOccurrencePerRecordList = glob.glob(
            '**/*_ConceptOccurrence.csv', recursive=True
        )
        # where to put it
        DataDestination4 = (
            './LTER/LTER_AVGconceptOccurrence.csv'
        )
        avgConceptOccurrencePerRecordList.sort()

        # get occurence per average record
        metadataEvaluation.CombineAverageXPathOccurrencePerRecord(
            avgXPathOccurrencePerRecordList, DataDestination2
        )
        metadataEvaluation.CombineAverageConceptOccurrencePerRecord(
            avgConceptOccurrencePerRecordList, DataDestination4
        )
        Report = 'LTER'
        metadataEvaluation.OrganizationSpreadsheet(
            Report, DataDestination, DataDestination2,
            DataDestination3, DataDestination4
        )
        os.chdir('../scripts')
        SpreadsheetLocation = (
            '../reports/LTER/LTER_Report.xlsx'
        )
        metadataEvaluation.WriteGoogleSheets(SpreadsheetLocation)
# run workflow for a specific set of metadata collections
# files that drive the script


ListofCollections = './ListofCollections.csv'
ListofOrganizations = "./ListOfOrganizations.csv"

# evaluate metadata collections
with open(ListofCollections, "r") as f:
    reader = csv.reader(f, delimiter=",")
    for row in enumerate(reader):
        MDeval(*row[1])

# create reports from transformed metadata
with open(ListofCollections, "r") as f:
    reader = csv.reader(f, delimiter=",")
    for row in enumerate(reader):
        createSiteReport(*row[1])

# run combine workflow for a set of organizations
with open(ListofOrganizations, 'r') as f2:
    reader2 = csv.reader(f2, delimiter=',')
    for row in enumerate(reader2):
        CombineOrganizationData(*row[1])


DirectoryChoice = 'data'
CombineLTER(DirectoryChoice)


with contextlib.suppress(FileNotFoundError):
    os.remove('./mycreds.txt')
