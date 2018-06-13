"""Written by Sean Gordon, Aleksandar Jelenek, and Ted Habermann.
Based on the NOAA rubrics Dr Habermann created, and his work
conceptualizing the documentation language so that rubrics using
recommendations from other earth science communities can be applied
to multiple metadata dialects as a part of the USGeo BEDI and
NSF DIBBs projects. This python module as an outcome of DIBBs allows
a user to initiate an evaluation of valid XML. If it is not a metadata
standard that has not been ingested as a documentation language dialect
in AllCrosswalks.xml, this XML can be evaluated using the XPath dataframe
functions, but will get lumped together as the concept, "Unknown",
in any of the concepts based evaluation. Other metadata standards can be
conceptualized and added to the Concepts Evaluator. Then the module can be
rebuilt and the recommendations analysis functions can be run

Basic workflow is to retrieve records, evaluate for concept and xpath content,
run concept/xpath counts and occurrence functions on csv output of evaluation,
create collectionspreadsheet with the outputs, if you want to compare between
collections, combine csv outputs with appropriate combination functions, create
organizationSpreadsheet. Finally run WriteGoogleSheets on any xlsx outputs
you want to share
"""


import pandas as pd
import csv
import gzip
import os
import shutil
import requests
import io
import subprocess
import xlsxwriter
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from lxml import etree
import sys

csv.field_size_limit(sys.maxsize)


# function to download metadata


def get_records(urls, xml_files, well_formed=True):
    """Download metadata records. Metadata records are download from the
    supplied ``urls`` and stored in files whose names are found on
    ``xml_files``. When ``well_formed`` is ``True`` downloaded XML will
    be saved to a file only if well-formed.
    """
    """ if we used a function
    like this to collect xml, it would be the root of any processing steps
    """
    if len(urls) != len(xml_files):
        raise ValueError('Different number of URLs and record file names')

    for url, fname in zip(urls, xml_files):
        try:
            r = requests.get(url)
            r.raise_for_status()
        except Exception:
            print('There was an error downloading from {}'.format(url))

        if well_formed:
            try:
                etree.fromstring(r.text)
            except Exception:
                print('Metadata record from {} not well-formed'.format(url))

        if fname[-4:] != '.xml':
            fname += '.xml'
        with open(fname, 'wt') as f:
            f.write(r.text)


def localAllNodesEval(MetadataLocation, Organization, Collection, Dialect):
    """an easy way to control transform of the records
    that you already have, whether you used get_records
    or not, the output is prepared for the xpath functions workflow
    any function with xpath in its name requires the output
    of this function including:XMLeval, XpathCounts, QuickEDataProduct,
    xpathOccurrence. These functions are required to use combine
    xpath functions: CombineXPathOccurrence, CombineXPathCounts,
    CombineAverageXPathOccurrencePerRecord. collectionSpreadsheet
    and OrganizationSpreadsheet require the before mentioned functions
    as well as the concept and combine concept functions.
    """
    subprocess.call(["./xmlTransform.sh", Organization, Collection,
                     Dialect])


def localKnownNodesEval(MetadataLocation, Organization, Collection, Dialect):
    """an easy way to control transform of the records
    that you already have, whether you used get_records
    or not, the output is prepared for the concept functions workflow
    this is the more accurate and lightweight way
    to query the collection for concepts
    """
    subprocess.call(["./conceptTransform.sh", Organization, Collection,
                     Dialect])


def XMLeval(MetadataLocation, Organization, Collection, Dialect):
    # eventually replaced with lxml functions
    """use vm to get old style combine
    and fill nans with 'unknown' the output of
    this function can be used with the concept functions
    I would not use unless approximate numbers were enough
    and I was an anonymous user. Should change on vm...
    """
    MetadataDestination = os.path.join('./zip/', Organization,
                                       Collection, Dialect, 'xml')
    os.makedirs(MetadataDestination, exist_ok=True)
    # os.makedirs(os.path.join('../data',Organization), exist_ok=True)
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

    """Change directories, delete upload directory and zip.
    Delete copied metadata.
    """

    shutil.rmtree('./upload')

    shutil.rmtree('./zip/')

    return(EvaluatedMetadataDF)

# def ExcelRAD(EvaluatedMetadataDF,DataDestination)
# def AddDialectDefinition(***)
# def AddDialect(***)
# def AddRecommendation(****)
# def JSONeval(****)
# def QualitativeRecommendationsAnalysis(dataframe, RecTag)
# def QuantitativeRecommendationsAnalysis(dataframe, RecTag)


def EvaluatedDatatable(EvaluatedMetadataDF, DataDestination):
    """Use if you wanted to create a csv of simplified xpaths
    instead of full. not recommended because the spreadsheet offers a
    simplified and full view of the xpaths
    """
    EvaluatedMetadataDF.to_csv(
        DataDestination,
        index=False, compression='gzip',
        columns=[
            'Collection', 'Dialect', 'Record',
            'Concept', 'XPath', 'Content'
        ])

    return(EvaluatedMetadataDF)


"""Optional simplify functions,
probably better to let the spreadsheet create the simplified element view
"""


def simpleXPathISO(EvaluatedMetadataDF):
    """ Create a simplified XPath output add a column for declaring
    the collection and dialect to uniquely identify data in combined data
    products. Used after XMLeval, localAllNodes or localKnownNodes
    """

    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '/gco:CharacterString', '')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '/[a-z]+:+?', '/')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '/@[a-z]+:+?', '/@')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '/[A-Z]+_[A-Za-z]+/?', '/')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '//', '/')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.rstrip(
        '//')
    return(EvaluatedMetadataDF)


def simpleXPathRe3data(EvaluatedMetadataDF):
    """Create a simplified XPath output for any dataFrame with an XPath column
replacements below used to simplify the re3data xPaths.
Important to consider a test on root so that one simpleXPath function
can get used on any data table

examples
/r3d:re3data/r3d:repository/r3d:providerType
/r3d:re3data/r3d:repository/r3d:keyword
/r3d:re3data/r3d:repository/r3d:institution/r3d:institutionName

becomes
providerType
keyword
institutionName
"""
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        '/r3d:re3data/r3d:repository/r3d:', '')
    EvaluatedMetadataDF['XPath'] = EvaluatedMetadataDF['XPath'].str.replace(
        'r3d:', '')

    return(EvaluatedMetadataDF)


# Create a Recommendations Analysis data product


def conceptCounts(EvaluatedMetadataDF, Organization, Collection,
                  Dialect, DataDestination):
    """requires a dataframe with concepts DF Can created be localKnownNodesEval,
    XMLeval, or a simpleXpath. It is required for combineConceptCounts
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    dialectOccurrenceDF = pd.read_csv('./dialectContains.csv')
    dialectOccurrenceDF = (dialectOccurrenceDF[
        dialectOccurrenceDF['Concept'] == Dialect])
    group_name = EvaluatedMetadataDF.groupby([
        'Collection', 'Record', 'Concept'], as_index=False)
    occurrenceMatrix = group_name.size().unstack().reset_index()
    occurrenceMatrix = occurrenceMatrix.fillna(0)
    occurrenceMatrix.columns.names = ['']
    occurrenceMatrix = pd.concat(
        [dialectOccurrenceDF, occurrenceMatrix], axis=0, ignore_index=True)
    mid = occurrenceMatrix['Collection']
    mid2 = occurrenceMatrix['Record']
    occurrenceMatrix.drop(
        labels=['Collection', 'Record', 'Concept'], axis=1, inplace=True)
    occurrenceMatrix.insert(0, 'Collection', mid)
    occurrenceMatrix.insert(0, 'Record', mid2)

    dialectOccurrenceDF = pd.read_csv('./dialectContains.csv')
    dialectOccurrenceDF = (
        dialectOccurrenceDF[dialectOccurrenceDF['Concept'] == Dialect])
    FILLvalues = dialectOccurrenceDF.to_dict('records')
    FILLvalues = FILLvalues[0]
    occurrenceMatrix = occurrenceMatrix.fillna(value=FILLvalues)
    occurrenceMatrix.reset_index()
    occurrenceMatrix = occurrenceMatrix.drop(occurrenceMatrix.index[0])
    occurrenceMatrix.to_csv(DataDestination, mode='w', index=False)
    return(occurrenceMatrix)


def XpathCounts(EvaluatedMetadataDF, Organization, Collection, Dialect,
                DataDestination):
    """XpathCounts requires a dataframe with xpath.The DF
    can created be localAllNodesEval, XMLeval(not accurate), or
    a simpleXpath. It is required for combineXpathCounts"""
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    group_name = (EvaluatedMetadataDF.groupby(
        ['Collection', 'Record', 'XPath'], as_index=False))
    Xpathdf = group_name.size().unstack().reset_index()
    Xpathdf = Xpathdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    Xpathdf.to_csv(DataDestination, mode='w', index=False)
    return(Xpathdf)


def conceptOccurrence(EvaluatedMetadataDF, Organization,
                      Collection, Dialect, DataDestination):
    # concept occurrence data product
    """requires a dataframe with concepts DF.
    Can created be localKnownNodesEval,XMLeval(inaccurate), or a
    simpleXpath function/ EvaluatedDatatable. It is required
    for combineConceptOccurrence
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    group_name = (EvaluatedMetadataDF.groupby(
        ['Record', 'Concept'], as_index=False))
    occurrenceMatrix = group_name.size().unstack().reset_index()
    occurrenceMatrix = occurrenceMatrix.fillna(0)
    occurrenceSum = occurrenceMatrix.sum()
    occurrenceCount = occurrenceMatrix[occurrenceMatrix != 0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(
        1, 'Collection', Organization + '_' + Collection + '_' + Dialect
    )
    result.insert(4, 'CollectionOccurrence%', Collection + '_' + Dialect)
    result.insert(4, 'AverageOccurrencePerRecord', Collection + '_' + Dialect)
    result.columns = [
        'Concept', 'Collection', 'ConceptCount', 'RecordCount',
        'AverageOccurrencePerRecord', 'CollectionOccurrence%'
    ]
    NumberOfRecords = result.at[0, 'ConceptCount'].count('.xml')
    result['CollectionOccurrence%'] = result['RecordCount'] / NumberOfRecords
    result.at[0, 'ConceptCount'] = NumberOfRecords
    result.at[0, 'Concept'] = 'Number of Records'
    result['AverageOccurrencePerRecord'] = (
        result['ConceptCount'] / NumberOfRecords)
    result['AverageOccurrencePerRecord'] = (
        result['AverageOccurrencePerRecord'].astype(float))
    result[["ConceptCount", "RecordCount"]] = (
        result[["ConceptCount", "RecordCount"]].astype(int))
    result['AverageOccurrencePerRecord'] = pd.Series(
        ["{0:.2f}".format(val) for val in result[
            'AverageOccurrencePerRecord'
        ]], index=result.index)
    result.to_csv(DataDestination, mode='w', index=False)
    return(result)


def xpathOccurrence(EvaluatedMetadataDF, Organization, Collection,
                    Dialect, DataDestination):
    # xpath occurrence data product
    """requires a list of xpathOccurrence csv.
    It is required for OrganizationSpreadsheet
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    group_name = EvaluatedMetadataDF.groupby(
        ['Record', 'XPath'], as_index=False)
    occurrenceMatrix = group_name.size().unstack().reset_index()
    occurrenceMatrix = occurrenceMatrix.fillna(0)
    occurrenceSum = occurrenceMatrix.sum()
    occurrenceCount = occurrenceMatrix[occurrenceMatrix != 0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(
        1, 'Collection', Organization + '_' + Collection + '_' + Dialect)
    result.insert(4, 'CollectionOccurrence%', Collection + '_' + Dialect)
    result.insert(4, 'AverageOccurrencePerRecord', Collection + '_' + Dialect)
    result.columns = [
        'XPath', 'Collection', 'XPathCount', 'RecordCount',
        'AverageOccurrencePerRecord', 'CollectionOccurrence%'
    ]
    NumberOfRecords = result.at[0, 'XPathCount'].count('.xml')
    result['CollectionOccurrence%'] = result['RecordCount'] / NumberOfRecords
    result.at[0, 'XPathCount'] = NumberOfRecords
    result.at[0, 'XPath'] = 'Number of Records'
    result.at[0, 'CollectionOccurrence%'] = NumberOfRecords
    result['AverageOccurrencePerRecord'] = (
        result['XPathCount'] / NumberOfRecords)
    result[['AverageOccurrencePerRecord', 'CollectionOccurrence%']] = (
        result[['AverageOccurrencePerRecord',
                'CollectionOccurrence%']].astype(float)
    )
    result[["XPathCount", "RecordCount"]] = (
        result[["XPathCount", "RecordCount"]].astype(int)
    )
    result['AverageOccurrencePerRecord'] = (pd.Series([
        "{0:.2f}".format(val) for val in result['AverageOccurrencePerRecord']
    ], index=result.index))
    result.at[0, 'AverageOccurrencePerRecord'] = NumberOfRecords
    result.to_csv(DataDestination, mode='w', index=False)
    return(result)


def CombineConceptOccurrence(CollectionComparisons, DataDestination):
    """Using concept occurrence data products, combine them and produce a
    collection occurrence% table with collections for columns and concepts
    for rows CombineConceptOccurrence requires a list of Concept Occurrence
    csv. It is required for OrganizationSpreadsheet
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedDF = CombinedDF.reset_index()
    CombinedPivotDF = CombinedDF.pivot_table(
        index='Concept', columns='Collection', values='CollectionOccurrence%')
    CombinedPivotDF = CombinedPivotDF.drop(['Number of Records'])
    ConceptCountsDF = CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF = ConceptCountsDF.reset_index()
    ConceptCountsDF.to_csv(DataDestination, mode='w', index=False)
    return ConceptCountsDF


def CombineConceptCounts(CollectionComparisons, DataDestination):
    """Using concept occurrence data products, combine them and produce
    a record count table with collections for columns and concepts for rows
    CombineConceptCounts requires a list of Concept Counts csv.
    It is required for OrganizationSpreadsheet
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedDF.to_csv(DataDestination, mode='w', index=False)
    return CombinedDF


def CombineXPathOccurrence(CollectionComparisons, DataDestination):
    """Using xpath occurrence data products, combine them and produce a
    collection occurrence% table with collections for columns and
    concepts for rows requires a list of xpathOccurrence csv.
    It is required for OrganizationSpreadsheet
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedPivotDF = CombinedDF.pivot(
        index='XPath', columns='Collection', values='CollectionOccurrence%')

    ConceptCountsDF = CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF = ConceptCountsDF.reset_index()
    ConceptCountsDF.to_csv(DataDestination, mode='w', index=False)
    return ConceptCountsDF


def CombineXPathCounts(CollectionComparisons, DataDestination):
    """Using xpath occurrence data products, combine them and produce a
    record count table with collections for columns and concepts for rows
    requires a list of xpath counts csv. It is required for
    OrganizationSpreadsheet
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    XPathCountCombinedDF = pd.concat(
        (pd.read_csv(f) for f in CollectionComparisons),
        axis=0, ignore_index=True)
    XPathCountCombinedDF = XPathCountCombinedDF.fillna(0)
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

    CombinedXPathCountsDF.to_csv(DataDestination, mode='w', index=False)
    return CombinedXPathCountsDF


def CombineEvaluatedMetadata(CollectionComparisons, DataDestination):
    """Using xpath occurrence data products, combine them and produce a
    collection occurrence% table with collections for columns and concepts
    for rows.Combines csv output of localAllNodesEval, XMLeval(inaccurate),
    localKnownNodes(inaccurate), simpleXpath/EvaluatedDatatable function.
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedDF.to_csv(
        DataDestination, mode='w', compression='gzip', index=False)
    return CombinedDF


def CombineAverageConceptOccurrencePerRecord(
        CollectionComparisons, DataDestination):
    """Using concept occurrence data products, combine them
    and produce a record count table with collections for columns and
    concepts for rows. It is required for OrganizationSpreadsheet.
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedDF = CombinedDF.reset_index()
    RecordCountCombinedPivotDF = CombinedDF.pivot(
        index='Concept', columns='Collection',
        values='AverageOccurrencePerRecord')
    RecordCountCombinedPivotDF = RecordCountCombinedPivotDF.drop(
        ['Number of Records']
    )
    pd.options.display.float_format = '{:,.0f}'.format
    RecordCountCombinedPivotDF = RecordCountCombinedPivotDF.fillna(0)
    RecordCountCombinedPivotDF.columns.names = ['']
    RecordCountCombinedPivotDF = RecordCountCombinedPivotDF.reset_index()
    RecordCountCombinedPivotDF.to_csv(DataDestination, mode='w', index=False)
    return RecordCountCombinedPivotDF


def CombineAverageXPathOccurrencePerRecord(
        CollectionComparisons, DataDestination):
    """Using concept occurrence data products, combine them
    and produce a record count table with collections for columns and
    concepts for rows. It is required for OrganizationSpreadsheet.
    """
    DataDestinationDirectory = DataDestination[:DataDestination.rfind('/') + 1]
    os.makedirs(DataDestinationDirectory, exist_ok=True)
    CombinedDF = pd.concat((pd.read_csv(f) for f in CollectionComparisons))
    CombinedPivotDF = CombinedDF.pivot(
        index='XPath', columns='Collection',
        values='AverageOccurrencePerRecord')

    pd.options.display.float_format = '{:,.0f}'.format
    ConceptCountsDF = CombinedPivotDF.fillna(0)
    ConceptCountsDF.columns.names = ['']
    ConceptCountsDF = ConceptCountsDF.reset_index()

    ConceptCountsDF.to_csv(DataDestination, mode='w', index=False)
    return ConceptCountsDF


def collectionSpreadsheet(Organization, Collection, Dialect,
                          EvaluatedConcepts, EvaluatedXpaths,
                          xpathOccurrence, xpathCounts,
                          conceptOccurrence, conceptCounts,
                          DataDestination):
    # create spreadsheet for an collection
    """requires xpath and concept occurrence,
    as well as the concept counts csv for a collection
    """
    os.makedirs('../reports/' + Organization, exist_ok=True)
    workbook = xlsxwriter.Workbook(
        DataDestination, {'strings_to_numbers': True})
    cell_format11 = workbook.add_format()
    cell_format11.set_num_format('0%')
    cell_format04 = workbook.add_format()
    cell_format04.set_num_format('0')
    cell_format05 = workbook.add_format()
    cell_format05.set_num_format('0.00')
    formatGreen = workbook.add_format({'bg_color': '#C6EFCE',
                                       'font_color': '#006100'})
    formatRed = workbook.add_format({'bg_color': '#FFC7CE',
                                     'font_color': '#9C0006'})
    formatYellow = workbook.add_format({'bg_color': '#FFEB9C',
                                        'font_color': '#9C6500'})
    ConceptOccurrence = workbook.add_worksheet('ConceptOccurrence')
    ConceptCounts = workbook.add_worksheet('ConceptCounts')
    XpathOccurrence = workbook.add_worksheet('XpathOccurrence')
    XpathOccurrence.set_column('A:A', 70)
    ConceptOccurrence.set_column('A:A', 15)
    ConceptCounts.set_column('A:OD', 15)
    XpathCounts = workbook.add_worksheet('XpathCounts')
    XpathCounts.set_column('A:OD', 15)
    ConceptContent = workbook.add_worksheet('ConceptContent')
    XpathContent = workbook.add_worksheet('XpathContent')

    with gzip.open(EvaluatedXpaths, "rt", newline="") as f:
        Reader = csv.reader(f, delimiter=',', quotechar='"')
        row_count = 0

        for row in Reader:
            for col in range(len(row)):
                XpathContent.write(row_count, col, row[col])
            row_count += 1
    with gzip.open(EvaluatedConcepts, "rt", newline="") as f:
        Reader = csv.reader(f, delimiter=',', quotechar='"')
        row_count = 0

        for row in Reader:
            for col in range(len(row)):
                ConceptContent.write(row_count, col, row[col])
            row_count += 1

    # create a worksheet from the concept occurrence csv
    Reader = csv.reader(open(
        conceptOccurrence, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            ConceptOccurrence.write(row_count, col, row[col])
        row_count += 1
    Reader = csv.reader(open(
        conceptOccurrence, 'r'), delimiter=',', quotechar='"')
    absRowCount = sum(1 for row in Reader)

    ConceptOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})
    ConceptOccurrence.set_column('F:F', 20, cell_format11)
    XpathOccurrence.set_column('F:F', 20, cell_format11)

    ConceptOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})
    ConceptOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    Reader = csv.reader(open(
        xpathOccurrence, 'r'), delimiter=',', quotechar='"')

    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            XpathOccurrence.write(row_count, col, row[col])

        row_count += 1
    Reader = csv.reader(open(
        xpathOccurrence, 'r'), delimiter=',', quotechar='"')
    absRowCount = sum(1 for row in Reader)

    XpathOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})

    XpathOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})

    XpathOccurrence.conditional_format(
        2, 5, absRowCount - 1, 5,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})

    Reader = csv.reader(open(
        conceptCounts, 'r'), delimiter=',', quotechar='"'
    )
    absRowCount = sum(1 for row in Reader)
    absColCount = len(next(csv.reader(
        open(conceptCounts, 'r'), delimiter=',', quotechar='"'
    )))

    ConceptCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})

    ConceptCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    ConceptCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})

    Reader = csv.reader(open(
        xpathCounts, 'r'), delimiter=',', quotechar='"'
    )
    absRowCount = sum(1 for row in Reader)
    absColCount = len(next(csv.reader(
        open(xpathCounts, 'r'), delimiter=',', quotechar='"'
    )))

    XpathCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})

    XpathCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    XpathCounts.conditional_format(
        1, 2, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})

    Reader = csv.reader(open(
        conceptCounts, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            ConceptCounts.write(row_count, col, row[col])

        row_count += 1

    Reader = csv.reader(open(
        xpathCounts, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            XpathCounts.write(row_count, col, row[col])

        row_count += 1

    XpathCounts.autofilter(0, 0, 0, 500)
    XpathOccurrence.autofilter(0, 0, 0, 5)
    ConceptCounts.autofilter(0, 0, 0, 500)
    ConceptOccurrence.autofilter(0, 0, 0, 5)
    workbook.close()


def OrganizationSpreadsheet(Organization, xpathOccurrence,
                            AVGxpathOccurrence, conceptOccurrence,
                            AVGconceptOccurrence,
                            *args, **kwargs):
    # create spreadsheet for an organization
    """requires each xpath and concept occurrence,
    csv for a organization
    (or any group of collections you want to compare)
    """

    ConceptCounts = kwargs.get('ConceptCounts', None)
    xpathCounts = kwargs.get('xpathCounts', None)

    if ConceptCounts is not None and xpathCounts is not None:
        os.makedirs('../../reports/' + Organization, exist_ok=True)
        workbook = xlsxwriter.Workbook(
            '../../reports/' + Organization + '/' + Organization +
            '_Report.xlsx',
            {'strings_to_numbers': True})
    else:
        os.makedirs('../reports/' + Organization, exist_ok=True)
        workbook = xlsxwriter.Workbook(
            '../reports/' + Organization + '/' + Organization +
            '_Report.xlsx',
            {'strings_to_numbers': True})
    workbook.use_zip64()
    cell_format11 = workbook.add_format()
    cell_format11.set_num_format('0%')
    cell_format04 = workbook.add_format()
    cell_format04.set_num_format('0')
    cell_format05 = workbook.add_format()
    cell_format05.set_num_format('0.00')

    formatGreen = workbook.add_format(
        {'bg_color': '#C6EFCE', 'font_color': '#006100'})
    formatRed = workbook.add_format(
        {'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
    formatYellow = workbook.add_format(
        {'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

    ConceptAnalysisWS = workbook.add_worksheet('ConceptOccurrenceAnalysis')
    conceptOccurrenceWS = workbook.add_worksheet('ConceptOccurrence')
    avgConceptOccurWS = workbook.add_worksheet('AVGconceptOccurrence')
    if ConceptCounts is not None:
        conceptcounts = workbook.add_worksheet('ConceptCounts')
    # ws5 = workbook.add_worksheet('Completeness vs Homogeneity')
    XpathAnalysisWS = workbook.add_worksheet('XpathOccurrenceAnalysis')
    xpathoccurrenceWS = workbook.add_worksheet('XpathOccurrence')
    avgXpathOccurWS = workbook.add_worksheet('AVGxpathOccurrence')
    if xpathCounts is not None:
        xpathcounts = workbook.add_worksheet('XpathCounts')
    XpathAnalysisWS.set_column('A:A', 70)
    XpathAnalysisWS.set_column('B:B', 20)
    ConceptAnalysisWS.set_column('A:A', 70)
    ConceptAnalysisWS.set_column('B:B', 20)

    conceptOccurrenceWS.set_column('A:A', 50)

    # create a worksheet from the cncept occurrence csv
    Reader = csv.reader(
        open(conceptOccurrence, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            conceptOccurrenceWS.write(row_count, col, row[col], cell_format11)
        row_count += 1

    xpathoccurrenceWS.set_column('A:A', 50)

    def skip_last(iterator):
        prev = next(iterator)
        for item in iterator:
            yield prev
            prev = item
    Reader = skip_last(
        csv.reader(open(xpathOccurrence, 'r'), delimiter=',', quotechar='"'))

    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            xpathoccurrenceWS.write(row_count, col, row[col], cell_format11)
        for col in range(1, len(row)):
            XpathAnalysisWS.write(
                row_count + 9, col + 4, row[col], cell_format11
            )

        for col in range(0, 1):
            XpathAnalysisWS.write(row_count + 9, col, row[col], cell_format11)

            Xpathcell = xlsxwriter.utility.xl_rowcol_to_cell(row_count + 9, 0)
            formulaElementSimplifier = (
                '=MID(' + Xpathcell +
                ',1+FIND("|",SUBSTITUTE(' + Xpathcell +
                ',"/","|",LEN(' + Xpathcell + ')-LEN(SUBSTITUTE(' +
                Xpathcell + ',"/","")))),100)'
            )
            XpathAnalysisWS.write(
                row_count + 9, col + 1, formulaElementSimplifier, cell_format11
            )
        row_count += 1

    avgXpathOccurWS.set_column('A:A', 50)
    avgConceptOccurWS.set_column('A:A', 30)
    Reader = csv.reader(
        open(AVGconceptOccurrence, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            avgConceptOccurWS.write(row_count, col, row[col], cell_format05)
        row_count += 1

    Reader = csv.reader(
        open(AVGxpathOccurrence, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row)):
            avgXpathOccurWS.write(row_count, col, row[col], cell_format05)

        for col in range(len(row) - 1):

            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)
            cell3 = xlsxwriter.utility.xl_rowcol_to_cell(2, col + 5)
            colRange = xlsxwriter.utility.xl_range(1, col + 1, 500, col + 1)
            colRange2 = xlsxwriter.utility.xl_range(2, 5, 2, len(row) + 3)

            formula2 = '=COUNTIF(xpathOccurrence!' + colRange + ',">"&0)'
            XpathAnalysisWS.write(2, col + 5, formula2)

            formula3 = (
                '=' + cell3 + '/COUNTA(xpathOccurrence!' + colRange + ')')
            XpathAnalysisWS.write(3, col + 5, formula3, cell_format11)

            formula4 = '=SUM(xpathOccurrence!' + colRange + ')/' + '%s' % cell3
            XpathAnalysisWS.write(4, col + 5, formula4, cell_format11)

            formula5 = '=' + '%s' % cell3 + '/MAX(' + colRange2 + ')'
            XpathAnalysisWS.write(5, col + 5, formula5, cell_format11)

            formula6 = (
                '=COUNTIF(xpathOccurrence!' +
                colRange + ',">="&1)/' + '%s' % cell3
            )
            XpathAnalysisWS.write(6, col + 5, formula6, cell_format11)

            formula7 = (
                '=COUNTIFS(xpathOccurrence!' +
                colRange + ',">"&0,xpathOccurrence!' +
                colRange + ',"<"&1)/' + '%s' % cell3
            )
            XpathAnalysisWS.write(7, col + 5, formula7, cell_format11)

            formula1 = (
                '=VLOOKUP("Number of Records",AVGxpathOccurrence!1:1048576,' +
                str(col + 2) + ')'
            )
            XpathAnalysisWS.write(1, col + 5, formula1)

            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)

            formula = '=xpathOccurrence!' + '%s' % cell2
            XpathAnalysisWS.write(0, col + 5, formula)
            dateFormula = (
                '=LEFT(RIGHT(xpathOccurrence!' + '%s' % cell2 +
                ',LEN(xpathOccurrence!' + '%s' % cell2 +
                ')-FIND("_", xpathOccurrence!' +
                '%s' % cell2 + ')),FIND("_",xpathOccurrence!' +
                '%s' % cell2 + '))'
            )
            XpathAnalysisWS.write(8, col + 5, dateFormula)
            collectFormula = (
                '=LEFT(xpathOccurrence!' + '%s' % cell2 +
                ',FIND("_",xpathOccurrence!' + '%s' % cell2 + ')-1)'
            )

            XpathAnalysisWS.write(9, col + 5, collectFormula)

        row_count += 1
    #######################################################################
    #
    if ConceptCounts is not None:
        Reader = csv.reader(
            open(ConceptCounts, 'r'), delimiter=',', quotechar='"')
        row_count = 0

        for row in Reader:
            for col in range(len(row)):
                conceptcounts.write(row_count, col, row[col], cell_format04)
            row_count += 1
    if xpathCounts is not None:
        Reader = csv.reader(
            open(xpathCounts, 'r'), delimiter=',', quotechar='"')
        row_count = 0

        for row in Reader:
            for col in range(len(row)):
                xpathcounts.write(row_count, col, row[col], cell_format04)
            row_count += 1

    XpathAnalysisWS.write('A2', 'Number of Records')
    XpathAnalysisWS.write('A3', 'Number of Elements / Attributes')
    XpathAnalysisWS.write(
        'A4',
        'Coverage w/r to Repository (CR): \
        number of elements / total number of elements'
    )
    XpathAnalysisWS.write('A5', 'Average Occurrence Rate')
    XpathAnalysisWS.write('A6', 'Repository Completeness: Number of elements \
    / number of elements in most complete collection in repository')
    XpathAnalysisWS.write('A7', 'Homogeneity: Number >= 1 \
    / Total Number of elements in the collection')
    XpathAnalysisWS.write('A8', 'Partial Elements: Number < 0 and < 1')
    XpathAnalysisWS.write('A9', 'Retrieval Date')
    XpathAnalysisWS.write('B1', 'Formulas')
    XpathAnalysisWS.write('C1', 'MIN')
    XpathAnalysisWS.write('D1', 'MAX')
    XpathAnalysisWS.write('E1', 'AVG')
    XpathAnalysisWS.write('B10', 'Element Name')
    XpathAnalysisWS.write('C10', '#Collections')
    XpathAnalysisWS.write('D10', '# = 100%')
    XpathAnalysisWS.write('E10', '# >= 100%')

    for row in range(1, 3):
        colRange4 = xlsxwriter.utility.xl_range(row, 5, row, 500)
        miniFormula = '=MIN(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 2, miniFormula, cell_format04)
        maxiFormula = '=MAX(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 3, maxiFormula, cell_format04)
        avgFormula = '=AVERAGE(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 4, avgFormula, cell_format04)

    for row in range(3, 8):
        colRange4 = xlsxwriter.utility.xl_range(row, 5, row, 500)
        miniFormula = '=MIN(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 2, miniFormula, cell_format11)
        maxiFormula = '=MAX(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 3, maxiFormula, cell_format11)
        avgFormula = '=AVERAGE(' + colRange4 + ')'
        XpathAnalysisWS.write(row, 4, avgFormula, cell_format11)

    Reader = csv.reader(
        open(xpathOccurrence, 'r'), delimiter=',', quotechar='"'
    )
    absRowCount = sum(1 for row in Reader)
    absColCount = len(next(csv.reader(
        open(xpathOccurrence, 'r'), delimiter=',', quotechar='"'
    )))

    XpathAnalysisWS.autofilter(10, 5, 10, absColCount + 3)
    xpathoccurrenceWS.autofilter(0, 0, 0, absColCount - 1)
    avgXpathOccurWS.autofilter(0, 0, 0, absColCount - 1)
    if xpathCounts is not None:
        xpathcounts.autofilter(0, 0, 0, absRowCount - 1)
    XpathAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount +
        3,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen}
    )
    XpathAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount + 3,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow}
    )
    XpathAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount + 3,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed}
    )
    xpathoccurrenceWS.conditional_format(
        1, 1, absRowCount - 2, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen}
    )
    xpathoccurrenceWS.conditional_format(
        1, 1, absRowCount - 2, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow}
    )
    xpathoccurrenceWS.conditional_format(
        1, 1, absRowCount - 2, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed}
    )
    avgXpathOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})
    avgXpathOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    avgXpathOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})
    for row in range(10, absRowCount + 8):
        colRange5 = xlsxwriter.utility.xl_range(row, 5, row, absRowCount + 7)
        numbCollectFormula = '=COUNTIF(' + colRange5 + ',">"&0)'
        CompleteCollectFormula = '=COUNTIF(' + colRange5 + ',"="&1)'
        GreatCollectFormula = '=COUNTIF(' + colRange5 + ',"<"&1)'
        XpathAnalysisWS.write(row, 2, numbCollectFormula)
        XpathAnalysisWS.write(row, 3, CompleteCollectFormula)
        XpathAnalysisWS.write(row, 4, GreatCollectFormula)
    XpathAnalysisWS.autofilter(9, 0, 9, absColCount + 3)
# Create a new scatter chart.
    #chart1 = workbook.add_chart({'type': 'scatter'})

    # Configure the first series.
    #chart1.add_series({
     #   'name': '=Completeness',
      #  'categories': '=XpathOccurrenceAnalysis!$F$3:$BP$3',
       # 'values': '=XpathOccurrenceAnalysis!$F$6:$BP$6',
    #})

    # Add a chart title and some axis labels.
    #chart1.set_title(
      #  {'name': Organization + ' Completeness vs Homogeneity',
       #  'name_font': {'size': 20}
        # }
    #)
    #chart1.set_x_axis(
     #   {'name': 'Homogeneity', 'name_font': {'size': 18, 'bold': False},
      #   'num_font': {'size': 14, 'bold': False}})
    #chart1.set_y_axis(
     #   {'name': 'Completeness (Repository)',
      #   'name_font': {'size': 18, 'bold': False},
       #  'num_font': {'size': 14, 'bold': False}
        # }
    #)
    # set size
    #chart1.set_size({'width': 1200, 'height': 700})
    # Set an Excel chart style.
    #chart1.set_style(11)
    #chart1.set_legend({'none': True})
    # Insert the chart into the worksheet (with an offset).

    #ws5.insert_chart('B1', chart1, {'x_offset': 25, 'y_offset': 10})
    #######################################################################
    Reader = skip_last(csv.reader(
        open(xpathOccurrence, 'r'), delimiter=',', quotechar='"'))

    row_count = 0
    for row in Reader:
        for col in range(len(row) - 1):

            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)
            cell3 = xlsxwriter.utility.xl_rowcol_to_cell(2, col + 5)
            colRange = xlsxwriter.utility.xl_range(1, col + 1, 500, col + 1)
            colRange2 = xlsxwriter.utility.xl_range(2, 5, 2, len(row) + 3)

            formula2 = '=COUNTIF(xpathOccurrence!' + colRange + ',">"&0)'
            XpathAnalysisWS.write(2, col + 5, formula2)

            formula3 = (
                '=' + cell3 + '/COUNTA(xpathOccurrence!' + colRange + ')')
            XpathAnalysisWS.write(3, col + 5, formula3, cell_format11)

            formula4 = '=SUM(xpathOccurrence!' + colRange + ')/' + '%s' % cell3
            XpathAnalysisWS.write(4, col + 5, formula4, cell_format11)

            formula5 = '=' + '%s' % cell3 + '/MAX(' + colRange2 + ')'
            XpathAnalysisWS.write(5, col + 5, formula5, cell_format11)

            formula6 = (
                '=COUNTIF(xpathOccurrence!' +
                colRange + ',">="&1)/' + '%s' % cell3
            )
            XpathAnalysisWS.write(6, col + 5, formula6, cell_format11)

            formula7 = (
                '=COUNTIFS(xpathOccurrence!' +
                colRange + ',">"&0,xpathOccurrence!' +
                colRange + ',"<"&1)/' + '%s' % cell3
            )
            XpathAnalysisWS.write(7, col + 5, formula7, cell_format11)

            formula1 = (
                '=VLOOKUP("Number of Records",AVGxpathOccurrence!1:1048576,' +
                str(col + 2) + ')'
            )
            XpathAnalysisWS.write(1, col + 5, formula1)

            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)

            formula = '=xpathOccurrence!' + '%s' % cell2
            XpathAnalysisWS.write(0, col + 5, formula)
            dateFormula = (
                '=LEFT(RIGHT(xpathOccurrence!' + '%s' % cell2 +
                ',LEN(xpathOccurrence!' + '%s' % cell2 +
                ')-FIND("_", xpathOccurrence!' +
                '%s' % cell2 + ')),FIND("_",xpathOccurrence!' +
                '%s' % cell2 + '))'
            )
            XpathAnalysisWS.write(8, col + 5, dateFormula)
            collectFormula = (
                '=LEFT(xpathOccurrence!' + '%s' % cell2 +
                ',FIND("_",xpathOccurrence!' + '%s' % cell2 + ')-1)'
            )

            XpathAnalysisWS.write(9, col + 5, collectFormula)
    #######################################################################
    Reader = skip_last(csv.reader(
        open(conceptOccurrence, 'r'), delimiter=',', quotechar='"'))

    row_count = 0

    for row in Reader:
        # for col in range(len(row)):
        # conceptOccurrenceWS.write(row_count, col, row[col], cell_format11)
        for col in range(1, len(row)):
            ConceptAnalysisWS.write(row_count + 9, col + 4,
                                    row[col], cell_format11)

        for col in range(0, 1):
            ConceptAnalysisWS.write(row_count + 9,
                                    col, row[col], cell_format11)

        row_count += 1

    conceptOccurrenceWS.set_column('A:A', 50)
    Reader = csv.reader(open(
        AVGconceptOccurrence, 'r'), delimiter=',', quotechar='"')
    row_count = 0

    for row in Reader:
        for col in range(len(row) - 1):
            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)
            cell3 = xlsxwriter.utility.xl_rowcol_to_cell(2, col + 5)
            colRange = xlsxwriter.utility.xl_range(1, col + 1, 500, col + 1)
            colRange2 = xlsxwriter.utility.xl_range(2, 5, 2, len(row) + 3)
            formula2 = '=COUNTIF(ConceptOccurrence!' + colRange + ',">"&0)'
            ConceptAnalysisWS.write(2, col + 5, formula2)

            formula3 = (
                '=' + cell3 + '/COUNTA(ConceptOccurrence!' + colRange + ')'
            )
            ConceptAnalysisWS.write(3, col + 5, formula3, cell_format11)

            formula4 = (
                '=SUM(ConceptOccurrence!' + colRange + ')/' + '%s' % cell3)
            ConceptAnalysisWS.write(4, col + 5, formula4, cell_format11)

            formula5 = '=' + '%s' % cell3 + '/MAX(' + colRange2 + ')'
            ConceptAnalysisWS.write(5, col + 5, formula5, cell_format11)

            formula6 = (
                '=COUNTIF(ConceptOccurrence!' +
                colRange + ',">="&1)/' + '%s' % cell3)

            ConceptAnalysisWS.write(6, col + 5, formula6, cell_format11)

            formula7 = (
                '=COUNTIFS(ConceptOccurrence!' + colRange +
                ',">"&0,ConceptOccurrence!' + colRange + ',"<"&1)/' +
                '%s' % cell3)

            ConceptAnalysisWS.write(7, col + 5, formula7, cell_format11)

            formula1 = (
                '=VLOOKUP("Number of Records", AVGxpathOccurrence!1:1048576,' +
                str(col + 2) + ')'
            )

            ConceptAnalysisWS.write(1, col + 5, formula1)

            cell2 = xlsxwriter.utility.xl_rowcol_to_cell(0, col + 1)

            formula = '=ConceptOccurrence!' + '%s' % cell2
            ConceptAnalysisWS.write(0, col + 5, formula)
            dateFormula = (
                '=LEFT(RIGHT(ConceptOccurrence!' + '%s' % cell2 +
                ',LEN(ConceptOccurrence!' + '%s' % cell2 +
                ')-FIND("_", ConceptOccurrence!' + '%s' % cell2 +
                ')),FIND("_",ConceptOccurrence!' + '%s' % cell2 + '))')

            ConceptAnalysisWS.write(8, col + 5, dateFormula)
            collectFormula = (
                '=LEFT(ConceptOccurrence!' + '%s' % cell2 +
                ',FIND("_",ConceptOccurrence!' + '%s' % cell2 + ')-1)')

            ConceptAnalysisWS.write(9, col + 5, collectFormula)

        row_count += 1
    #######################################################################
    #
    ConceptAnalysisWS.write('A2', 'Number of Records')
    ConceptAnalysisWS.write('A3', 'Number of Concepts')
    ConceptAnalysisWS.write('A4', 'Coverage w/r to Repository (CR): \
        number of concepts / total number of concepts')
    ConceptAnalysisWS.write('A5', 'Average Occurrence Rate')
    ConceptAnalysisWS.write(
        'A6',
        'Repository Completeness: Number of concepts / num\
    ber of concepts in most complete collection in repository'
    )
    ConceptAnalysisWS.write('A7', 'Homogeneity: Number >= 1 /\
     Total Number of concepts in the collection')
    ConceptAnalysisWS.write('A8', 'Partial Concepts: Number < 0 and < 1')
    ConceptAnalysisWS.write('A9', 'Retrieval Date')
    ConceptAnalysisWS.write('B1', 'Formulas')
    ConceptAnalysisWS.write('C1', 'MIN')
    ConceptAnalysisWS.write('D1', 'MAX')
    ConceptAnalysisWS.write('E1', 'AVG')
    ConceptAnalysisWS.write('C10', '#Collections')
    ConceptAnalysisWS.write('D10', '# = 100%')
    ConceptAnalysisWS.write('E10', '# >= 100%')

    for row in range(1, 3):
        colRange4 = xlsxwriter.utility.xl_range(row, 5, row, 500)
        miniFormula = '=MIN(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 2, miniFormula, cell_format04)
        maxiFormula = '=MAX(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 3, maxiFormula, cell_format04)
        avgFormula = '=AVERAGE(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 4, avgFormula, cell_format04)

    for row in range(3, 8):
        colRange4 = xlsxwriter.utility.xl_range(row, 5, row, 500)
        miniFormula = '=MIN(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 2, miniFormula, cell_format11)
        maxiFormula = '=MAX(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 3, maxiFormula, cell_format11)
        avgFormula = '=AVERAGE(' + colRange4 + ')'
        ConceptAnalysisWS.write(row, 4, avgFormula, cell_format11)

    Reader = csv.reader(
        open(conceptOccurrence, 'r'), delimiter=',', quotechar='"')
    absRowCount = sum(1 for row in Reader)
    absColCount = len(next(csv.reader(
        open(conceptOccurrence, 'r'), delimiter=',', quotechar='"')))
    ConceptAnalysisWS.autofilter(10, 5, 10, absColCount + 3)
    conceptOccurrenceWS.autofilter(0, 0, 0, absColCount - 1)
    avgConceptOccurWS.autofilter(0, 0, 0, absColCount - 1)
    if ConceptCounts is not None:
        conceptcounts.autofilter(0, 0, 0, 26)
    ConceptAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount + 3,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})
    ConceptAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount + 3,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    ConceptAnalysisWS.conditional_format(
        10, 5, absRowCount + 7, absColCount + 3,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})
    conceptOccurrenceWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})
    conceptOccurrenceWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    conceptOccurrenceWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})
    avgConceptOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '>=', 'value': 1, 'format': formatGreen})
    avgConceptOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': 0, 'format': formatYellow})
    avgConceptOccurWS.conditional_format(
        1, 1, absRowCount - 1, absColCount - 1,
        {'type': 'cell', 'criteria': '=', 'value': -1, 'format': formatRed})

    for row in range(10, absRowCount + 8):
        colRange5 = xlsxwriter.utility.xl_range(row, 5, row, absColCount + 7)
        numbCollectFormula = '=COUNTIF(' + colRange5 + ',">"&0)'
        CompleteCollectFormula = '=COUNTIF(' + colRange5 + ',"="&1)'
        GreatCollectFormula = '=COUNTIF(' + colRange5 + ',"<"&1)'
        ConceptAnalysisWS.write(row, 2, numbCollectFormula)
        ConceptAnalysisWS.write(row, 3, CompleteCollectFormula)
        ConceptAnalysisWS.write(row, 4, GreatCollectFormula)

    workbook.close()


def WriteGoogleSheets(SpreadsheetLocation):
    """requires collectionSpreadsheet or
    OrganizationSpreadsheet output.
    (or really any Excel workbook you want to instantiate on Google Sheets
    under the same name)
    """
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = (
        './client_secrets.json')

    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile("./mycreds.txt")
    # if not creds or creds.invalid:

    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.credentials.invalid:
        # Refresh them if expired
        gauth.Refresh('./mycreds.txt')
    else:
        # Initialize the saved creds
        gauth.Authorize()
# Save the current credentials to a file
    gauth.SaveCredentialsFile("./mycreds.txt")

    drive = GoogleDrive(gauth)

    SpreadsheetName = SpreadsheetLocation.rsplit('/', 1)[-1]

    test_file = drive.CreateFile({'title': SpreadsheetName})
    test_file.SetContentFile(SpreadsheetLocation)
    test_file.Upload({'convert': True})

    # Insert the permission.
    permission = test_file.InsertPermission(
        {'type': 'anyone', 'value': 'anyone', 'role': 'reader'})

    print(test_file['alternateLink'])  # Display the sharable link.
