import pandas as pd
import csv

def dataProducts(EvaluatedMetadataDF):
    #variables needed to save data products
    FileName=Collection+'_'+Dialect
    FileDirectory='../data/'+Organization
    FilePath=FileDirectory+'/'+FileName
    Evaluated=FilePath+'_Evaluated.csv.gz'
    SimplifiedEvaluated=FilePath+'_EvaluatedSimplified.csv.gz'
    RAD=FilePath+'_RAD.csv'
    QuickE=FilePath+'_QuickE.csv'
    Occurrence=FilePath+'_Occurrence.csv'
    XpathOccurrence=FilePath+'_XPathOccurrence.csv'

    NotProvidedRAD=FilePath+'_NotProvided_RAD.csv'
    NotProvidedQuickE=FilePath+'_NotProvided_QuickE.csv'
    NotProvidedOccurrence=FilePath+'_NotProvided_Occurrence.csv'
    NotProvidedXpathOccurrence=FilePath+'_NotProvided_XPathOccurrence.csv'

    ProvidedRAD=FilePath+'_Provided_RAD.csv'
    ProvidedQuickE=FilePath+'_Provided_QuickE.csv'
    ProvidedOccurrence=FilePath+'_Provided_Occurrence.csv'
    ProvidedXpathOccurrence=FilePath+'_Provided_XPathOccurrence.csv'

    #add a column for declaring the collection and dialect to uniquely identify data in combined data products
    EvaluatedMetadataDF.insert(3, 'Collection', Collection+'_'+Dialect)
    #save to file
    EvaluatedMetadataDF.to_csv(Evaluated, mode = 'w', compression='gzip', index=False)


    #Create a simplified XPath output
    EvaluatedSimplifiedMetadataDF = EvaluatedMetadataDF.copy()
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.replace('/gco:CharacterString', '')
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.replace('/[a-z]+:+?', '/')
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.replace('/@[a-z]+:+?', '/@')
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.replace('/[A-Z]+_[A-Za-z]+/?', '/')
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.replace('//', '/')
    EvaluatedSimplifiedMetadataDF['XPath']=EvaluatedSimplifiedMetadataDF['XPath'].str.rstrip('//')
    EvaluatedSimplifiedMetadataDF.to_csv(SimplifiedEvaluated, mode = 'w', compression='gzip', index=False)

    #Create a Recommendations Analysis data product

    group_name = EvaluatedSimplifiedMetadataDF.groupby(['Collection','Record', 'Concept'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceMatrix.columns.names = ['']
    pd.options.display.float_format = '{:,.0f}'.format
    occurrenceMatrix.to_csv(RAD, mode = 'w', index=False)
    occurrenceMatrix
    #create a QuickE data product

    group_name = EvaluatedSimplifiedMetadataDF.groupby(['XPath', 'Record'], as_index=False)
    QuickEdf=group_name.size().unstack().reset_index()
    QuickEdf=QuickEdf.fillna(0)
    pd.options.display.float_format = '{:,.0f}'.format
    QuickEdf.to_csv(QuickE, mode = 'w', index=False)
    QuickEdf

    #concept occurrence data product

    group_name = EvaluatedSimplifiedMetadataDF.groupby(['Record', 'Concept'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceSum=occurrenceMatrix.sum()
    occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(1, 'Collection', FileName)
    result.insert(4, 'CollectionOccurrence%', FileName)
    result.insert(4, 'AverageOccurrencePerRecord', FileName)
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

    #xpath occurrence data product

    group_name = EvaluatedSimplifiedMetadataDF.groupby(['Record', 'XPath'], as_index=False)
    occurrenceMatrix=group_name.size().unstack().reset_index()
    occurrenceMatrix=occurrenceMatrix.fillna(0)
    occurrenceSum=occurrenceMatrix.sum()
    occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()

    result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
    result.insert(1, 'Collection', FileName)
    result.insert(4, 'CollectionOccurrence%', FileName)
    result.insert(4, 'AverageOccurrencePerRecord', FileName)
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

    # Create dataframe of just the elements that do not have a version of Not Provided for their content
    ContentProvidedDF = EvaluatedSimplifiedMetadataDF[EvaluatedSimplifiedMetadataDF.Content!=("Not provided" or "Not%20provided")]

    if len(ContentProvidedDF)==len(EvaluatedSimplifiedMetadataDF):
       print("No elements contain a variant of 'Not provided' in their content for this collection")
       
    else:
        print("Secondary data products, RAD, QuickE, Occurrence, being created for collection for all elements that contain a variant of 'Not provided' in their content and a set of products for the elements that do not contain a variant of 'Not provided' in their content")
        
        # Create dataframe of just the elements that do not have a version of Not Provided for their content
        ContentProvidedDF = EvaluatedSimplifiedMetadataDF[EvaluatedSimplifiedMetadataDF.Content!=("Not provided" or "Not%20provided")]

        # Create secondary data products: RAD, QuickE, Occurrence for both provided and not provided content.

        #not provided RAD

        group_namenotProvided = ContentNotProvidedDF.groupby(['Collection','Record', 'Concept'], as_index=False)
        occurrenceMatrixnotProvided=group_namenotProvided.size().unstack().reset_index()
        occurrenceMatrixnotProvided=occurrenceMatrixnotProvided.fillna(0)
        pd.options.display.float_format = '{:,.0f}'.format
        occurrenceMatrixnotProvided.to_csv(NotProvidedRAD, mode = 'w', index=False)

        #Provided RAD

        group_nameProvided = ContentProvidedDF.groupby(['Collection','Record', 'Concept'], as_index=False)
        occurrenceMatrixProvided=group_nameProvided.size().unstack().reset_index()
        occurrenceMatrixProvided=occurrenceMatrixProvided.fillna(0)
        pd.options.display.float_format = '{:,.0f}'.format
        occurrenceMatrixProvided.to_csv(ProvidedRAD, mode = 'w', index=False)

        #not provided QuickE

        group_namenotProvided = ContentNotProvidedDF.groupby(['XPath', 'Record'], as_index=False)
        QuickEdfnotProvided=group_namenotProvided.size().unstack().reset_index()
        QuickEdfnotProvided=QuickEdfnotProvided.fillna(0)
        pd.options.display.float_format = '{:,.0f}'.format
        QuickEdfnotProvided.to_csv(NotProvidedQuickE, mode = 'w', index=False)

        #Provided QuickE

        group_nameProvided = ContentProvidedDF.groupby(['XPath', 'Record'], as_index=False)
        QuickEdfProvided=group_nameProvided.size().unstack().reset_index()
        QuickEdfProvided=QuickEdfProvided.fillna(0)
        pd.options.display.float_format = '{:,.0f}'.format
        QuickEdfProvided.to_csv(ProvidedQuickE, mode = 'w', index=False)

        #Provided Occurrence
        
        group_name = ContentProvidedDF.groupby(['Record', 'Concept'], as_index=False)
        occurrenceMatrix=group_name.size().unstack().reset_index()
        occurrenceMatrix=occurrenceMatrix.fillna(0)
        occurrenceSum=occurrenceMatrix.sum()
        occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()
        
        result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
        result.insert(1, 'Collection', FileName)
        result.insert(4, 'CollectionOccurrence%', FileName)
        result.insert(4, 'AverageOccurrencePerRecord', FileName)
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
        result.to_csv(ProvidedOccurrence, mode = 'w', index=False)
       
        #Not provided Occurrence
        
        group_name = ContentNotProvidedDF.groupby(['Record', 'Concept'], as_index=False)
        occurrenceMatrix=group_name.size().unstack().reset_index()
        occurrenceMatrix=occurrenceMatrix.fillna(0)
        occurrenceSum=occurrenceMatrix.sum()
        occurrenceCount=occurrenceMatrix[occurrenceMatrix!=0].count()
        
        result = pd.concat([occurrenceSum, occurrenceCount], axis=1).reset_index()
        result.insert(1, 'Collection', FileName)
        result.insert(4, 'CollectionOccurrence%', FileName)
        result.insert(4, 'AverageOccurrencePerRecord', FileName)
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
        result.to_csv(NotProvidedOccurrence, mode = 'w', index=False)
    print("Data products created for the", Collection, "collection from the", Organization, "organization")
