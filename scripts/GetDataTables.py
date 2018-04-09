import pandas as pd
import os
import metadataEvaluation

df_known = pd.read_csv('/Users/scgordon/MetadataEvaluation/raw/AND/2005_dataKnown.csv')
df_all = pd.read_csv('/Users/scgordon/MetadataEvaluation/raw/AND/2005_dataAll.csv')
df_unknown = df_all[(~df_all.XPath.isin(df_known.XPath))]
df = pd.concat([df_known, df_unknown], axis=0)
df.drop_duplicates(inplace=True)
df.fillna('Unknown', inplace=True)
EvaluatedMetadataDF=df.replace({'Dialect': {'Unknown': '$3'}})

os.makedirs('/Users/scgordon/MetadataEvaluation/data/AND', exist_ok=True)
#EvaluatedMetadataDF.to_csv('$MetadataEvaluation/data/$1/$2_EML_Evaluated.csv.gz',
#          index=False,
#          compression='gzip', columns=['Collection', 'Dialect', 'Record', 'Concept', 'Content', 'XPath'])
#ConceptCounts(EvaluatedMetadataDF, '$1', '$2', '$3')
#XpathCounts(EvaluatedMetadataDF, '$1', '$2', '$3')
#QuickEDataProduct(EvaluatedMetadataDF, '$1', '$2', '$3')
metadataEvaluation.conceptOccurrence(EvaluatedMetadataDF, 'AND', '2005', 'EML','/Users/scgordon/MetadataEvaluation/data/AND/2005_EML_Occurrence.csv')
#xpathOccurrence(EvaluatedMetadataDF, '$1', '$2', '$3')
