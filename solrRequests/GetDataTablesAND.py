import pandas as pd
import os
import metadataEvaluation
import subprocess

subprocess.call(["./xmlTransform.sh", $1, $2, $3])
EvaluatedMetadataDF = pd.read_csv('/Users/scgordon/MetadataEvaluation/data/AND/2005_Evaluated.csv.gz')
metadataEvaluation.xpathOccurrence(EvaluatedMetadataDF, $1, $2, $3,$4)
