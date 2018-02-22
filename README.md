# Re3data
A repository branch to explore metadata in Re3data and research the metadata about repositories. Possible research directions include determining recommendations for journals to vet repositories with, study of what is claimed vs what is actually offered, test set of repos to test api layer of vm Metadata Evaluation analytics. Studies of different metadata dialects. EX there are over 200 repositories using ISO 19115 dialect of the documentation language. Do they use the dialect in the same way? Do usage patterns follow communities of practice? Funding agency or country of origin?

If you are new to Jupyter and want to explore the notebooks in this repository, use the wiki to get started.

[Get Started](https://github.com/scgordon/MetadataEvaluation/wiki/Getting-Started)

The Evaluation notebook should allow the user to gain an understanding of metadataEvaluation.py functions used to create data products.

[Evaluation Notebook](https://github.com/scgordon/MetadataEvaluation/blob/re3data/notebook/Re3data_Evaluation.ipynb)

More functions are described in the metadataEvaluation module Tutorial:

[Module Tutorial](https://github.com/scgordon/MetadataEvaluation/blob/master/notebook/metadataEvaluation_ModuleTutorial.ipynb)

The Exploration notebook directly addresses the information needs listed below and shows the location the csv files are for each data product for further visualization in other environments. In this notebook you just hold shift and press return/enter, or make selections from dropdown.

[Exploration Notebook](https://github.com/scgordon/MetadataEvaluation/blob/re3data/notebook/Re3data_Exploration.ipynb)

The functions used in both Notebooks attempt to:

* Check the occurrence of metadata elements and compare percentages across timestamped collections of Re3data metadata

* Create lists of records at Re3data that do not contain an element

* Create lists of what content occurs at a selected element 

* count the unique values of content at a selected element to see what variations occurr at selected elements. Useful for identifiying opportunities to ensure consistency. Also useful to see if nonstandard element content has a standardized location

Link below to interactive webbuild of the repository via MyBinder which opens the Exploration Notebook particularly useful for exploration of the third and fourth bullet points:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/scgordon/MetadataEvaluation/re3data?filepath=%2Fnotebook%2FRe3data_Exploration.ipynb)

