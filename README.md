# NCAR-DASH
A repository for the DSET team to explore metadata in the DASH WAF and determine curation needs via Jupyter Notebooks. 

If you are new to Jupyter and want to explore the notebooks in this repository, use the wiki to get started.

https://github.com/scgordon/NCAR-DASH/wiki/Getting-Started

The Evaluation notebook should allow the user to gain an understanding of metadataEvaluation.py functions used to create data products.

https://github.com/scgordon/NCAR-DASH/blob/master/notebook/DSET_DASH_Evaluation.ipynb

The Presentation notebook directly addresses the information needs listed below and shows the location the csv files are for each data product for further visualization in other environments. In this notebook you just hold shift and press return/enter, or make selections from dropdown.

https://github.com/scgordon/NCAR-DASH/blob/master/notebook/DSET_DASH_Presentation.ipynb

The functions used in both Notebooks attempt to:

* Check the occurrence of metadata elements and compare percentages across DASH collections

* Check for a specific set of essential elements across the collection, provide counts, provide occurrence

* Create lists of records at NCAR that do not contain an element

* Create lists of what content occurs at a selected element 

* count the unique values of content at a selected element to see what variations occurr at selected elements. Useful for identifiying opportunities to ensure consistency. Also useful to see if nonstandard element content has a standardized location

Link below to interactive webbuild of the repository via MyBinder which opens the Presentation Notebook particularly useful for exploration of the fourth and fifth bullet points:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/scgordon/NCAR-DASH/master?filepath=%2Fnotebook%2FDSET_DASH_Presentation.ipynb)
