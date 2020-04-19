# Udacity - Data Engineer Nanodegree - Sparkify - DataLake AWS and Spark Project

## About this project

Sparkify dataset is a database that contains music and artist records created for educational purposes.
The data extracted from this database it's on a json format and its available at this repository in [data](./data) folder.
It is also available at AWS by Udacity at this AWS S3 Bucket: s3a://udacity-dend

For this project two datasets were used: songs and log data. 

- Song json files, were used to get the information about songs (of course) and artist.
- Log json files, were used to get the information about covers song, artist and the complementary information for each song. 

## Prerequisites to use this repository

1. Python 3.x
1. Spark configured, at Jupyter Notebook
1. Jupyter Notebook (Recommended, to install with Anaconda)
1. Windows/Mac/Linux - Compatible
2. Firefox, Chrome, Edge Navigators - Compatible

## Deployment instructions

From terminal, command line or kind of:

1. Execute as follows:
	
	1. `python ./etl.py` - This scripts is intended to load data from dataset into database Sparkify Tables  

Just to test purposes there is another file called **etl_testing.ipynb**