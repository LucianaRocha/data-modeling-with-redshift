[//]: # (Image References)

[image1]: ./images/star_schema_model_songplays.png "Model"
[image2]: ./images/filepaths.png "Filepaths songdata"
[image3]: ./images/song_file_example.png "Song file example"
[image4]: ./images/filepaths_logdata.png "Filepaths logdata"
[image5]: ./images/log_file_example.png "Log file example"
[image6]: ./images/notebook.png "Notebook"
[image7]: ./images/run_notebook.png "Run notebook"
[image8]: ./images/star_schema_model_songplays.png "Star Schema"
[image9]: ./images/result_1.png "Result"
[image10]: ./images/result_null.png "Result null"
[image11]: ./images/result_count.png "Result count"
[image12]: ./images/dwh_cfg.png "config"


# Project: Data Warehouse hosted on Redshift
This project is part of the [Data Engineering Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027), by [Udacity](https://www.udacity.com/).  

## Introduction  

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. It is possible to test the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare the results with the expected results.

### Project Description  

In this project, I applied what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I needed to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Star Schema for Song Play Analysis
For this project, I used the song and log datasets and created a [star-schema model](https://en.wikipedia.org/wiki/Star_schema) optimized for queries on song play analysis. 

Star and snowflake schemas use the concepts of **fact** and **dimension** tables to make getting value out of the data easier.  

- **Fact table** consists of measurements, metrics or facts a business process.
- **Dimension table** a structure that categorizes facts and measures in order to enable users to answer business questions.

**Benefits of star-schema:**  

- Star-schemas are denormalized. Denormalization increases performance by reducing the number of joins between tables. Data integrity will take a bit of a potential hit, as there will be more copies of the data (to reduce JOINS). The benefits of star-schema denormalization are:  

	- Simpler queries
	- Simplified business reporting logic
	- Query performance gains
	- Fast aggregations
	- Feed cubes
	- Fast aggregations
	
         
## Data modeling

For this project, I modeled a star-schema that includes one fact table and four dimension tables. See the descriptions below:

**Fact Table**  

- **songplays:** records in log data associated with song plays. The log data was filtered with page = 'NextSong'.  

	- Columns: songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent.  
	- Distribution strategy: sortkey distkey in start\_time
	- Solution: I decided to use this strategy because it is fact table with 62046 rows (big table)

**Dimension Tables**  

- **users:** users in the app.  
	- Columns: user\_id, first\_name, last\_name, gender, level.
	- Distribution strategy: sortkey in user\_id and diststyle all
	- Solution: I decided to use this strategy because this table is a small dimension with 208 rows

- **songs:** songs in music database.  
	- Columns: song\_id, title, artist\_id, year, duration  
	- Distribution strategy: sortkey distkey in song\_id
	- Solution: I decided to use this strategy because this table is a large dimension with 119168 rows 

- **artists:** artists in music database.
	- Columns: artist\_id, name, location, latitude, longitude  
	- Distribution strategy: sortkey distkey in artist\_id
	- Solution: I decided to use this strategy because this table is a large dimension with 80200 rows 

- **time:** timestamps of records in songplays broken down into specific units.
	- Columns: start\_time, hour, day, week, month, year, weekday  
	- Distribution strategy: sortkey in start\_time and diststyle all
	- Solution: I decided to use this strategy because this table is a small dimension with 13626 rows
	
![Data Modeling][image8]  
	
# Project Datasets  

## Datasets

I worked with two datasets that reside in S3. Here are the S3 links for each:  
	- Song data: s3://udacity-dend/song\_data  
	- Log data: s3://udacity-dend/log\_data  
		
Log data json path: s3://udacity-dend/log\_json\_path.json

### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

![Filepaths][image2]

And below is an example of what a single song file, TRAAAAW128F429D538.json, looks like.

![Song file example][image3]  

### Log Dataset  
 
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

![Filepaths logdata][image4]

And below is an example of what the data in a log file, 2018-11-01-events.json, looks like.  

![Log file example][image5]


# ETL pipeline

I created three files **.py** that can be run by Anaconda Prompt (Windows) or Terminal (Linux) without the help of a graphic interface. The files are organized as follows:  

- **sql_queries.py:** 
	- contains sql queries to: drop tables, create tables, insert tables, and copy operations   
		- **create\_table\_queries:** for each 'create table' command was assigned a variable name 
		- **drop\_table\_queries:** for each 'drop table' command was assigned a variable name  
	- for each 'insert table' command was assigned a variable name
	- There are 2 COPY operations to load 2 staging tables which were used as a data source to load the  dimensions and fact tables
		- staging_events
		- staging_songs

The lists are imported and used in create\_tables.py  

- **create_tables.py:**  
	- creates a star schema for this project
	- creates a database for this project
	- imports the psycopg2 to create a connection and a cursor for this project
	- imports the lists from sql\_queries.py 
	- defines a function that uses the drop\_table\_queries list and runs a drop commands sequentially
	- defines a function that uses the create\_table\_queries list and runs a create commands sequentially  

- **etl.py:**  
	- imports the sql\_queries.py 
	- defines functions that:
		- read the data files (log\_data and song\_data)
		- create staging tables for data files
		- insert data into each table. The functions use the variable names defined for each 'insert table' command in **sql_queries.py**  
	

# Getting Started
## Files included in this repository  
The project includes six files:  

 1. **create_tables.py:** drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.  
 2. **dw\_hosted\_on\_redshift.ipynb:** reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.  
 3. **etl.py:** reads and processes files from song\_data and log\_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.  
 4. **sql_queries.py:** contains all your sql queries, and is imported into the last three files above.  
 5. **README.md:** provides discussion about this project. 

## Preparing the environment to run the project
**1.** Install the Anaconda Plataform

- For more information about it, see [Anaconda Installation Guide](https://docs.anaconda.com/anaconda/install/) and [User guide](https://docs.anaconda.com/anaconda/user-guide/)

- Download the [Anaconda Installer](https://www.anaconda.com/distribution/), version Python 3.x  
conda  

**2.** Create and activate a new environment for this project

- Open Anaconda Prompt (Windows) or Terminal (Linux) and type

	```
	conda create --name datamodeling
	```  

	```
	conda activate datamodeling
	```
	
**3.** Required libraries for this project

Some libraries will be installed with Anaconda installer and others need to be installed later. For this project you will need:  

- ipython-sql (not provided by Anaconda installer)  
- jsonschema  
- jupyter notebook  
- psycopg2 (not provided by Anaconda installer)  
- python  
- boto3

**4.** In your new environment check if the required libraries were installed by Anaconda installer  

- In your Anaconda Prompt (Windows) or Terminal (Linux) type the line command below and verify the packages installed by Anaconda.
		
	```
	conda list
	```  

- Install libraries not provided by Anaconda installer  
	- In your Anaconda Prompt (Windows) or Terminal (Linux) type the line command below and install the required libraries. Below is an example:
	
	```
	conda install boto3 
	```  

**5.** On [AWS platform] (https://aws.amazon.com/) it is necessary:  

- Create a dwh.cfg file  
- Create an IAM Role  
- Create a Security Group  
- Launch a Redshift Cluster  
- Create an IAM user  
- Create an S3 Bucket  
- Create a PostgreSQL DB Instance using RDS  

For more information about you can create the steps above see [Getting Started Guide](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html)

## Getting the code 
There are two options to get this project:

- Download it as a zip file

	- Do the download in this [link](https://github.com/LucianaRocha/data-modeling-with-postgresql/archive/master.zip).
	- Unzip the file in a folder of your choice.  

- Clone this repository using Git version control system

	- Open Anaconda Prompt (Windows) or Terminal (Linux), navigate to the folder of your choice, and type the command below:  

	```
	git clone https://github.com/LucianaRocha/data-modeling-with-postgresql.git
	```
	

# Running the project  

There are two options to run this project:  

1. By Jupyter Nobebook, or
2. By Anaconda Prompt (Windows) or Terminal (Linux)

For any options you will need to create a configuration file with the file name dwh.cfg as below:  

![Config][image12]

## By Jupyter Nobebook  

-  Open Anaconda Prompt (Windows) or Terminal (Linux), navigate to the project folder, and type the commands below to activate the project environment, and to open Jupyter.
If you are keen to know more about notebooks and other tools of Project Jupyter, you find more information on this [website](https://jupyter.org/index.html).

	```
	conda activate datamodeling
	```
	
	```
	jupyter notebook
	```	

- Click on the dw\_hosted\_on\_redshift.ipynb to open the notebook and follow the instructions within it.

- Run the notebook.  

![Run notebook][image7]  


## By Anaconda Prompt (Windows) or Terminal (Linux)

- Open Anaconda Prompt (Windows) or Terminal (Linux), navigate to the project folder, and type the command below to create the tables:

	```
	python create_tables.py
	```

- In your terminal, after creating all tables of the previous step, type the command below to load the tables:  
		
	```
	python etl.py
	```	

# Ideas for future work
There are many BI techniques and I would like improve this model with [Kimball Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/):  

- [Slowly changing dimension](https://www.kimballgroup.com/2013/02/design-tip-152-slowly-changing-dimension-types-0-4-5-6-7/)
- [Handling Null Foreign Keys in Fact Tables](https://www.kimballgroup.com/2010/10/design-tip-128-selecting-default-values-for-nulls/)
- [White Paper: An Architecture for Data Quality](https://www.kimballgroup.com/2007/10/white-paper-an-architecture-for-data-quality/)

# References
- Data Warehouse and Business Intelligence Resources [Kimball Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- Boto3 Docs [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)
- PEP8 [style guidelines](https://www.python.org/dev/peps/pep-0008/)
- PostgreSQL [documentation](https://www.postgresql.org/docs/11/index.html)
- AWS Documentation [documentation](https://docs.aws.amazon.com/index.html)
- SQL [Kickstarter SQL Style Guide](https://gist.github.com/fredbenenson/7bb92718e19138c20591)  
