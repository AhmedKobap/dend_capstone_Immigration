# IMMIGRATION CAPSTONE PROJECT Overview

I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the work space. 
This is where the data comes from. 

In this project four datasets are accessed to create a star schema with fact and dimension tables. An ETL pipeline is built using python, postgresql, pyspark and redshift with an analytic focus. The star schema is optimized for immigrants who came to the US for a temporary stay for pleasure.

Staging Data Uploaded on daily bases on S3 bucked after First stage of cleansing
File named 'I94_SAS_Labels_Descriptions.SAS' Contians five lookups (Countries,port, Mode, Address,Visa) by divide the records to diffrent dataframe and load them to each table.

<b> the implemented star schema model found with project file named "ERD_Model.png"</b>

### Explore and Assess the Data

The immigration data set in SAS format from April 2016 titled 'i94_apr16_sub.sas7bdat' consists of data for temporary visits to the US from all over the world. The file titled 'I94_SAS_Labels_Descriptions.SAS' has an explaination of codes used in the immigration dataset. The immigration dateset has around a million rows. The original dataset has the following columns with the number of rows listed for each column.

The file titled 'us-cities-demographics.csv' consists of data in csv format. It lists cities and various demographics such as city, state, median age, male population, female population, total population, veterans, foreign-born, average house size, state code, race, count

The file titled 'airport-codes_csv.csv' consists of data in csv format. It lists airport_id, airport_type ,airport_name, elevation_ft, continent, iso_country, iso_region ,municipality, gps_code, iata_code, local_code and coordinates(latitude and longitude)

the mapping sheet attached with project files named 'Mapping_sheet_Immigrants.xlsx' that contains datatypes and description for columns.


### ETL Process
1. The Capstone Project.ipynb file shown below is run to gather and clean the immigration, demographics, airport and lookus datasets and prepare staging files for Redshift.

    The 'dwh.cfg' file is created with the secret and access keys to load files in S3.

    The 'dwh.cfg' file also contains cluster properties to load files in redshift.

    The following is done for the immigration dataset :

    A. The dataset is converted to CSV format from sas7bdat using Pyspark.

    B. The columns that are not relevant to the data model are dropped. The remaining columns are renamed to become more nderstandable.

    C. The  values that contains "" are replaced with null.

    D. The columns are converted to their appropriate data types .

    E. The final 'IMMIGRATION.CSV' is loaded in the 'capstone-kobap' S3 bucket.

    F- loading 'IMMIGRATION.CSV' to star schema in redshift cluster.

    The following is done for the demographics dataset :

    A. The csv dataset is set to the correct deliminator (;).

    B. The columns are converted to their appropriate data types

    D. Each row in the column is converted to a CSV file with prefix 'DEMOGRAPHICS.CSV' and loaded in the 'capstone-kobap' S3 bucket.

    E- loading 'DEMOGRAPHICS.CSV' to star schema in redshift cluster.

 The following is done for the Airport dataset :

    A. The csv dataset is set to the correct deliminator (,).

    B. The columns are converted to their appropriate data types

    D. Each row in the column is converted to a CSV file with prefix 'AIRORT.CSV' and loaded in the 'capstone-kobap' S3 bucket.

   	E- loading 'AIRORT.CSV' to star schema in redshift cluster.
    
    Note: data flow.png illustrates data flow for different layer starting from source system files till data warehouse.

Finally all model lookup are generated from 'I94_SAS_Labels_Descriptions.SAS' file after cleansing, trim and remove not needed strings, then create file for each lookup in  'capstone-kobap' S3 bucket.
at the end all lookup files are transformed to redshift cluster.

### implemented quality checks####
1- trim values by trimming values in dataframe as shown below 
addrl_df['addrl_ds']= addrl_df['addrl_ds'].str.strip()
2- replace "" with none during write files in s3 bucket as shown below:
df_spark1.write.format('csv').option('header',True).mode('overwrite'). option('sep','|').save("IMM_FILES/",nullValue=None)
3- Check count between inserted records and dataframes as shown below:
print('AIRPORT dataframe=', len(df_airport),' inserted records= ',count_list[4])
4- analysis quality checks to can understand the data to implement the model
by using df.info() for pandas dataframe and printschema for pyspark dataframe 

### Project Write Up

Big data tools were needed to process this data given its size. Specifically, I used the following tools in my project:

    PySpark - PySpark allows you to interact with the Apache Spark data processing framework by writing Python code. In paricular, PySpark allows you to express data as "DataFrames," which allows you to concentrate on data transformations and other tasks without managing how the dataset is ditributed over nodes in the computing cluster.

    S3 - I created staging layer located on Amazon's S3 storage service. 
    
    
    redshift - my model created on redshift and data located in tables.

###  Purpose
### The resulting tables:
Facts and Dimensions can be found in the (ERD_Model.png)
The purpose of the model is to analyze the immigration data and the connections between the arrival ports and states where the immigrants settle (which can be analyzed in connection to the us_cities_demographics).



Solutions for Different Scenarios
#### The data was increased by 100x
   -For large dataset Spark can be used to write parquet files to S3. The parquet files can then be copied over to Redshift.
   -For large dataset also use Redshift Spectrum which enables you to run queries against exabytes of data in Amazon S3. There is no loading or ETL required.
   - also We can use a cluster manager such as Yarn and increase the number of nodes depending on the needs.
    
#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
In this case, the ETL process should be scheduled with Airflow (or a similar tool) and the immigration data set should be updated daily. Since the other datasets wouldn't get updated as often, not all the data transformations will run daily.

#### The database needed to be accessed by 100+ people.
We can use a cluster manager such as Yarn and increase the number of nodes depending on the needs.
Redshift automatically distributes data and query load across all nodes (massively paralled processing)
Redshift performs high-quality data compression. Columnar data stores can be compressed much more than row-based data stores.
