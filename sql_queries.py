import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN=config.get('IAM_ROLE','ARN')
key=config.get('AWS','key')
secret=config.get('AWS','secret')

# CREATE SCHEMA
CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS Capstone;"
SET_SCHEMA = "SET search_path TO Capstone;"
# DROP TABLES

staging_immigrants_table_drop = "drop table if exists Capstone.immigrants"
staging_DEMOGRAPHICS_table_drop = "drop table if exists Capstone.DEMOGRAPHICS"
PORT_table_drop = "drop table if exists Capstone.PORT"
VISA_table_drop = "drop table if exists Capstone.VISA"
AIRPORT_table_drop = "drop table if exists Capstone.AIRPORT"
STATE_table_drop = "drop table if exists Capstone.STATE"
MODEL_table_drop = "drop table if exists Capstone.MODE"
COUNTRY_table_drop = "drop table if exists Capstone.COUNTRY"



# CREATE TABLES
staging_immigrants_table_create= ("""
create table Capstone.immigrants
    (
     IMMIGRANT_ID  int ,
    ARRIVAL_YEAR  int,
    ARRIVAL_MONTH  int,
    COUNTY_CITIZEN  int,
    COUNTY_RESIDENCE  int,
    PORT_ID  varchar(10),
    ARRIVAL_DATE  date,
    MODE_ID int,
    STATE_ID  varchar(10),
    AGE  int,
    VISA_ID  int,
    MATCH_FLAG  varchar(20),
    BIRTH_YEAR  int,
    GENDER  varchar(10),
    AIRLINE  varchar(10),
    ADMISSION_NUMBER  bigint,
    FLIGHT_NO  varchar(20),
    VISA_TYPE  varchar(20)
    
    );

""")
#DEMOGRAPHIC_ID INT IDENTITY (1,1) PRIMARY KEY ,
staging_DEMOGRAPHICS_table_create = ("""
create table Capstone.DEMOGRAPHICS
    (
DEMOGRAPHIC_ID INT IDENTITY (1,1) PRIMARY KEY ,
CITY  varchar(100),
STATE  varchar(80),
MEDIAN_AGE  float,
MALE_POPULATION  float,
FEMALE_POPULATION  float,
TOTAL_POPULATION  bigint,
FOREIGN_BORN  float,
AVE_HOUSEHOLD  float,
STATE_ID  varchar(10),
RACE varchar(50),
POP_COUNT  int
    );
""")

AIRPORT_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.AIRPORT
  (
     ident varchar(10) ,
     AIRPORT_TYPE varchar(25),
     AIRPORT_NAME varchar(200),
     elevation_ft float,
     continent varchar(10),
     COUNTRY varchar(10),
     iso_region varchar(10),
     MUNICIPALITY varchar(100),
     gps_code varchar(10),
	 iata_code varchar(10),
	 local_code varchar(10),
	 coordinates varchar(100),
	 STATE_ID varchar(10)
  );  
""")

PORT_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.PORT
  (
     PORT_ID  varchar(10) NOT NULL  distkey,
     PORT_Desc  varchar(100) NOT NULL
  ); 
""")

VISA_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.VISA
  (
     VISA_ID  int NOT NULL PRIMARY KEY,
     VISA_Desc  varchar(25) NOT NULL
  ); 
""")

STATE_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.STATE
  (
     STATE_ID  varchar(10) NOT NULL PRIMARY KEY,
     STATE  varchar(80) NOT NULL
  );  
""")

COUNTRY_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.COUNTRY
  (
     COUNTRY_ID  int NOT NULL  distkey,
     COUNTRYT_Desc  varchar(80) NOT NULL
  ); 
""")

MODEL_table_create = ("""
CREATE TABLE IF NOT EXISTS Capstone.MODE
  (
     MODE_ID  INT NOT NULL  distkey,
     MODE_Desc  varchar(50) NOT NULL
  ); 
""")

# STAGING TABLES

#staging_events_copy = ("""
#copy Capstone.VISA
#from 's3://capstone-kobap/' 
#credentials 'aws_iam_role={}' 
#CSV 's3://capstone-kobap/VISA.csv delimiter '|''   region 'us-west-2';
#""").format(ARN)



VISA_copy = ("""
copy Capstone.VISA from 's3://capstone-kobap/VISA.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    removequotes
    delimiter '|';
    """).format(key,secret)

state_copy = ("""
copy Capstone.STATE from 's3://capstone-kobap/STATE.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    removequotes
    delimiter '|';
    """).format(key,secret)

model_copy = ("""
copy Capstone.MODE from 's3://capstone-kobap/Model.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    removequotes
    delimiter '|';
    """).format(key,secret)

port_copy = ("""
copy Capstone.PORT from 's3://capstone-kobap/PORT.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    removequotes
    delimiter '|';
    """).format(key,secret)


country_copy = ("""
copy Capstone.COUNTRY from 's3://capstone-kobap/COUNTRY.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    removequotes
    delimiter '|';
    """).format(key,secret)

AIRPORT_copy = ("""
copy Capstone.AIRPORT from 's3://capstone-kobap/airport.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    ESCAPE
    delimiter '|';
    """).format(key,secret)


DEMOGRAPHICS_copy = ("""
copy Capstone.DEMOGRAPHICS from 's3://capstone-kobap/DEMOGRAPHICS.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    ESCAPE
    delimiter '|'
    ;
    """).format(key,secret)


IMMIGRATION_copy = ("""
copy Capstone.immigrants from 's3://capstone-kobap/IMMIGRATION.CSV'
    access_key_id '{0}'
    secret_access_key '{1}'
    region 'us-west-2'
    ignoreheader 1
    delimiter '|'
    ACCEPTINVCHARS ESCAPE 
    dateformat 'auto'
    ;
    """).format(key,secret)

#get tables count
IMMIGRATION_count = ("SELECT count(*) FROM Capstone.immigrants;")
DEMOGRAPHICS_count = ("SELECT count(*) FROM Capstone.DEMOGRAPHICS;")
PORT_count = ("SELECT count(*) FROM Capstone.PORT;")
VISA_count = ("SELECT count(*) FROM Capstone.VISA;")
AIRPORT_count = ("SELECT count(*) FROM Capstone.AIRPORT;")
STATE_count = ("SELECT count(*) FROM Capstone.STATE;")
MODE_count = ("SELECT count(*) FROM Capstone.MODE;")
COUNTRY_count = ("SELECT count(*) FROM Capstone.COUNTRY;")

#

## QUERY LISTS
set_schema=[CREATE_SCHEMA,SET_SCHEMA]
create_table_queries = [staging_immigrants_table_create, staging_DEMOGRAPHICS_table_create, AIRPORT_table_create, PORT_table_create, VISA_table_create, STATE_table_create,COUNTRY_table_create,MODEL_table_create]
drop_table_queries = [staging_immigrants_table_drop, staging_DEMOGRAPHICS_table_drop,  AIRPORT_table_drop, PORT_table_drop, VISA_table_drop, STATE_table_drop,MODEL_table_drop,COUNTRY_table_drop]
copy_table_queries = [VISA_copy,state_copy,model_copy,port_copy,country_copy,AIRPORT_copy,DEMOGRAPHICS_copy,IMMIGRATION_copy]
count_table_queries = [ IMMIGRATION_count,DEMOGRAPHICS_count, PORT_count, VISA_count,AIRPORT_count,STATE_count,MODE_count,COUNTRY_count]
#