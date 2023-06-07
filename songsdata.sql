--in AWS console we need to open the Amazon RedShift Console
-- "Create cluster" to start creating the Redshift cluster.
 --now we need to open the SQL client tool such as SQL workbench (see the connectivity)
 
 --firstly we need to create the DDL (Data Defination Language) and create the tables
 
 -- Create the staging table for songsdata
CREATE TABLE staging_songsdata (
    song_id VARCHAR(10),
    song_title VARCHAR(25),
    artist_name VARCHAR(25),
    song_duration FLOAT
);

-- Create the staging table for logsdata
CREATE TABLE staging_logsdata (
    ts TIMESTAMP,
    user_id INT,
    level VARCHAR(10),
    session_id INT,
    location VARCHAR(25),
    user_agent VARCHAR(25),
    song_title VARCHAR(25),
    artist_name VARCHAR(25)
);

--we need to run inorder to create the table in Redshift DataBase

Step 3 - Loading the Data into Redshift

--in sql workbench we need connect to RedShift
--do for both songsdata as well as logsdata

COPY staging_songsdata
FROM 's3://afzalsongsdata/songsdata.json'
IAM_ROLE 'Owner'
FORMAT AS JSON 'auto';

COPY staging_logsdata
FROM 's3://afzallogsdata/log_data.json'
IAM_ROLE 'owner'
FORMAT AS JSON 'auto';

 data is loaded into the staging tables, we can perform any necessary data validation and cleansing steps
 
 Use SQL statements, such as INSERT INTO or SELECT INTO, to move the data from the staging tables to the final tables 
 within the Redshift database
 
 -- Create the final song table
CREATE TABLE songsfinaltable (
    song_id VARCHAR(32) PRIMARY KEY,
    song_title VARCHAR(256),
    artist_name VARCHAR(256),
    song_duration FLOAT
);

-- Move data from staging_song_data to songs table
INSERT INTO songsfinaltable (song_id, song_title, artist_name, song_duration)
SELECT song_id, song_title, artist_name, song_duration
FROM staging_song_data;
