# jdbc-image-tool
Quickly exports/imports database tables using JDBC and Kryo. 
Supports Oracle, MSSQL, MySQL/MariaDB and PostgreSQL databases.

## Overview
1. Build the project using maven
   * mvn install
2. Modify the associated import/export script with your database connection parameters
   * jdbc_url
   * jdbc_user
   * jdbc_password
3. Run the script with a zip file as the only argument
   * ./exportPostgres.sh image.zip
   * ./exportMariadb.sh image.zip
   * ./exportMssql.sh image.zip
   * ./exportOracle.sh image.zip
   * ./importMssql.sh image.zip
   * ./importOracle.sh image.zip
   * ./importPostgres.sh image.zip
   * ./importMariadb.sh image.zip
   * ./dumpFile.sh image.zip
      * lists the tables contained in the file, see next item
   * ./dumpFile.sh image.zip#passwd
      * dumps metadata and contents of _passwd_ table stored inside image.zip
   * ./dumpFileHeader.sh image.zip#passwd
      * dumps metadata of _passwd_ table stored inside image.zip

## How it works
The key principles are:
1. Database metadata are used to export/import user+schema tables to/from a zip file with entries 
per exported/imported table.
1. Table and column names are always case-insensitive internally, error is reported when there are more tables 
with the same case-insensitive name.
1. More threads are used speed up data export/import, by default set to the number of (client) 
machine processors; see tool_concurrency parameter in the scripts.
1. The lowest transaction isolation level is used to make database export/import fast. 
1. Concurrent execution requires an extra setup/teardown instructions during data import. 
These vary between database types, they always include disabling/enabling foreign 
key constraints, see src/pz/tool/jdbcimage/main/&lt;DatabaseType&gt;.java for more details.
   * All triggers are disabled on Oracle before data import and then enabled after data import.
   * Oracle sequences that are used to set up identity columns have to be set manually after data import.
   * All foreign key constraints are dropped on Postgress, but a table jdbcimage_create_constraints 
     is created with rows that are used to recreate them after data import.  
   * Identity column sequences are reset to the lowest value after data import on Postgres.
1. Streams of data are used for both export and import to have the lowest memory footprint, typically 256M of heap 
memory is good enough. BLOB, CLOBs and lengthty columns still might require more heap memory depending on data 
and JDBC driver in use, so you also might have to increase java heapsize and/or lower batch size used during 
data import, there are parameters in the scripts to do so. 

## Missing pieces
* tests, review, better organization of shell scripts, error handling of invalid args
* docker support
