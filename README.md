# jdbc-image-tool
Quickly exports/imports all schema tables using JDBC and Kryo. Supports Oracle, MSSQL, MySQL/MariaDB and PostgreSQL databases, typically a zip file is exported from one database type and then imported to another supported database, possibly  of a different type.

## Overview
1. Build the project using maven
   * mvn install
2. Modify the associated import/export script with your database connection parameters
   * jdbc_url
   * jdbc_user
   * jdbc_password
3. Run the script with a zip file as the only argument
   * ./export.sh -Djdbc_url=jdbc:mariadb://localhost:3306/qa -Djdbc_user=root -Djdbc_password=root image.zip
      * See more examples in [exportMariadb.sh](exportMariadb.sh), [exportPostgres.sh](exportPostgres.sh), [exportMssql.sh](exportMssql.sh) and [exportOracle.sh](exportOracle.sh)
   * ./import.sh -Djdbc_url=jdbc:mariadb://localhost:3306/qa -Djdbc_user=root -Djdbc_password=root image.zip
      * See more examples in [importMariadb.sh](importMariadb.sh), [importPostgres.sh](importPostgres.sh), [importMssql.sh](importMssql.sh) and [importOracle.sh](importOracle.sh)
   * ./dumpFile.sh image.zip
      * lists the tables contained in the file, see next item
   * ./dumpFile.sh image.zip#passwd
      * dumps metadata and contents of _passwd_ table stored inside image.zip
   * ./dumpFileHeader.sh image.zip#passwd
      * dumps metadata of _passwd_ table stored inside image.zip

## How it works
The key principles are:
1. More threads are used speed up data export/import, by default set to the number of (client) 
machine processors; see tool_concurrency parameter in the scripts.
1. The lowest transaction isolation level is used to make database export/import faster. 
1. Database metadata are used to export/import all user+schema tables to/from a zip file with entries 
per exported/imported table.
1. Table and column names are always case-insensitive internally, error is reported when there are more tables 
with the same case-insensitive name.
1. Concurrent execution requires an extra setup/teardown instructions during data import. 
These vary between database types, but they always include disabling/enabling foreign 
key constraints, see [DatabaseType.java](src/pz/tool/jdbcimage/main/DatabaseType.java) for more details.
   * All triggers are disabled on Oracle before data import and then enabled after data import.
   * Oracle sequences, when used, are out of scope and usually have to be reset manually after data import.
   * All foreign key constraints are dropped by the tool on Postgress before importing the data, but a table jdbcimage_create_constraints is created with rows that are used to recreate them after data import.  
   * Identity column sequences are reset to the lowest value after data import on Postgres.
1. Streams of data are used for both export and import to have the lowest memory footprint, typically 256M of heap 
memory is good enough. BLOB, CLOBs and lengthty columns still might require more heap memory depending on data 
and JDBC driver in use, so you also might have to increase java heap memory and/or lower batch size used during 
data import, there are parameters in the scripts to do so.
1. The result files and zipped and binary encoded using [KRYO](https://github.com/EsotericSoftware/kryo) to be small. The files can be dumped using [dumpFile.sh](dumpFile.sh) and [dumpFileHeader.sh](dumpFileHeader.sh). Each file contains a header that described the columns and their types, so that the image can be transferrable between database types.
1. The scripts accept several properties as arguments supplied as -Dproperty=value
   * -Djdbc_url=jdbc:mariadb://localhost:3306/qa - JDBC connection string 
   * -Djdbc_user=user 
   * -Djdbc_password=password 
   * -Dignored_tables=a,b,c - used to ignore specific tables during import and export 
   * -Dtool_concurrency=7 - can be used to limit execution threads
   * -Dtool_builddir - build directory used during import export to save/serve table files
   * -Dbatch.size=100 - how many rows to wrap into a batch during table import

## Missing pieces
* tests, review, better organization of shell scripts, error handling of invalid args
* docker support to have a simple packacking of a reusable tool with examples
