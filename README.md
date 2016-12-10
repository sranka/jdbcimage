# jdbc-image-tool
Quickly exports/imports database tables using JDBC and Kryo. 
Supports Oracle, MSSQL and PostgreSQL databases.

## Overview
1. Build the project using maven 3
   * mvn install
2. Modify the associated import/export script with your database connection parameters
   * jdbc_url
   * jdbc_user
   * jdbc_password
3. Run the script with a zip file as the only argument
   * exportMssql image.zip
   * importMssql image.zip
   * importOracle image.zip
   * exportOracle image.zip
   * importPostgresql image.zip
   * exportPostgresql image.zip
   * dumpFile image.zip
      * lists the tables that you can dump, see next item
   * dumpFile image.zip!passwd
      * dumps contents of the passwd table stored inside image.zip

## Missing pieces
* tests, review, better organization of shell scripts, error handling of invalid args
