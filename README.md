# jdbc-image-tool
Quickly exports/imports database tables using JDBC and Kryo.

## Overview
1. Build the project using maven3: mvn3 install
2. Modify the associated import export script with your database connection paramters
3. Run the script

## Missing pieces
* unit tests
* directly produce and consume zip file
* cleanup of generated directory upon failure
* support for other databases (other than Oracle, MSSQL) and new column types

