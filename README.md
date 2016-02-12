# jdbc-image-tool
Quickly exports/imports database tables using JDBC and Kryo.

## Overview
1. Build the project using maven3: mvn3 install
2. To export data, modify and execute export.bat script
3. To import data, modify and execute import.bat script

## Missing pieces
* unit tests
* directly produce and consume zip file
* exception logging in the code (ToDo items in the code)
* cleanup of generated directory upon failure
* support for other databases (current only Oracle, MSSQL)

