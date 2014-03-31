azure-tables-hadoop
===================
Description 
===================
A Hadoop input format and a Hive storage handler so that you can access data stored in Windows Azure Storage tables from within a Hadoop (or HdInsight) cluster. 


Usage 
===================
From Hive 
Make sure the jar is in hive.aux.jars.path (add it in your hive-site.xml). Example:

  <property>
    <name>hive.aux.jars.path</name>
    <value>file:///c:/azure-tables-hadoop/target/microsoft-hadoop-azure-0.0.1.jar</value>
  </property>
Create the table like this:

CREATE EXTERNAL TABLE az_table(intField int, stringField string) 
STORED BY 'com.microsoft.hadoop.azure.hive.AzureTableHiveStorageHandler'
TBLPROPERTIES(
"azure.table.name" = "<tableName>",
"azure.table.account.uri" = "http://<account>.table.core.windows.net",
"azure.table.storage.key" = "<key>"
);

Caveats
===================
Column names should match (case doesn't matter)
Only {string, int, bigint, double, boolean} types are supported
Storing data into the table is not supported

From Map-Reduce 
===================
There's an example in the code, SixCounter, that you can use to guide you. Short story: use AzureTableConfiguration.configureInputTable() to configure the input table, and set your input format as AzureTableInputFormat.