### Overview
We have sample CSV data in src/main/resources/input. There are a total of 50 financial transactions over 5 different dates.
While the first three dates have new transactions, the last two dates also contain update records for historical transactions.

#### Loading
We use ```SpendingActivityTask``` to save data into a delta table.  A merge operation is performed for matching ```txn_id```.

Because we use 5 different source files for each business date, there will be 5 delta log files.

#### Querying
Run ```SpendingActivityQueryTask``` to see the total number of records for each date.

|Version|Rows|
|---|---|
|0|15|
|1|30|
|2|45|
|3|47|
|4|50|

#### Sample Questions
- Find total number of activities by account
- As of version 3, what was transaction id ```txn10``` labelled as?
- What is the latest label of transaction id ```txn10``` and when was it last updated?
- Acc5 bought something from Apple Store Sydney on 2021-03-05, how did the label for this transaction change over time?

#### Querying from Presto/Trino
Create sym link manifest by running ```PrestoTrinoConfigTask```

Create Hive Table and Repair it
```
CREATE DATABASE banking;

CREATE EXTERNAL TABLE banking.transactions(
    account string,
    txn_date date,
    txn_id string,
    merchant string,
    amount float,
    category string,
    last_updated timestamp
) partitioned by (load_date date)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/opt/data/output/activity/_symlink_format_manifest/';

MSCK REPAIR TABLE banking.transactions;
```






