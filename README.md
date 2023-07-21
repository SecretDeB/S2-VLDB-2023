# Information-Theoretically Secure and Highly Efficient Search and Row Retrieval

This repository includes the implementation of the S2 framework.

<b> NOTICE:</b> This is an academic proof-of-concept prototype and has not received careful code review. This implementation is NOT ready for production use.

## Repository Structure
```
.
└── S2-VLDB-2023/
    ├── src/                                  * Java code for each algorithm/
    │   ├── _00_Database_Table_Creator/
    │   ├── _01_oneColumnStringSearch/
    │   ├── _02_oneColumnNumberSearch/
    │   ├── _03_AND_Search/
    │   ├── _04_OR_Search/
    │   ├── _05_Multiplicative_Row_Fetch/
    │   └── _06_PRG_Row_Fetch/
    ├── config/                                * Property files for servers/clients
    ├── constant/                              * Constants used in programs
    ├── utility/                                * Contains helper functions
    └── mysqlConnector/                        * Has JAR files for MySQL database

```
## Dependencies
To build S2, one need to install:
* Java (openjdk version "19" has been used for testing)
* MySQL (v8.0.33 has been used for testing)
  
## Setup
### To run locally
Once the `TPCH` dataset shares have been loaded into the `MySQL` database, one can execute each of the programs under the `src/` folder to perform single keyword, conductive, disjunctive or row fetch operations. In order to do so, simply compile all Java programs within the respective programs and run Java programs in the below fashion in separate terminals. For instance, if one wants to execute the program `_01_oneColumnStringSearch` run as below:

```
java -cp .:mysqlConnector/mysql-connector-java-8.0.29.jar src/_01_oneColumnStringSearch/server/Server1.java
java -cp .:mysqlConnector/mysql-connector-java-8.0.29.jar src/_01_oneColumnStringSearch/server/Server2.java
java -cp .:mysqlConnector/mysql-connector-java-8.0.29.jar src/_01_oneColumnStringSearch/combiner/Combiner.java
java -cp .:mysqlConnector/mysql-connector-java-8.0.29.jar src/_01_oneColumnStringSearch/client/Client.java "$columnName,$columnValue"
```

Make sure to bring up the servers and the combiner before running the client program. One can also change the configuration parameters to run the programs on different numbers of threads, number of rows etc. Below shows the input to the client for each program: 

```
_02_oneColumnNumberSearch: "$columnName,$columnValue"
_03_AND_Search: "$columnName1,$columnValue1,$columnName2,$columnValue2"
_04_OR_Search: "$columnName1,$columnValue1,$columnName2,$columnValue2"
_05_Multiplicative_Row_Fetch: "$rowNumber1,$rowNumber2,$rowNumber3"
_06_PRG_Row_Fetch: "$rowNumber1,$rowNumber2,$rowNumber3"
```

Results of each program will be written in files saved under `result/`.

### To run on AWS machines
Similar to running on the local machine, prepare separate AWS machines with required dependencies and data loaded into MySQL. In order to run the programs, change the `port` and `host_ip` for each server based on AWS machines configs and run the same command as mentioned for local setup for program execution.

## Share Creation
In order to create secret shares of `TPCH` dataset, compile and execute `_00_Database_Table_Creator` program as below:

```
java -cp .:mysqlConnector/mysql-connector-java-8.0.29.jar src/_00_Database_Table_Creator/Database_Table_Creator.java $numRows
```

The shares are saved under `data/shares/` for each server. Once shares are generated, load the data into server tables with `serverNo` as `1/2/3/4` with the below schema:

```
CREATE TABLE SERVERTABLE$serverNo (
                                LINENUMBER  INT NOT NULL,
                                PARTKEY    INT NOT NULL,
                                ORDERKEY    INT NOT NULL,
                                SUPPKEY VARCHAR(50) NOT NULL,
                                M_LINENUMBER  INT NOT NULL,
                                M_PARTKEY     INT NOT NULL,
                                M_ORDERKEY    INT NOT NULL,
                                M_SUPPKEY  INT NOT NULL,
                                rowID INT NOT NULL auto_increment primary key
);
```
