# Snowflake database reverse tool

## Intro
This Python command line is used to reverse engineer a Snowflake database with an optional list of schemas in order to extract and save DDLs of all non-infrastructure related objects, i.e:
- **file formats**: generated as *create or replace* statement
- **masking policies**: generated as *create or replace* statement
- **row access policies**: generated as *create or replace* statement
- **tags**: generated as *create or replace* statement
- **sequences**: generated as *create if not exists* statement
- **tables**: generated as *create if not exists* statement
- **views**: generated as *create or replace* statement
- **pipes**: generated as *create if not exists* statement
- **streams**: generated as *create if not exists* statement
- **user functions**: generated as *create or replace* statement
- **procedures**: generated as *create or replace* statement
- **tasks**: generated as *create or replace* statement

## Output folder structure
Each DDL is saved in a SnowSQL file, according to the following structure of the destination folder specified in argument:
```
/<dest_folder>
  /00_<db name>.sql
  /01_<schema name #1>
    /00_<schema name #1>.sql
    /01_file format
      /00_<file format name #1>.sql
      ...
      /00_<file format name #n>.sql
    /02_sequences
      /00_<sequence name #1>.sql
      ...
      /00_<sequence name #n>.sql
    /03_tables
      /00_<table name #1>.sql
      ...
      /00_<table name #n>.sql
    /04_views
      /00_<view name #1>.sql
      ...
      /00_<view name #n>.sql
    /05_pipes
      /00_<pipe name #1>.sql
      ...
      /00_<pipe name #n>.sql
    /06_streams
      /00_<stream name #1>.sql
      ...
      /00_<stream name #n>.sql
    /07_user functions
      /00_<user function #1>.sql
      ...
      /00_<user function #n>.sql
    /08_procedures
      /00_<procedure #1>.sql
      ...
      /00_<procedure #n>.sql
    /09_tasks
      /00_<task #1>.sql
      ...
      /00_<task #n>.sql
  ...
  /01_<schema name #n>
```

## Specifities of generated SnowSQL scripts 
All the SnowSQL scripts have the variable substition enabled by adding the header line to ease object name parametrization:
```
!set variable_substitution=true;
```

If your Snowflake objects are nammed according to your different environments (for example: DEV, UAT, PROD), you can set the env pattern and the replace token to parametrize the target environment in your CI/CD process.
For example, for a database called SAMPLE_DEV_DB, the related SnowSQL script should be:
```
!set variable_substitution=true;
CREATE DATABASE IF NOT EXISTS "SAMPLE_&{env}_DB";

```

For more information about the SnowSQL CLI please refer to [Snowflake official documentation](https://docs.snowflake.com/en/user-guide/snowsql-use.html)

## Addtional notes
1. This tool doesn't use any Snowflake compute ressources, it only scans database/schemas metadatas with the [SHOW](https://docs.snowflake.com/en/sql-reference/sql/show.html) command, and extract DDL object with the [GET_DDL](https://docs.snowflake.com/en/sql-reference/functions/get_ddl.html) command
2. Currently, this tool doesn't manage object dependencies, your need to rename your sql files in order to manage the execution order.

## How to use it ?
Install Python 3.9.0 or higher. Clone the **snow-db-reverse** repository.
To get help, you can run the following command:
```
python snow-db-reverse.py -h 
```

### List of arguments

| **Argument** | **Required** | **Description** |
| :--- | :----: | :--- |
| -a or --account | True | Snowflake account to connect to |
| -u or --user | True | User to connect to Snowflake |
| -p or --password | False | User password, if none, password will be asked at runtime |
| -r or --role | True | User role |
| -d or --database | True | Database to explore |
| -s or --schemas | False | List of schemas to explore, comma separated values. Empty for all schemas |
| -e or --envPattern | False | Environment pattern in object name |
| -t or --envReplaceToken | False | Replace token for environment, use the SnowSQL CLI notation if you want to benefit of variable substitution |
| -f or --folder | True | The folder where you want to store the DDLs scripts |

### Example
```
python snow_db_reverse_ddl.py -a "xn001.west-europe.azure" -u "myuser" -p "MyStr0ngP@ssw0rd" -r "myrole" -d "SAMPLE_DEV_DB" -s "MYSCHEMA1_SCH,MYSCHEMA2_SCH" -e "_DEV_" -t "_&{env}_" -f "C:\tmp\snowsql\models\SAMPLE_DB"  
```

