import os
import sys
import argparse
import shutil
import snowflake.connector

# InitSfConn: to connect to snowflake account
# Parameters:
# - pSfAccount: Snowflake account  
# - pSfUser: User
# - pSfPwd: Password
# - pSfRole: Role
# Returns:
# - Snowflake connection
def InitSfConn(pSfAccount, pSfUser, pSfPwd, pSfRole):
    vSfCon = snowflake.connector.connect(
        account = pSfAccount,
        user = pSfUser,
        password = pSfPwd,
        role = pSfRole
    )
    return vSfCon

# GetSfStmtResultSet: to execute a sql statement and pass the result set back
# Parameters:
# - pSfConn: Snowflake connection
# - pSqlStmt: sql statement to execute 
# - pFetchAll: boolean True => fetch all, False => fetch one
# Returns:
# - The Sql statement result set 
def GetSfStmtResultSet(pSfConn, pSqlStmt, pFetchAll):
    # create a cursor on Snowflake connection in order to execute pSqlStmt and get the result set
    vCursForSqlStmt = pSfConn.cursor()
    vSfResultSet=[""]
    
    # execute the pSqlStmt and catch the result set
    try:
        vCursForSqlStmt.execute(pSqlStmt)
        if pFetchAll:
            vSfResultSet = vCursForSqlStmt.fetchall()
        else:
            vSfResultSet = vCursForSqlStmt.fetchone()
    except snowflake.connector.errors.ProgrammingError as e:
        print("Statement error: {0}".format(e.msg))
        print(pSqlStmt)
    except:
        print("Unexpected error: {0}".format(e.msg))
    finally:
        # close the cursor
        vCursForSqlStmt.close()

    return vSfResultSet


# GetDatabaseObjectsByType: get list of objects accordin to a specific type
# Parameters:
# - pSfConn (mandatory): Snowflake connection
# - pSfDatabaseName (mandatory): Database name
# - pSfObjectType (mandatory): Object type
# - pSfShemaName : schema name (mandatory if )
# returns:
# - list of objects
def GetDatabaseObjectsByType(pSfConn, pSfDatabaseName, pSfObjectType, pSfShemaName=""):
    # build Snowflake Show statement depending on pSfObjectType parameter
    vSqlStmt=""
    if pSfObjectType=="schemas":
        vSqlStmt = f'show {pSfObjectType} in database "{pSfDatabaseName}"'
    else:
        vSqlStmt =  f'show {pSfObjectType} in schema "{pSfDatabaseName}"."{pSfShemaName}"'
    
    # execute and return the Show sql statement
    vObjects = GetSfStmtResultSet(pSfConn, vSqlStmt, True)

    return vObjects

# GetObjectDdl: return the dll script for a specific object
# Parameters:
# - pSfConn: Snowflake connection
# - pSfDatabaseName: Database name
# - pSfSchemaName: Schema name
# - pSfOjectType: object type 
# - pSfOjectName: object name
# returns:
# - ddl script as string
def GetObjectDdl(pSfConn, pSfDatabaseName,pSfSchemaName,pSfOjectType,pSfOjectName,pSfOjectArguments,pCreateOrReplace):
    if pSfOjectType.lower()=="user function":
        vSfOjectType="function"
    elif pSfOjectType.lower()=="file format":
        vSfOjectType="file_format"
    else:
        vSfOjectType=pSfOjectType

    # Build dynamically Snowflake get_ddl statement
    vSqlStmtToGettDdl=f'SELECT GET_DDL(\'{vSfOjectType}\',\'"{pSfDatabaseName}"."{pSfSchemaName}"."{pSfOjectName}"{pSfOjectArguments}\',true)'

    # manage create or replace/ create if not exists statement depending on pCreateOrReplace parameter
    vSqlStmtDdl=GetSfStmtResultSet(pSfConn, vSqlStmtToGettDdl, False)[0]
    if pCreateOrReplace==False:
        vSqlStmtDdl=vSqlStmtDdl.replace(f'create or replace {pSfOjectType.upper()} ',f'create {pSfOjectType.upper()} if not exists ',1)

    return vSqlStmtDdl
    
# SaveDdlScript: save a ddl script in file
# Parameters:
# - pFileName: target file name
# - pDdlScript: the ddl script to save
# - pEnvPatternToReplaceInDdl: environment pattern to replace for SnowSQL variable substitution
# - pEnvPatternReplaceTokenInDdl: environment replace token for SnowSQL variable substitution
# returns:
# - N/A
def SaveDdlScript(pFileName, pDdlScript, pEnvPatternToReplaceInDdl, pEnvPatternReplaceTokenInDdl):
    # save the DDL script as SnowSQL script
    if len(pDdlScript)>0:
        os.makedirs(os.path.dirname(pFileName), exist_ok=True)
        with open(pFileName, "wt") as vDdlFile:
            # add SnowSql set variable_substitution for each scripts
            vDdlFile.write('!set variable_substitution=true;\n')
            
            # add the DDL command
            vDdlFile.write(f'{pDdlScript.replace(pEnvPatternToReplaceInDdl,pEnvPatternReplaceTokenInDdl)}\n')
    return

# GenerateDbDdlScripts: extract all ddl objects for a database and save them as sql scripts
# Parameters:
# - pRootFolder: root path where the ddl scripts will be saved
# - pSfConn: the connection to the snowflake account
# - pSfDatabaseName: the database name we want to reverse
# - pSchemaObjectTypes: list of objects types we can to get 
# - pSfShemasFilter: array schemas to extract (empty => extract all)
# - pEnvPatternToReplaceInDdl: environment pattern to replace for SnowSQL variable substitution with &{env}
# returns:
# - N/A
# transform it to use multi threading
def GenerateDbDdlScripts(pRootFolder,pSfConn,pSfDatabaseName,pSchemaObjectTypes,pSfShemasFilter,pEnvPatternToReplaceInDdl,pEnvPatternReplaceTokenInDdl):
    # Generate declarative Ddl script for the pSfDatabaseName database
    SaveDdlScript(
        os.path.join(pRootFolder, f'00_{pSfDatabaseName}.sql'),
        f'CREATE DATABASE IF NOT EXISTS "{pSfDatabaseName}";',
        pEnvPatternToReplaceInDdl,
        pEnvPatternReplaceTokenInDdl
        )

    # get all schemas
    vSchemas=GetDatabaseObjectsByType(pSfConn, pSfDatabaseName, "schemas")
    for vSchema in vSchemas:
        # loop on all schemas or only on those specified in pSfShemasFilter and except the Snowflake system schema named INFORMATION_SCHEMA
        if (len(pSfShemasFilter)==0 or (vSchema[1].lower() in (string.lower() for string in pSfShemasFilter))) and vSchema[1]!="INFORMATION_SCHEMA":
            # Generate declarative Ddl script for the schema
            vSchemaFolder=os.path.join(pRootFolder, f'01_{vSchema[1]}')
            SaveDdlScript(
                os.path.join(vSchemaFolder, f'00_{vSchema[1]}.sql') ,
                f'CREATE SCHEMA IF NOT EXISTS "{pSfDatabaseName}"."{vSchema[1]}" DATA_RETENTION_TIME_IN_DAYS={vSchema[8]} COMMENT=\'{vSchema[6]}\';',
                pEnvPatternToReplaceInDdl,
                pEnvPatternReplaceTokenInDdl
                )
            
            # loop over object types in the schema
            for vSchemaObjectType in pSchemaObjectTypes:
                # get list of objects by type using the show command
                vObjects = GetDatabaseObjectsByType(pSfConn, pSfDatabaseName, vSchemaObjectType["name"], vSchema[1])
                
                # loop over each object for the given type
                for vObject in vObjects:
                    # exclude built-in procedures objects
                    if (vSchemaObjectType["name"]=="procedures" and vObject[3]!="Y") or vSchemaObjectType["name"]!="procedures": 
                        # get arguments for procedures and user functions
                        if vSchemaObjectType["name"] in ["procedures","user functions"]:
                            vSchemaObjectTypeArg=vObject[8][:vObject[8].index("RETURN")].replace(vObject[1],"")
                        else:
                            vSchemaObjectTypeArg=""
                        
                        # Generate and save declarative Ddl script for each object
                        SaveDdlScript(
                            os.path.join(vSchemaFolder, f'{vSchemaObjectType["order"]}_{vSchemaObjectType["name"]}/00_{vObject[1]}.sql'),
                            GetObjectDdl(pSfConn, pSfDatabaseName, vSchema[1], vSchemaObjectType["name"][:len(vSchemaObjectType["name"])-1], vObject[1],vSchemaObjectTypeArg,vSchemaObjectType["createOrReplace"]),
                            pEnvPatternToReplaceInDdl,
                            pEnvPatternReplaceTokenInDdl
                            )
    return

# CmdArgParser: parse and control command arguments
# Parameters:
# - N/A
# returns:
# - array of command arguments
def CmdArgParser():
    vCmdArgParser = argparse.ArgumentParser(description='Database reverse engineering to generate all DDL script as SnowSQL scripts')
    vCmdArgParser.add_argument('-f','--folder', action='store', required=True, help='The folder where you want to store the DDLs scripts')
    vCmdArgParser.add_argument('-a','--account', action='store', required=True, help='Snowflake account to connect to')
    vCmdArgParser.add_argument('-u','--user', action='store', required=True, help='User to connect to Snowflake')
    vCmdArgParser.add_argument('-p','--password', action='store', required=True, help='Password')
    vCmdArgParser.add_argument('-r','--role', action='store', required=True, help='Role')
    vCmdArgParser.add_argument('-d','--database', action='store', required=True, help='Database to explore')
    vCmdArgParser.add_argument('-s','--schemas', action='store', required=False, help='List of schemas to explore, comma separated values')
    vCmdArgParser.add_argument('-e','--envPattern', action='store', required=False, help='Environment pattern in object name')
    vCmdArgParser.add_argument('-t','--envReplaceToken', action='store', required=False, help='Replace token for environment')
    return vCmdArgParser.parse_args() 

# Main
def Main():
    #parse command arguments
    vCmdArgs = CmdArgParser()

    # get command args and set script variables
    vRootFolder = vCmdArgs.folder
    vSfAccount = vCmdArgs.account
    vSfUser = vCmdArgs.user
    vSfPassword = vCmdArgs.password
    vSfRole = vCmdArgs.role
    vDatabaseName = vCmdArgs.database
    vSfShemasFilter = [] if vCmdArgs.schemas is None else [schema for schema in vCmdArgs.schemas.split(',')] 
    vEnvPatternToReplaceInDdl = vCmdArgs.envPattern
    vEnvPatternReplaceTokenInDdl = vCmdArgs.envReplaceToken

    # set object types for which we want to retrieve DDL, dictionnry = [[object type name, order, True: create or replace / False: create if not exists]]
    vObjectTypes = [
        {"name":"file formats","order":"01","createOrReplace":True},
        {"name":"sequences","order":"02","createOrReplace":False},
        {"name":"tables","order":"03","createOrReplace":False},
        {"name":"views","order":"04","createOrReplace":True},
        {"name":"pipes","order":"05","createOrReplace":False},
        {"name":"streams","order":"06","createOrReplace":False},
        {"name":"user functions","order":"07","createOrReplace":True},
        {"name":"procedures","order":"08","createOrReplace":True},
        {"name":"tasks","order":"09","createOrReplace":True}
    ]

    # init connection to snowflake
    vSfCon = InitSfConn(vSfAccount,vSfUser,vSfPassword,vSfRole)
    
    # re create empty target folder
    try:
        shutil.rmtree(vRootFolder)
    except OSError:
        pass
    finally:
        os.makedirs(vRootFolder,exist_ok=True)

    # generate ddl for each objects of each database schemas 
    GenerateDbDdlScripts(vRootFolder,vSfCon,vDatabaseName,vObjectTypes,vSfShemasFilter,vEnvPatternToReplaceInDdl,vEnvPatternReplaceTokenInDdl)

    return

Main()
