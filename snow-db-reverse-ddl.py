import os
import shutil
import snowflake.connector
import argparse
import logging
import datetime

class SnowflakeDatabaseSchemasDdlExtractor:
    """
    Class to extract all ddl objects for a list of Snowflake database schemas
    """
    
    """
    Object constructor 
    :param pArgs: key pair value dictionnnary with 
    :param pSfAccount: Snowflake account  
    :param pSfUser: User
    :param pSfPwd: Password
    :param pSfRole: Role
    :param pDatabaseName : Database name
    :param pSchemas : list of schemas
    """
    def __init__(self, **pArgs):
        #init snowflake connection
        self.__aSfConn = snowflake.connector.connect(
            account = pArgs.get("account"),
            user = pArgs.get("user"),
            password = pArgs.get("password"),
            role = pArgs.get("role")
        )

        # set Database to scan
        self.SetDatabaseToScan(pArgs.get("database"))
        
        # set Schemas to scan
        self.SetSchemasToScan(pArgs.get("schemas"))

        # array of object types which support get_ddl
        self.__aSchemaObjectTypes = [
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


    """
    Set database name to scan
    :param pDatabaseName: database name
    """
    def SetDatabaseToScan(self, pDatabaseName):        
        self.__aDatabaseName = pDatabaseName

    """
    Set schemas to scan
    :param pSchemaNames: array of schemas names, empty array to scan all schemas
    """
    def SetSchemasToScan(self, pSchemaNames):
        self.__aSchemas = list()

        # get all schema for the database
        vSchemas = self.GetDatabaseObjectsByType("schemas")
        
        # loop on all schemas or only on those specified in pSchemaNames and except the Snowflake system schema named INFORMATION_SCHEMA
        for vSchema in vSchemas:
            if (len(pSchemaNames)==0 or (vSchema[1].lower() in (string.lower() for string in pSchemaNames))) and vSchema[1]!="INFORMATION_SCHEMA":
                self.__aSchemas.append(vSchema)

    """
    Re-create output folder
    :param pOutputFolder: the output folder
    """
    def EmptyOutputFolder(self, pOutputFolder):
        # re create empty target folder
        try:
            shutil.rmtree(pOutputFolder)
        except OSError:
            pass
        finally:
            os.makedirs(pOutputFolder, exist_ok=True)

    """
    Execute a sql statement and pass the result set back
    :param pSqlStmt: sql statement to execute 
    :param pFetchAll: boolean True => fetch all, False => fetch one
    :returns: The Sql statement result set 
    """
    def GetSfStmtResultSet(self, pSqlStmt, pFetchAll):
        # create a cursor on Snowflake connection in order to execute pSqlStmt and get the result set
        vCursForSqlStmt = self.__aSfConn.cursor()
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

    """
    Get list of objects according to a specific type
    :param pSfDatabaseName: Database name
    :param pSfObjectType: Object type
    :param pSfShemaName: schema name
    :returns: list of objects
    """
    def GetDatabaseObjectsByType(self, pSfObjectType, pSfShemaName=""):
        # build Snowflake Show statement depending on pSfObjectType parameter
        vSqlStmt=""
        if pSfObjectType=="schemas":
            vSqlStmt = f'show {pSfObjectType} in database "{self.__aDatabaseName}"'
        else:
            vSqlStmt =  f'show {pSfObjectType} in schema "{self.__aDatabaseName}"."{pSfShemaName}"'
        
        # execute and return the Show sql statement
        vObjects = self.GetSfStmtResultSet(vSqlStmt, True)

        return vObjects

    """
    Save a ddl script in file
    :param pFileName: target file name
    :param pDdlScript: the ddl script to save
    :param pEnvPatternToReplaceInDdl: environment pattern to replace for SnowSQL variable substitution
    :param pEnvPatternReplaceTokenInDdl: environment replace token for SnowSQL variable substitution
    """
    def SaveDdlScript(self, pFileName, pDdlScript, pEnvPatternToReplaceInDdl, pEnvPatternReplaceTokenInDdl):
        # save the DDL script as SnowSQL script
        if len(pDdlScript)>0:
            os.makedirs(os.path.dirname(pFileName), exist_ok=True)
            with open(pFileName, "wt") as vDdlFile:
                # add SnowSql set variable_substitution for each scripts
                vDdlFile.write('!set variable_substitution=true;\n')
                
                # add the DDL command
                vDdlFile.write(f'{pDdlScript.replace(pEnvPatternToReplaceInDdl,pEnvPatternReplaceTokenInDdl)}\n')


    """
    return the dll script for a specific object
    :param pSfConn: Snowflake connection
    :param pSfDatabaseName: Database name
    :param pSfSchemaName: Schema name
    :param pSfOjectType: object type 
    :param pSfOjectName: object name
    :returns: ddl script as string
    """
    def GetObjectDdl(self, pSfSchemaName, pSfOjectType, pSfOjectName, pSfOjectArguments, pCreateOrReplace):
        if pSfOjectType.lower()=="user function":
            vSfOjectType="function"
        elif pSfOjectType.lower()=="file format":
            vSfOjectType="file_format"
        else:
            vSfOjectType=pSfOjectType

        # Build dynamically Snowflake get_ddl statement
        vSqlStmtToGettDdl=f'SELECT GET_DDL(\'{vSfOjectType}\',\'"{self.__aDatabaseName}"."{pSfSchemaName}"."{pSfOjectName}"{pSfOjectArguments}\',true)'

        # manage create or replace/ create if not exists statement depending on pCreateOrReplace parameter
        vSqlStmtDdl=self.GetSfStmtResultSet(vSqlStmtToGettDdl, False)[0]
        if pCreateOrReplace==False:
            vSqlStmtDdl=vSqlStmtDdl.replace(f'create or replace {pSfOjectType.upper()} ',f'create {pSfOjectType.upper()} if not exists ',1)

        return vSqlStmtDdl

    """
    Extract all ddl objects for the database schemas and save them as sql scripts
    :param pOutputFolder: root path where the ddl scripts will be saved
    :param pEnvPatternToReplaceInDdl:  pattern to replace for SnowSQL variable substitution to parametrize environment
    :param pEnvPatternReplaceTokenInDdl: replace token for SnowSQL variable substitution to parametrize environment
    """
    def GenerateDbDdlScripts(self, pOutputFolder, pEnvPatternToReplaceInDdl, pEnvPatternReplaceTokenInDdl):
        # Empty output folder
        self.EmptyOutputFolder(pOutputFolder)

        # Generate declarative Ddl script for the pSfDatabaseName database
        self.SaveDdlScript(
            os.path.join(pOutputFolder, f'00_{self.__aDatabaseName}.sql'),
            f'CREATE DATABASE IF NOT EXISTS "{self.__aDatabaseName}";',
            pEnvPatternToReplaceInDdl,
            pEnvPatternReplaceTokenInDdl
            )

        # loop over the schemas to scan
        for vSchema in self.__aSchemas:
            # Generate declarative Ddl script for the schema
            vSchemaFolder=os.path.join(pOutputFolder, f'01_{vSchema[1]}')
            self.SaveDdlScript(
                os.path.join(vSchemaFolder, f'00_{vSchema[1]}.sql') ,
                f'CREATE SCHEMA IF NOT EXISTS "{self.__aDatabaseName}"."{vSchema[1]}" DATA_RETENTION_TIME_IN_DAYS={vSchema[8]} COMMENT=\'{vSchema[6]}\';',
                pEnvPatternToReplaceInDdl,
                pEnvPatternReplaceTokenInDdl
                )
            
            # loop over object types in the schema
            for vSchemaObjectType in self.__aSchemaObjectTypes:
                # get list of objects by type using the show command
                vObjects = self.GetDatabaseObjectsByType(vSchemaObjectType["name"], vSchema[1])
            
                # loop over each object for the given type
                for vObject in vObjects:
                    # exclude built-in procedures objects
                    if (vSchemaObjectType["name"]=="procedures" and vObject[3]!="Y") or vSchemaObjectType["name"]!="procedures": 
                        # get arguments for procedures and user functions
                        vSchemaObjectTypeArg="" 
                        if vSchemaObjectType["name"] in ["procedures","user functions"]:
                            vSchemaObjectTypeArg=vObject[8][:vObject[8].index("RETURN")].replace(vObject[1],"")
                            
                        # Generate and save declarative Ddl script for each object
                        self.SaveDdlScript(
                            os.path.join(vSchemaFolder, f'{vSchemaObjectType["order"]}_{vSchemaObjectType["name"]}/00_{vObject[1]}.sql'),
                            self.GetObjectDdl(vSchema[1], vSchemaObjectType["name"][:len(vSchemaObjectType["name"])-1], vObject[1],vSchemaObjectTypeArg,vSchemaObjectType["createOrReplace"]),
                            pEnvPatternToReplaceInDdl,
                            pEnvPatternReplaceTokenInDdl
                            )

"""
parse and control command arguments
:returns: array of command arguments
"""
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

"""
main
"""
if __name__ == '__main__':
    logging.basicConfig(filename='./snow-db-reverse-ddl.log', level=logging.INFO)
    logging.info(f'Started at {datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")}')

    #parse command arguments
    vCmdArgs = CmdArgParser()

    # init SnowflakeDatabaseSchemasDdlExtractor with connection to snowflake, database and schemas to scan
    vSfDbSchemaDdlExtractor = SnowflakeDatabaseSchemasDdlExtractor(
        account=vCmdArgs.account, 
        user=vCmdArgs.user, 
        password=vCmdArgs.password, 
        role=vCmdArgs.role, 
        database=vCmdArgs.database, 
        schemas=[] if vCmdArgs.schemas is None else [schema for schema in vCmdArgs.schemas.split(',')] 
        )

    # generate ddl for each objects of each database schemas 
    vSfDbSchemaDdlExtractor.GenerateDbDdlScripts(vCmdArgs.folder,vCmdArgs.envPattern,vCmdArgs.envReplaceToken)

    logging.info(f'Ended at {datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")}')
