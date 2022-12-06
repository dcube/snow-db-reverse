"""..."""
import os
import shutil
import argparse
import logging
import datetime
import getpass
import snowflake.connector

class SnowflakeDatabaseSchemasDdlExtractor:
    """
    Class to extract all ddl objects for a list of Snowflake database schemas
    """

    def __init__(self, **kwargs):
        """
        Object constructor
        :param kwargs: key pair value dictionnnary (account, user, password, role, database name, list of schemas)
        """
         # enter password if not set
        if kwargs["password"] is None or kwargs["password"]=="":
            _password=getpass.getpass(prompt='Enter password:')
        else:
            _password=kwargs["password"]

        #init snowflake connection
        self.__snowflake_connection = snowflake.connector.connect(
            account = kwargs["account"],
            user = kwargs["user"],
            password = _password,
            role = kwargs["role"]
        )

        # set Database to scan
        self.set_database_to_scan(kwargs["database"])

        # set Schemas to scan
        self.set_schemas_to_scan(kwargs["schemas"])

        # array of object types which support get_ddl
        self.__schema_object_types : dict = [
            {"name":"file formats","order":"01","createOrReplace":True},
            {"name":"masking policies","order":"01","createOrReplace":True},
            {"name":"row access policies","order":"01","createOrReplace":True},
            {"name":"tags","order":"01","createOrReplace":True},
            {"name":"sequences","order":"02","createOrReplace":False},
            {"name":"tables","order":"03","createOrReplace":False},
            {"name":"views","order":"04","createOrReplace":True},
            {"name":"pipes","order":"05","createOrReplace":False},
            {"name":"streams","order":"06","createOrReplace":False},
            {"name":"user functions","order":"07","createOrReplace":True},
            {"name":"procedures","order":"08","createOrReplace":True},
            {"name":"tasks","order":"09","createOrReplace":True}
        ]

    def set_database_to_scan(self, _database_name: str) -> None:
        """
        Set database name to scan
        :param _database_name: database name
        """
        self.__database_name = _database_name


    def set_schemas_to_scan(self, _schemas_name : list[str]) -> None:
        """
        Set schemas to scan
        :param _schemas_name: array of schemas names, empty array to scan all schemas
        """
        self.__schemas_to_scan = list()

        # get all schema for the database
        _schemas = self.get_database_objects_by_type("schemas")

        # loop on all schemas or only on those specified in pSchemaNames and except the Snowflake system schema named INFORMATION_SCHEMA
        for _schema in _schemas:
            if (len(_schemas_name)==0 or (_schema[1].lower() in (string.lower() for string in _schemas_name))) and _schema[1]!="INFORMATION_SCHEMA":
                self.__schemas_to_scan.append(_schema)

    def empty_output_folder(self, _output_folder: str) -> None:
        """
        Re-create output folder
        :param _output_folder: the output folder
        """
        # re create empty target folder
        try:
            shutil.rmtree(_output_folder)
        except OSError:
            pass
        finally:
            os.makedirs(_output_folder, exist_ok=True)

    def get_snowflake_sqlstmt_resultset(self, _sqlstmt:str, _fetch_all:bool) -> list[tuple]:
        """
        Execute a sql statement and pass the result set back
        :param _sqlstmt: sql statement to execute
        :param _fetch_all: boolean True => fetch all, False => fetch one
        :returns: The Sql statement result set
        """
        # create a cursor on Snowflake connection in order to execute _sqlstmt and get the result set
        _cursor_sqlstmt = self.__snowflake_connection.cursor()
        _resultset=[""]

        # execute the _sqlstmt and catch the result set
        try:
            _cursor_sqlstmt.execute(_sqlstmt)
            if _fetch_all:
                _resultset = _cursor_sqlstmt.fetchall()
            else:
                _resultset = _cursor_sqlstmt.fetchone()
        except snowflake.connector.errors.ProgrammingError as snowflake_exc:
            print(f"Statement error: {snowflake_exc.msg}")
            print(_sqlstmt)
        finally:
            # close the cursor
            _cursor_sqlstmt.close()

        return _resultset

    def get_database_objects_by_type(self, _snowflake_object_type:str, _snowflake_schema_name:str="") -> list[tuple]:
        """
        Get list of objects according to a specific type
        :param pSfDatabaseName: Database name
        :param _snowflake_object_type: Object type
        :param _snowflake_schema_name: schema name
        :returns: list of objects
        """
        # build Snowflake Show statement depending on _snowflake_object_type parameter
        _sqlstmt=""
        if _snowflake_object_type=="schemas":
            _sqlstmt = f'show {_snowflake_object_type} in database "{self.__database_name}"'
        else:
            _sqlstmt =  f'show {_snowflake_object_type} in schema "{self.__database_name}"."{_snowflake_schema_name}"'

        # execute and return the Show sql statement
        _objects = self.get_snowflake_sqlstmt_resultset(_sqlstmt, True)

        return _objects

    def save_ddl_script(self, _filename:str, _ddlscript:str, _env_pattern:str, _env_pattern_replacement:str) -> None:
        """
        Save a ddl script in file
        :param _filename: target file name
        :param _ddlscript: the ddl script to save
        :param _env_pattern: environment pattern to replace for SnowSQL variable substitution
        :param _env_pattern_replacement: environment replace token for SnowSQL variable substitution
        """
        # save the DDL script as SnowSQL script
        if len(_ddlscript)>0:
            os.makedirs(os.path.dirname(_filename), exist_ok=True)
            with open(_filename, "wt",encoding="utf-8") as _ddl_ouptut_file:
                # add SnowSql set variable_substitution for each scripts
                _ddl_ouptut_file.write('!set variable_substitution=true;\n')

                # add the DDL command
                _ddl_ouptut_file.write(f'{_ddlscript.replace(_env_pattern,_env_pattern_replacement)}\n')


    def get_object_ddl(self, _snowflake_schema_name: str, _snowflake_oject_type: str, _snowflake_oject_name: str, _snowflake_oject_args: str, _create_or_replace:bool) -> str:
        """
        return the dll script for a specific object
        :param _snowflake_schema_name: Schema name
        :param _snowflake_oject_type: object type
        :param _snowflake_oject_name: object name
        :param _snowflake_oject_args: object arguments (usefull for stored proc, adf, udtf)
        :param _create_or_replace: create or replace statement otherwise create if not exists
        :returns: ddl script as string
        """
        if _snowflake_oject_type.lower()=="user function":
            _snowflake_object_type="function"
        elif _snowflake_oject_type.lower()=="file format":
            _snowflake_object_type="file_format"
        else:
            _snowflake_object_type=_snowflake_oject_type

        # Build dynamically Snowflake get_ddl statement
        _sqlstmt_ddl=""
        if _snowflake_object_type != 'stage':
            _sqlstmt_to_get_ddl=f'SELECT GET_DDL(\'{_snowflake_object_type}\',\'"{self.__database_name}"."{_snowflake_schema_name}"."{_snowflake_oject_name}"{_snowflake_oject_args}\',true)'

            # manage create or replace/ create if not exists statement depending on _create_or_replace parameter
            _sqlstmt_ddl=self.get_snowflake_sqlstmt_resultset(_sqlstmt_to_get_ddl, False)[0]
            if not _create_or_replace:
                _sqlstmt_ddl=_sqlstmt_ddl.replace(f'create or replace {_snowflake_oject_type.upper()} ',f'create {_snowflake_oject_type.upper()} if not exists ',1)

        return _sqlstmt_ddl
        

    def generate_db_ddl_scripts(self, _output_folder, _env_pattern, _env_pattern_replacement):
        """
        Extract all ddl objects for the database schemas and save them as sql scripts
        :param _output_folder: root path where the ddl scripts will be saved
        :param _env_pattern:  pattern to replace for SnowSQL variable substitution to parametrize environment
        :param _env_pattern_replacement: replace token for SnowSQL variable substitution to parametrize environment
        """
        # Empty output folder
        self.empty_output_folder(_output_folder)

        # Generate declarative Ddl script for the pSfDatabaseName database
        self.save_ddl_script(
            os.path.join(_output_folder, f'00_{self.__database_name}.sql'),
            f'CREATE DATABASE IF NOT EXISTS "{self.__database_name}";',
            _env_pattern,
            _env_pattern_replacement
            )

        # loop over the schemas to scan
        for _schema in self.__schemas_to_scan:
            # Generate declarative Ddl script for the schema
            _schema_folder=os.path.join(_output_folder, f'01_{_schema[1]}')
            self.save_ddl_script(
                os.path.join(_schema_folder, f'00_{_schema[1]}.sql') ,
                f'CREATE SCHEMA IF NOT EXISTS "{self.__database_name}"."{_schema[1]}" DATA_RETENTION_TIME_IN_DAYS={_schema[8]} COMMENT=\'{_schema[6]}\';',
                _env_pattern,
                _env_pattern_replacement
                )

            # loop over object types in the schema
            for _schema_object_type in self.__schema_object_types:
                # get list of objects by type using the show command
                _objects = self.get_database_objects_by_type(_schema_object_type["name"], _schema[1])

                # loop over each object for the given type
                for _object in _objects:
                    # exclude built-in procedures objects
                    if (_schema_object_type["name"]=="procedures" and _object[3]!="Y") or _schema_object_type["name"]!="procedures":
                        # get arguments for procedures and user functions
                        _schema_object_type_arg=""
                        if _schema_object_type["name"] in ["procedures","user functions"]:
                            _schema_object_type_arg=_object[8][:_object[8].index("RETURN")].replace(_object[1],"")

                        # determine object type for get dll operator
                        _object_type = "policy" if "policies" in _schema_object_type["name"] else _schema_object_type["name"][:len(_schema_object_type["name"])-1]

                        # Generate and save declarative Ddl script for each object
                        self.save_ddl_script(
                            os.path.join(_schema_folder, f'{_schema_object_type["order"]}_{_schema_object_type["name"]}/00_{_object[1]}.sql'),
                            self.get_object_ddl(_schema[1], _object_type, _object[1],_schema_object_type_arg,_schema_object_type["createOrReplace"]),
                            _env_pattern,
                            _env_pattern_replacement
                            )


def cmd_arg_parser():
    """
    parse and control command arguments
    :returns: array of command arguments
    """
    _cmd_arg_parser = argparse.ArgumentParser(description='Database reverse engineering to generate all DDL script as SnowSQL scripts')
    _cmd_arg_parser.add_argument('-f','--folder', action='store', required=True, help='The folder where you want to store the DDLs scripts')
    _cmd_arg_parser.add_argument('-a','--account', action='store', required=True, help='Snowflake account to connect to')
    _cmd_arg_parser.add_argument('-u','--user', action='store', required=True, help='User to connect to Snowflake')
    _cmd_arg_parser.add_argument('-p','--password', action='store', required=False, help='Password')
    _cmd_arg_parser.add_argument('-r','--role', action='store', required=True, help='Role')
    _cmd_arg_parser.add_argument('-d','--database', action='store', required=True, help='Database to explore')
    _cmd_arg_parser.add_argument('-s','--schemas', action='store', required=False, help='List of schemas to explore, comma separated values')
    _cmd_arg_parser.add_argument('-e','--envPattern', action='store', required=False, default="", help='Environment pattern in object name')
    _cmd_arg_parser.add_argument('-t','--envReplaceToken', action='store', required=False, default="", help='Replace token for environment')
    return _cmd_arg_parser.parse_args()


if __name__ == '__main__':
    # Create log dir if not exists
    log_filename=f'./logs/snow_db_reverse_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    os.makedirs(os.path.dirname(log_filename),exist_ok=True)
    logging.basicConfig(filename=log_filename, level=logging.INFO)
    logging.info('Started at %s', datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))

    # parse command arguments
    cmd_args = cmd_arg_parser()

    # init SnowflakeDatabaseSchemasDdlExtractor with connection to snowflake, database and schemas to scan
    snowflake_ddl_extractor = SnowflakeDatabaseSchemasDdlExtractor(
        account=cmd_args.account,
        user=cmd_args.user,
        password=cmd_args.password,
        role=cmd_args.role,
        database=cmd_args.database,
        schemas=[] if cmd_args.schemas is None else [schema for schema in cmd_args.schemas.split(',')]
        )

    # generate ddl for each objects of each database schemas
    snowflake_ddl_extractor.generate_db_ddl_scripts(cmd_args.folder,cmd_args.envPattern,cmd_args.envReplaceToken)

    logging.info('Ended at %s', datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S"))
