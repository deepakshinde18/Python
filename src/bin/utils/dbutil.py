import ibm_db
import Sybase
from Sybase import DatabaseError
import sqlite3
import os


class DBHandler(object):
    def __init__(self):
        self.db_connection = None
        self.initialise_connection()


    def initialise_connection(self):
        raise NotImplementedError("Mandatory to initialise DB specific connection")


    def execute_stored_procedure(self, sp_name):
        """ execute store procedure and return result
            :return : list of tuples
        """
        if isinstance(self, Sqlite3DBHandler):
            raise AttributeError('Execute stored proc is not avialble for sqlite3 dbs')


    def execute_select_sql(self, sql):
        """ execute select sql and return all data
            :return: list of tuples
        """

        print('Executing sql :- [{}]'.format(sql))
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
        except DatabaseError as e:
            print('Exception while running select sql :- [{}]'.format(str(e)))
            return None
        finally:
            cursor.close()
        print('Number of records return from database :- [{}]'.format(len(data)))
        return data


    def execute_select_sql_get_data_with_header(self, sql):
        """ execute select sql and return all data
            :return: list of tuples
        """

        print('Executing sql :- [{}]'.format(sql))
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            column_names = tuple([item[0] for item in cursor.description])
            data.insert(0, column_names)
        except DatabaseError as e:
            print('Exception while running select sql :- [{}]'.format(str(e)))
            return None
        finally:
            cursor.close()
        print('Number of records return from database :- [{}]'.format(len(data)))
        return data


    def execute_insert_sql(self, sql, sql_params_data=None):
        """ Execute insert sql and return True/False depend on sql executed successfully or not
            :param sql_params_data: array of data used in sql parameter substitution
            return : True/False
        """

        print('Executing sql :- [{}]'.format(sql))
        cursor = self.db_connection.cursor()
        try:
            if sql_params_data is not None and not isinstance(sql_params_data, (list, tuple)):
                raise ValueError('sql_params_data should be either list or tuple')
            elif sql_params_data:
                cursor.execute(sql, sql_params_data)
            else:
                cursor.execute(sql)
        except DatabaseError as e:
            print('Exception while running select sql :- [{}]'.format(str(e)))
            self.db_connection.rollback()
            cursor.close()
            return False
        cursor.close()
        return True


    def execute_sql_get_exception(self, sql):
        """ Execute insert sql and return True/False depend on sql executed successfully or not
            and return exception message
        """

        print('Executing sql :- [{}]'.format(sql))
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(sql)
        except (DatabaseError, Exception) as e:
            print('Exception while running sql :- [{}]'.format(str(e)))
            self.db_connection.rollback()
            cursor.close()
            return False, str(e)
        cursor.close()
        return True, None


    def execute_delete_sql(self, sql):
        return self.execute_insert_sql(sql)


    def execute_count_sql(self, sql):
        if 'count(' not in sql.lower():
            raise ValueError('Only count sql queries are allowed')
        data = self.execute_select_sql(sql)
        val = data[0][0]
        return val


    def close_connection(self):
        if self.db_connection:
            self.db_connection.close()
            print('Closed DB Connection')


class SybaseDBHandler(DBHandler):
    def __init__(self, database_server, database_name):
        self.database_server = database_server
        self.database_name = database_name
        super(self.__class__, self).__init__()


    def initialise_connection(self):
        try:
            db_connection = Sybase.connect(self.database_server, 'username', 'password', self.database_name)
            db_connection.connect()
            cursor = db_connection.cursor()
            print('Setting up the database [{}] as connection'.format(self.database_name))
            cursor.execute('use {}'.format(self.database_name))
            cursor.close()
        exept DatabaseError as e:
            raise DatabaseError('Unable to connect to server {}'.format(self.database_server))
        else:
            print('Succesfully connected to server {} and database {}'.format(self.database_server, self.database_name))
            self.db_connection = db_connection


class DB2DBHandler(DBHandler):
    def __init__(self, database_server):
        self.database_server = database_server
        super(self.__class__, self).__init__()


    def initialise_connection(self):
        try:
            db_connection = ibm_db.connect(self.database_server, 'username', 'password')
        except Exception as e:
            raise Exception('Unableto connect to DB {}'.format(self.database_server))
        else:
            print('Succesfully connected to DB {}'.format(self.database_server))
            self.db_connection = db_connection


class Sqlit3DBHandler(DBHandler):
    def __init__(self, database_file_path):
        self.database_file_path = database_file_path
        super(self.__class__, self).__init__()


    def initialise_connection(self):
        try:
            conn = sqlite3.connect(self.database_file_path, isolation_level=None, timedout=1000)
            conn.text_factory = str
        except sqlite3.DatabaseError as e:
            raise Exception('Unableto connect to DB {}'.format(self.database_file_path))
        else:
            print('Succesfully connected to DB {}'.format(self.database_file_path))
            self.db_connection = conn

