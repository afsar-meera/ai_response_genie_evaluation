import gc
import time
from typing import Union
import pyodbc
import numpy as np
import pandas as pd
from pandas import DataFrame
from config import MSSQL_AUTH, ENVIRON

CLIENT = None

if ENVIRON in ["DEVELOPMENT", "PRODUCTION"]:
    READ_DSN = "ReadOnlySQLServer"
    WRITE_DSN = "ReadWriteSQLServer"
elif ENVIRON == "LOCAL":
    READ_DSN = "TestMSSQLServerDatabase"
    WRITE_DSN = "TestMSSQLServerDatabase"
else:
    raise ValueError("invalid ENVIRON value received")


class MssqlHandler:
    def __init__(self, instance_type, force_local=False):
        if instance_type not in ["r", "rw"]:
            raise ValueError("invalid database instance type")
        self.instance_type = instance_type
        self.mssql_conn = None
        self.mssql_cursor = None
        self.retry_sleep = 5
        if force_local:
            self.DSN = "TestMSSQLServerDatabase"
        else:
            if self.instance_type == "r":
                self.DSN = READ_DSN
            else:
                self.DSN = WRITE_DSN
        try:
            print(f"[INFO] connection request from {self.__class__.__name__}")
            print(f"[INFO] connecting to DSN {self.DSN}, in mode: {self.instance_type}")
            self.reinit_db()
        except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
            print(f"First time Connection to MSSQL Database via MssqlHandler failed, retrying " \
                  f"{2} times")

            for number in range(1, 3):
                try:
                    print(f"[INFO] connecting to the db for {number}")
                    self.reinit_db()
                    break
                except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
                    self.mssql_conn = None
                    self.mssql_cursor = None
                    time.sleep(self.retry_sleep)

            if not self.mssql_conn:
                raise ConnectionError("Failed to connect to MsSQL db")

    def reinit_db(self):
        self.mssql_conn = pyodbc.connect(f'DSN={self.DSN};UID={MSSQL_AUTH[self.instance_type]["username"]}',
                                         password=MSSQL_AUTH[self.instance_type]['password'], readonly=True)
        self.mssql_cursor = self.mssql_conn.cursor()

    def reinit_cursor(self):
        try:
            self.mssql_cursor = self.mssql_conn.cursor()
        except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
            self.reinit_db()

    def execute(self, query: str, values: Union[int, str, tuple] = ()):
        self.reinit_cursor()
        query = "\n".join([s.strip() for s in query.split("\n") if s.strip() and s != "\n"])
        print("QUERY:", query, "\n")
        try:
            self.mssql_cursor.execute(query, values)
        except (pyodbc.ProgrammingError, pyodbc.DataError) as e:
            raise NotImplementedError(f"wrong query being executed, {e}")
        except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError) as e:
            print(f"Query Execution failed {query}, error => {e}, retrying connection")
            self.retry_connection(query=query)
        gc.collect()

    def commit(self):
        self.mssql_conn.commit()

    def retry_connection(self, query: str):
        for number in range(1, 4):
            try:
                print(f"[INFO] connecting to db for {number}")
                self.reinit_cursor()
                if isinstance(self.mssql_cursor, pyodbc.Cursor):
                    self.mssql_cursor.execute(query)
                else:
                    raise pyodbc.OperationalError("Cursor uninitialized")
                break
            except (pyodbc.OperationalError, pyodbc.InternalError, pyodbc.IntegrityError, AttributeError):
                self.mssql_conn = None
                self.mssql_cursor = None
                time.sleep(2)

        if not self.mssql_cursor:
            print(f"In retrying connection, cursor initialisation failed, " \
                  f"query = {query}, after {2} retries")

        return True

    def get_df(self, df):
        if len(df.index) > 0:
            remaining_df = df
        else:
            if len(df.columns) > 0:
                columns = []
                rows = []
                for i in range(len(self.mssql_cursor.description)):
                    columns.append(self.mssql_cursor.description[i][0])
                    rows.append(None)
                dataframe = dict(zip(columns, rows))
                remaining_df = pd.DataFrame(dataframe, index=[np.arange(0, 1)])
            else:
                remaining_df = "Invalid response from db"
        return remaining_df

    def fetch_df(self):
        if self.mssql_cursor.description is None:
            rem_df = "Invalid response from db"
        else:
            headers = [tup[0] for tup in self.mssql_cursor.description]
            df = DataFrame.from_records(self.mssql_cursor.fetchall(), columns=headers)
            rem_df = self.get_df(df)
        return rem_df

    def get_2nd_df(self):
        self.mssql_cursor.nextset()
        rem_df = self.fetch_df()
        return rem_df

    def close(self):
        self.mssql_cursor.close()
        self.mssql_conn.close()
