import cx_Oracle
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib
import time


conn_oracle = {
    'host': '10.182.50.108',
    # 'host': '10.18.1.80',
    'port': 1521,
    'user': 'ZSMSTPM',
    'psw': 'ZSMSTPM',
    'service': 'CCMDB1'
}

CONN_ORA_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**conn_oracle)
CONN_SQL_STR = '''DRIVER={ODBC Driver 17 for SQL Server};SERVER=WAP119602;DATABASE=CCMDB;UID=ZSMSTPM;PWD=ZSMSTPM'''


class oracle_DB():
    

    def __init__(self):
        pass
    

    ''' Connect to Oracle database '''

    def Oracle_connect(self, querry):
        """
		Connects to database and extracts
		raw data
		Parameters:
		----------------
		arg1: string: SQL query
		Returns: Pandas Dataframe
		----------------
		"""
        try:
            conn = cx_Oracle.connect(CONN_ORA_STR)
            df = pd.read_sql_query(con=conn, sql=querry)
            return df
        except cx_Oracle.DatabaseError as e:
            print(e)
            conn.close()
        finally:
            conn.close()

    def getsetupdata(self):

        query_text ="""
                        select * from SETUP_PASS_TAB
                    """
        try:
            query_result = self.Oracle_connect(query_text)
        except cx_Oracle.DatabaseError as e:
            print(e)
        
        return query_result



if __name__ == "__main__":
    ' initialize oracle database pipline '

    oracle_flow = oracle_DB()
    setup_data = oracle_flow.getsetupdata()

    #setup_data['DTSTRORE'] = setup_data['DTSTRORE'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    print(setup_data.info())

    print(setup_data.head())
    
    ' initialize sql database pipline'
    start_time = time.time()
    params = urllib.parse.quote_plus(CONN_SQL_STR)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str)
    # result = pd.read_sql_query(con=engine,sql='select * from GT_SHIFT')
    # print(result)
    # print(result.describe())
    # print(result.info())   
    #print(setup_data.head())
    #print(setup_data.info())

    'insert data to setup table'
    try:
        setup_data.to_sql(name='SETUP_PASS_TAB', con=engine, if_exists='append', index=False)
        print(" %s seconds ---" % (time.time() - start_time))
    except Exception as identifier:
        print(identifier)
    
    







