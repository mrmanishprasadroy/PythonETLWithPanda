import cx_Oracle
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import exc
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
CONN_SQL_STR = '''DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.182.52.130\CCMDBDEV;DATABASE=CCMDB;UID=ZSMSTPM;PWD=ZSMSTPM'''


query_text ="""
                        select * from ORDER_PROFILE_ARRAY_TAB
                    """


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
        try:
            query_result = self.Oracle_connect(query_text)
        except cx_Oracle.DatabaseError as e:
            print(e)
        
        return query_result


def get_different_rows(source_df, new_df):
    """
            Returns just the rows from the new dataframe that differ from the source dataframe
            indicator introduce _merge coulmn to dataframe
            choose the new_df row only
            drop the coulmn _merge and return the dataframe
            Parameters:
            ----------------
            arg1: Data Frame: Source Data Frame
            arg1: Data Frame: Source Data Frame
            Returns: Pandas Dataframe with only rows that are not present in source_df
            ----------------
    """
    merged_df = source_df.merge(new_df, indicator=True, how='outer') 
    changed_rows_df = merged_df[merged_df['_merge'] == 'right_only'] 
    return changed_rows_df.drop('_merge', axis=1) 



##if __name__ == "__main__":
def main_etl():
    ' initialize oracle database pipline '

    oracle_flow = oracle_DB()
    df_ora = oracle_flow.getsetupdata()

    #setup_data['DTSTRORE'] = setup_data['DTSTRORE'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    #print(setup_data.shape)

    #print(setup_data.head())
    
    ' initialize sql database pipline'
    params = urllib.parse.quote_plus(CONN_SQL_STR)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    df_sql = pd.read_sql(con=conn_str,sql=query_text)

    df = get_different_rows(df_sql,df_ora)

    print( '{}{}'.format("Number of Rows In table: ", df.shape[0])) # Number  of Rows
    engine = create_engine(conn_str)
        
    'insert data to setup table'
    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql(name='ORDER_PROFILE_ARRAY_TAB', con=engine, if_exists='append', index=False)
        except exc.IntegrityError as identifier:
            print(identifier)

    
    







