import pandas
import streamlit
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

"""
# Streamlit + Cube demo!

Query Cube via SQL API from Streamlit.

"""

connection_string = 'mysql+pymysql://cube@conservation-seeley:b0ae7aa12cc92ec8ea10d197ce8efd0f@conservation-seeley.sql.aws-us-east-2.cubecloudapp.dev/db';
with streamlit.echo():
    conn = create_engine(connection_string)
    df = pandas.read_sql_query('select count, status from Orders', conn)
    streamlit.dataframe(df)
