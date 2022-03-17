import os
import mysql.connector
import pandas
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
import warnings
warnings.filterwarnings('ignore')

class CubeUtil:
    def __init__(self):
        self._transport = 'sql'

        # TODO: validation
        connection_string = f'mysql+pymysql://{os.environ["CUBE_SQL_USER"]}:{os.environ["CUBE_SQL_PASSWORD"]}@{os.environ["CUBE_SQL_HOST"]}/{os.environ["CUBE_SQL_DB"]}'
        self.conn = create_engine(connection_string)

        self._meta = Meta(self)

    def run_sql_query(self, query):
        return pandas.read_sql_query(query, self.conn)

    @property
    def meta(self):
        return self._meta

class Meta:
    def __init__(self, cube):
        self.cube = cube
        self._data = None


    @property
    def cubes(self):
        if self._data is None:
           self._data = self.cube.run_sql_query("SHOW TABLES")

        """
        Cube SQL API doesn't support selection for SHOW TABLES yet. Hence we
        need to filter tables only from 'db' schema within pandas now.
        """

        return self._data.loc[self._data['TABLE_SCHEMA'] == 'db']['TABLE_NAME'];

    def members_for_cube(self, cube_name):
        data =  self.cube.run_sql_query(f'DESCRIBE {cube_name}') # TODO: sanitize?
        measures = data.loc[data['Type'].isin(['int'])]['Field'];
        time_dimensions = data.loc[data['Type'].isin(['datetime'])]['Field'];
        dimensions = data.loc[data['Type'].isin(['varchar(255)'])]['Field'];
        return {'dimensions': dimensions, 'time_dimensions': time_dimensions, 'measures': measures}

