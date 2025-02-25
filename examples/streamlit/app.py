import pandas as pd
import numpy as np
import streamlit as st
from utils import CubeUtil
import datetime


"""
# Streamlit + Cube demo!

Query builder with Cube and Streamlit.

"""

cube = CubeUtil()

selected_cube = st.sidebar.selectbox(
    label="Select a cube", options=cube.meta.cubes
)

members = cube.meta.members_for_cube(selected_cube)

selected_measures = st.sidebar.multiselect(
    label="Select measures", options=members['measures'],
    default=members['measures'][0]
)

selected_dimensions = st.sidebar.multiselect(
    label="Select dimensions", options=members['dimensions']
)

selected_time_dimension = st.sidebar.selectbox(
    label="Select time dimension", options=members['time_dimensions'], index=0
)

selected_time_grain = st.sidebar.selectbox(
    label="Select time grain", options=['Day', 'Month', 'Week'], index=0
)

selected_date_from = st.sidebar.date_input("From", datetime.date(2020, 1, 1))
selected_date_to = st.sidebar.date_input("To", datetime.date(2021, 1, 1))

if len(selected_measures) > 0 or len(selected_dimensions) > 0:
    with st.spinner(f'Fetching members for {selected_cube}'):
        members = cube.meta.members_for_cube(selected_cube)
        sql = f'''
            SELECT
                {",".join(selected_measures)},
                {",".join(selected_dimensions)}
                {"," if len(selected_dimensions) > 0 else ""}
                date_trunc('{selected_time_grain.lower()}', {selected_time_dimension})
            FROM {selected_cube}
            WHERE {selected_time_dimension} > '{selected_date_from}' AND {selected_time_dimension} < '{selected_date_to}';
            '''
        df = cube.run_sql_query(sql);
        st.dataframe(df)
else:
    st.write('Select at least one measure or one dimension.')
