---
title: Connecting to Streamlit
permalink: /config/downstream/streamlit
---

<InfoBox>
  <b>The SQL API and Extended Support for BI Tools</b> workshop on June 22, 2022.<br/> 
  You'll have the opportunity to learn the latest on Cube's <a href="https://cube.dev/blog/expanded-bi-support/">SQL API.</a><br /> 
  Check out the agenda and resigter for the workshop today on the <a href="https://cube.dev/events/sql-api">event page</a> 👈
</InfoBox>

You can query Cube from Streamlit notebooks via [SQL API][ref-sql-api].

```python
import pandas
import streamlit
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

"""
# Streamlit + Cube demo!

Query Cube via SQL API from Streamlit.

"""

connection_string = 'mysql+pymysql://user:password@host/db'
with streamlit.echo():
    conn = create_engine(connection_string)
    df = pandas.read_sql_query('SELECT MEASURE(total_sum), status from orders GROUP BY status', conn)
    streamlit.dataframe(df)
```

<div style="text-align: center">
  <img
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/streamlit.png"
    style="border: none"
    width="80%"
  />
</div>

[ref-sql-api]: /backend/sql
