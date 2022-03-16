---
title: Connecting to Streamlit
permalink: /config/downstream/streamlit
---

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
