---
title: Connecting to Streamlit
permalink: /config/downstream/streamlit
---

<InfoBox>

The SQL API and Extended Support for BI Tools workshop is on June 22nd at 9-10:30 am PT! You'll have the opportunity to learn the latest on Cube's [SQL API](https://cube.dev/blog/expanded-bi-support/). 

You can register for the workshop at [the event page](https://cube.dev/events/sql-api). 👈

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
