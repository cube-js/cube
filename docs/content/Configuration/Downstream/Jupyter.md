---
title: Connecting to Jupyter
permalink: /config/downstream/jupyter
---

You can query Cube from Jupyter notebooks via [SQL API][ref-sql-api].

```python
from sqlalchemy import create_engine
import warnings
import pandas

warnings.filterwarnings('ignore')

conn = create_engine('mysql+pymysql://user:password@host/db')

data_frame = pandas.read_sql('SELECT MEASURE(total_sum), status from orders GROUP BY status');
```

<img
  src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/2b0d23c8-37fa-4550-8c99-53196c832a26.gif"
  style="border: none"
  width="80%"
/>


[ref-sql-api]: /backend/sql
