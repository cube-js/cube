import streamlit as st
import datetime
import os
import psycopg2

from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from langchain.docstore.document import Document


def log(message):
    current_time = datetime.datetime.now()
    milliseconds = current_time.microsecond // 1000
    timestamp = current_time.strftime(
        "[%Y-%m-%d %H:%M:%S.{:03d}] ".format(milliseconds)
    )
    st.text(timestamp + message)


def check_input(question: str):
    if question == "":
        raise Exception("Please enter a question.")
    else:
        pass


_postgres_prompt = """\
You are a PostgreSQL expert. Given an input question, create a syntactically correct PostgreSQL query to run and return it as the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per PostgreSQL. 
Never query for all columns from a table. You must query only the columns that are needed to answer the question.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Create meaningful aliases for the columns. For example, if the column name is products_sold.count, you should it as total_sold_products.
Note that the columns with (member_type: measure) are numeric columns and the ones with (member_type: dimension) are string columns.
You should include at least one column with (member_type: measure) in your query.
There are two types of queries supported against cube tables: aggregated and non-aggregated. Aggregated are those with GROUP BY statement, and non-aggregated are those without. Cube queries issued to your database will always be aggregated, and it doesn't matter if you provide GROUP BY in a query or not.
Whenever you use a non-aggregated query you need to provide only column names in SQL:

SELECT status, count FROM orders

The same aggregated query should always aggregate measure columns using a corresponding aggregating function or special MEASURE() function:

SELECT status, SUM(count) FROM orders GROUP BY 1
SELECT status, MEASURE(count) FROM orders GROUP BY 1

If you can't construct the query answer `{no_answer_text}`

Only use the following table: {table_info}

Only look among the following columns and pick the relevant ones: 


{columns_info}

Question: {input_question}


"""

PROMPT_POSTFIX = """\
Return the answer as a JSON object with the following format:

{
    "query": "",
    "filters": [{"column": \"\", "operator": \"\", "value": "\"\"}]
}
"""

CUBE_SQL_API_PROMPT = PromptTemplate(
    input_variables=[
        "input_question",
        "table_info",
        "columns_info",
        "top_k",
        "no_answer_text",
    ],
    template=_postgres_prompt,
)

_NO_ANSWER_TEXT = "I can't answer this question."


def call_sql_api(sql_query: str):
    load_dotenv()
    CONN_STR = os.environ["DATABASE_URL"]

    # Initializing Cube SQL API connection)
    connection = psycopg2.connect(CONN_STR)
    
    cursor = connection.cursor()
    cursor.execute(sql_query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    return columns, rows


def create_docs_from_values(columns_values, table_name, column_name):
    value_docs = []

    for column_value in columns_values:
        print(column_value)
        metadata = dict(
            table_name=table_name,
            column_name=column_name,
        )

        page_content = column_value
        value_docs.append(Document(page_content=page_content, metadata=metadata))

    return value_docs
