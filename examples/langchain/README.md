# Tabular Data Retrieval 

This is an example of a chatbot built with Cube, Langchain, Snowflake and Streamlit. 

[Check this app deployed on Streamlit Cloud.](https://cube-langchain.streamlit.app/)

## Why Semantic Layer for LLM-powered apps?

When building text-to-SQL applications, it is crucial to provide LLM with rich context about underlying data model. Without enough context  it’s hard for humans to comprehend data, LLM will simply compound on that confusion to produce wrong answers. 

In many cases it is not enough to feed LLM with database schema and expect it to generate the correct SQL. To operate correctly and execute trustworthy actions, it needs to have enough context and semantics about the data it consumes; it must understand the metrics, dimensions, entities, and relational aspects of the data by which it's powered. Basically—LLM needs a semantic layer.

![architecture](https://ucarecdn.com/32e98c8b-a920-4620-a8d2-05d57618db8e/)

[Read more on why to use a semantic layer with LLM-power apps.](https://cube.dev/blog/semantic-layer-the-backbone-of-ai-powered-data-experiences)




## Getting Started

- **Cube project**. If you don't have a Cube project already, you follow [this tutorial](https://cube.dev/docs/product/getting-started/cloud) to get started with with sample e-commerce data model.
- **OpenAI API**. This example uses OpenAI API, so you'll need an OpenAI API key.
- Make sure you have Python version >= 3.8
- Install dependencies: `pip install -r requirements.txt`
- Copy `.env.example` as `.env` and fill it in with your credentials. You need OpenAI API Key and credentials to access your Cube deployment.
- Run `streamlit run streamlit_app.py`

## Community
If you have any questions or need help - please [join our Slack community](https://slack.cube.dev/?ref=langchain-example-readme) of amazing developers and data engineers.
