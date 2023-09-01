# Cube and Langchain demo app

This is an example of a chatbot built with Cube, Langchain, and Streamlit. 

[Why use a semantic layer with LLM for chatbots?](https://cube.dev/blog/semantic-layer-the-backbone-of-ai-powered-data-experiences)

## Pre-requisites

- Valid Cube Cloud deployment. Your data model should have at least one view.
- This example uses OpenAI API, so you'll need an OpenAI API key.
- Python version `>=` 3.8

## How to run

- Install dependencies: `pip install -r requirements.txt`
- Copy `.env.example` as `.env` and fill it in with your credentials
- Run `python ingest.py`. It will use `CubeSemanticLoader` Langchain library to load metadata and save it in vectorstore
- Run `streamlit run main.py`
