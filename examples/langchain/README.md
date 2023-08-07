# Cube and Langchain demo app

This is an example of chatbot built with Cube, Langchaing and Streamlit. 

[Why to use semantic layer with LLM for chatbots?](https://cube.dev/blog/semantic-layer-the-backbone-of-ai-powered-data-experiences)

## Pre-requisites

- Valid Cube Cloud deployment. Your data model should have at least one view.
- This example uses OpenAI API, so you'll need an OpenAI API key.
- Python version `>=` 3.8

## How to run

- Install dependencies: `pip install -r requirements.txt`
- Run `python ingest.py`. It will use `CubeSemanticLoader` Langchain library to load metadata and save it in vectorstore
- Copy `.env.example` as `.env` and fill it in with your credentials
- Run `streamlit run main.py`