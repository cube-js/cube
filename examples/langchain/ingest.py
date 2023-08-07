import pickle
import jwt
import os

from dotenv import load_dotenv
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores.faiss import FAISS

from langchain.document_loaders import CubeSemanticLoader

load_dotenv()


def ingest_cube_meta():
    api_url = os.environ["CUBE_API_URL"]
    cubejs_api_secret = os.environ["CUBE_API_SECRET"]
    security_context = {}
    api_token = jwt.encode(security_context, cubejs_api_secret, algorithm="HS256")

    loader = CubeSemanticLoader(api_url, api_token)
    documents = loader.load()

    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_documents(documents, embeddings)

    # Save vectorstore
    with open("vectorstore.pkl", "wb") as f:
        pickle.dump(vectorstore, f)


if __name__ == "__main__":
    ingest_cube_meta()
