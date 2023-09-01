import pickle
import jwt
import os

from dotenv import load_dotenv
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores.faiss import FAISS

from langchain.document_loaders import CubeSemanticLoader

load_dotenv()

def ingest_cube_meta():
    security_context = {}
    token = jwt.encode(security_context, os.environ["CUBE_API_SECRET"], algorithm="HS256")

    loader = CubeSemanticLoader(os.environ["CUBE_API_URL"], token)
    documents = loader.load()

    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_documents(documents, embeddings)

    # Save vectorstore
    with open("vectorstore.pkl", "wb") as f:
        pickle.dump(vectorstore, f)


if __name__ == "__main__":
    ingest_cube_meta()
