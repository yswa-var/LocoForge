from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize the LLM
llm = ChatOpenAI(
    model="gpt-3.5-turbo", 
    temperature=0.9,
    api_key=os.getenv("OPENAI_API_KEY")
) 