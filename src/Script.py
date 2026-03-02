from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=api_key)

query = "Hello! Say hello back in one sentence."

response = client.responses.create(
    model="gpt-4.1-mini",
    input=query
    )
print(response.output_text)
