import openai

openai.api_key = ""
models = openai.models.list()
print(models)
