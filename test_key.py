import openai

openai.api_key = "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"

models = openai.models.list()
print(models)
