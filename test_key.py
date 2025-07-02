import openai

openai.api_key = "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"
models = openai.models.list()
print(models)
