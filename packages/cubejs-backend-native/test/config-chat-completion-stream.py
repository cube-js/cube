from cube import config

config.schema_path = "models"


# Streaming form of the `chat_completion` hook. A Python async generator cannot
# cross the bridge to JavaScript, so a streamed response is handed back as a
# `next` function that yields one chunk per call and None when complete —
# closing over whatever iterator the gateway call produced.
@config
def chat_completion(request):
    tokens = iter(["strea", "med ", request["model"]])

    async def next_chunk():
        try:
            return {"content": next(tokens)}
        except StopIteration:
            return None

    return {"next": next_chunk}
