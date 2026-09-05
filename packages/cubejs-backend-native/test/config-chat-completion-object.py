from cube import config

config.schema_path = "models"


class NotBridgeable:
    """An arbitrary Python object, standing in for a LangChain chat model."""

    def stream(self, messages):
        return []


# Documents the boundary the two supported forms exist to work around: an
# arbitrary Python object has no cross-language representation, so it reaches
# JavaScript as an unrepresentable reference and throws there.
@config
def chat_completion(request):
    return NotBridgeable()
