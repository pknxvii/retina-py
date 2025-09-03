import uuid

def generate_doc_id() -> str:
    return str(uuid.uuid4())
