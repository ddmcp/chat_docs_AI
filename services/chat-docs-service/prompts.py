import os

def load_prompt(filename: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "prompts", filename)
    with open(path, "r") as f:
        return f.read().strip()
SYSTEM_PROMPT = load_prompt("SYSTEM_PROMPT.txt")