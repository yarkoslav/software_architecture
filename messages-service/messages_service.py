from fastapi import FastAPI

messages = FastAPI()

@messages.get("/")
async def root():
    print(f"LOG from MESSAGES: GET")
    return {"messages": "Not implemented"}
