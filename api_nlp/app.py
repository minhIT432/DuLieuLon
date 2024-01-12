from fastapi import FastAPI, Form
from transformers import pipeline

pipe = pipeline("text-classification", model="cardiffnlp/twitter-roberta-base-sentiment-latest")

app = FastAPI()

@app.post("/predict")
async def predict(text: str = Form(...)):
    data = {"success": False}
    if text:
        # Sử dụng model để dự đoán
        result = pipe(text)
        data["result"] = result
        data["success"] = True
    return data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)