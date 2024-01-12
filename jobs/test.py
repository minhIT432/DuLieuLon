import httpx

def send_comment_to_api(comment):
    url = "http://host.docker.internal:5000/predict"  # Điều chỉnh URL nếu cần thiết
    data = {"text": comment}

    try:
        response = httpx.post(url, data=data)
        response.raise_for_status()  # Nếu có lỗi, raise exception
        result = response.json()
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}

# Sử dụng hàm để gửi comment và in ra kết quả
comment_to_send = "so deleciuos"
result = send_comment_to_api(comment_to_send)

if result["success"]:
    print(f"Comment: {comment_to_send}")
    print("Prediction Result:", result["result"])
else:
    print("Error:", result["error"])