import requests

URL = "http://localhost:4000/docs"

def test_docs_route():
    try:
        response = requests.get(URL)
        response.raise_for_status()
        print("[DOCS] ✅ Successfully accessed API Docs")        
    except requests.exceptions.RequestException as e:
        print("Request Error:", e)