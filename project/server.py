import requests
import json
def call_predict(request):
    URL = "http://34.67.245.226/predict"
    json_data = request.get_json()
    print("NISHA")
    print(str(json_data))
    r = requests.post(url = URL, data=json.dumps(json_data))
    data = r.json()
    return data

