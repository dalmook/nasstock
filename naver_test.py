import requests

client_id = "DrXYoPSPMaSvOcJ04nKV"
client_secret = "L_NVVAbhXH"

url = "https://openapi.naver.com/v1/search/news.json?query=삼성전자&display=5"

headers = {
    "X-Naver-Client-Id": client_id,
    "X-Naver-Client-Secret": client_secret
}

res = requests.get(url, headers=headers)
print(res.json())