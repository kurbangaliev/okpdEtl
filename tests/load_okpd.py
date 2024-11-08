from concurrent.futures import ThreadPoolExecutor
import requests

def download_file(url, filename, extractpath):
    response = requests.get(url)
    # if "content-disposition" in response.headers:
    #     content_disposition = response.headers["content-disposition"]
    #     #filename = content_disposition.split("filename=")[1]
    # else:
    #     #filename = url.split("/")[-1]
    with open(extractpath + filename, mode="wb") as file:
        file.write(response.content)
    print(f"Downloaded file {filename}")

download_file("https://ofdata.ru/open-data/download/okpd_2.json.zip", "okpd_2.json.zip", "data/")