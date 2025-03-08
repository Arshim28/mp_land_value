import requests
import json
from pathlib import Path

district_id = "42"

url = (
    "https://mpbhulekh.gov.in/gisS_proxyURL.do?"
    "http%3A%2F%2F10.115.250.94%3A8091%2Fgeoserver%2Fows%3Fservice%3DWFS"
    "%26version%3D1.0.0%26request%3DGetFeature%26srsName%3DEPSG%3A4326"
    "%26geometryName%3DGEOM%26typeName%3Dmpwork%3AMS_KHASRA_GEOM"
    "%26filter%3D%3CFilter%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3EDISTRICT_ID%3C%2FPropertyName%3E"
    f"%3CLiteral%3E{district_id}%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
    "%26outputFormat%3Djson"
)

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/x-www-form-urlencoded",
    "cookie": "JSESSIONID=imfMOOjOH0hieu7KNXp6ii-HmGfFAH05-8Rg3ej4.gisapp1",
    "dnt": "1",
    "referer": (
        f"https://mpbhulekh.gov.in/MPWebGISEditor/GISKhasraViewerStart?"
        f"distId={district_id}&maptype=villagemap&maptable=MS_KHASRA_GEOM&usertype=login"
    ),
    "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "x-requested-with": "XMLHttpRequest"
}

output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / f"district_{district_id}_full_data.json"

print(f"Fetching all khasra data for district {district_id} with EPSG 4326...")

response = requests.get(url, headers=headers)

if response.status_code == 200:
    try:
        data = response.json()
        feature_count = len(data.get('features', []))
        print(f"Successfully fetched {feature_count} features")
    except requests.exceptions.JSONDecodeError:
        print("Response is not valid JSON. Saving raw content.")
        data = response.text

    with open(output_file, "w", encoding="utf-8") as f:
        if isinstance(data, dict):
            json.dump(data, f, indent=4)
        else:
            f.write(data)

    print(f"Data successfully saved to {output_file}")
else:
    print(f"Error: {response.status_code}")
    if response.content:
        print(f"Response content: {response.text[:500]}...")