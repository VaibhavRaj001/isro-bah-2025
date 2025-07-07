import requests

url = "https://city.imd.gov.in/api/cityweather.php?id=42182"  # Ranchi (example ID)
response = requests.get(url)
print(response.status_code)
print(response.text)
#print(response.json())