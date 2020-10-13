import requests
from fake_useragent import UserAgent

# search_listings(parameters={"place_name": "zone 1"})


ua = UserAgent()

user_agent = ua.chrome

url = 'http://api.nestoria.co.uk/api?place_name=zone+1&action=search_listings&country=uk&encoding=json&number_of_results=20&sort=newest&listing_type=rent'

url2 = 'https://api.nestoria.co.uk/api?country=uk&pretty=1&action=metadata&place_name=Clapham&price_type=fixed&encoding=json'

headers = {
    'User-Agent': user_agent
}

response = requests.get(url2, headers=headers)

print(response.json())
