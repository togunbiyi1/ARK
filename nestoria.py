import json, requests
from collections import namedtuple
from urllib.parse import urlencode
from fake_useragent import UserAgent

NESTORIA_API_URL = 'http://api.nestoria.co.uk/api?'

ua = UserAgent()

user_agent = ua.chrome

headers = {
    'User-Agent': user_agent
}

api_defaults = {
    'action': 'search_listings',
    'country': 'uk',
    'encoding': 'json',
    'number_of_results': '20',
    'sort': 'newest',
    'listing_type': 'rent'
}

api_elements = [
    'place_name', 'south_west', 'north_east', 'centre_point', 'radius',
    'number_of_results', 'property_type', 'price_max', 'price_min',
    'bedroom_max', 'bedroom_min', 'size_max', 'size_min', 'sort', 'keywords',
    'keywords_exclude', 'action', 'number_of_results', 'country', 'encoding', 'page'
]

property_elements = [
    'auction_date',  'bathroom_number', 'bedroom_number', 'car_spaces',
    'commission', 'construction_year', 'datasource_name', 'guid', 'img_height',
    'img_url', 'img_width', 'keywords', 'latitude', 'lister_name', 'lister_url',
    'listing_type', 'location_accuracy', 'longitude', 'price', 'price_coldrent',
    'price_currency', 'price_formatted', 'price_high', 'price_low', 'price_type',
    'property_type', 'summary', 'thumb_height', 'thumb_url', 'thumb_width',
    'title', 'updated_in_days', 'updated_in_days_formatted'
]

sort_options = [
    'bedroom_lowhigh', 'bedroom_highlow', 'price_lowhigh',
    'price_highlow', 'newest', 'oldest'
]

Property = namedtuple('Property', property_elements)


def _build_search_parameters(parameters={}):
    api_parameters = {}
    for element in [x for x in parameters if x in api_elements]:
        # should validate the data passed in not just the element name - checks parameters keys are in api_elements
        api_parameters[element] = parameters[element]

    for element in api_defaults.keys():
        # adds default elements if not included in parameters
        if element not in api_parameters:
            api_parameters[element] = api_defaults[element]

    return urlencode(api_parameters)


def _get_results(parameters={}):
    api_parameters = _build_search_parameters(parameters)
    url = NESTORIA_API_URL + api_parameters
    print(url)
    response = requests.get(url, headers=headers)
    results = response.json()

    return results


def _build_search_results(parameters={}):
    results = _get_results(parameters)
    properties = {}
    if results['response']['application_response_code'].startswith('1'):
        for result in results['response']['listings']:

            # sometimes we don't get what we would expect so best to be safe
            for element in property_elements:
                if element not in result:
                    result[element] = ''

            properties[result['guid']] = Property(**result)

    return properties


def search_listings(parameters={}):
    parameters['action'] = 'search_listings'
    return _build_search_results(parameters)
