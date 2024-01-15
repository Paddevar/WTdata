import requests
import json

base_url= "https://openlibrary.org/search.json"

query = {'q':  'programming',
         # 'title': 'test',
         'sort': 'new'
}

query_string = '&'.join([f'{key}={value}' for key, value in query.items()])
full_query_url = base_url + '?' + query_string
r_json = requests.get(full_query_url)

r = json.loads(r_json.text)
print(r['docs'][0])