from concurrent import futures
import requests

nums = range(1, 10)
url_tpl = 'http://jsonplaceholder.typicode.com/todos/{}'


def get_data(myid):
    url = url_tpl.format(myid)
    return requests.get(url).text


with futures.ThreadPoolExecutor(12) as executor:
    results = executor.map(get_data, nums)
print(list(results))
