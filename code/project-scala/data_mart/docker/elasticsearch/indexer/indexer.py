from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ConnectionError
import json
import time
import random

es = Elasticsearch(hosts=[{"host":'elasticsearch'}], retry_on_timeout = True)

for _ in range(100):
    try:
        es.cluster.health(wait_for_status='yellow')
    except ConnectionError:
        time.sleep(2)

with open("data.json", "r") as read_file:
    data = json.load(read_file)

actions = [
    {
    '_index' : 'visits',
    '_type' : 'content',
    '_id' : random.randint(0, 20),
    '_source' : elem,
    }
    for elem in data
]

# create index
print("Indexing Elasticsearch db... (please hold on)")
bulk(es, actions)
print("...done indexing :-)")