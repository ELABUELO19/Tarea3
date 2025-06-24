import os
import time
import random
from pymongo import MongoClient

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://storage:27017')
DB_NAME = 'waze_data'
COLLECTION = 'alerts'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

def generate_dummy_alert():
    alert = {
        'type': 'dummy',
        'message': 'Dummy alert generated',
        'timestamp': time.time(),
        'location': {
            'lat': -33.5 + random.random() * 0.1,
            'lon': -70.6 + random.random() * 0.1
        }
    }
    return alert

if __name__ == '__main__':
    while True:
        alert = generate_dummy_alert()
        collection.insert_one(alert)
        print('Generated dummy alert:', alert)
        time.sleep(60)
