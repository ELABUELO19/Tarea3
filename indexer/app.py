import os
import time
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers

# Configuración de MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://storage:27017/waze_data')
client = MongoClient(MONGO_URI)
db = client.get_default_database()
collection = db.alerts

# Configuración de Elasticsearch
ES_HOST = os.getenv('ES_HOST', 'elasticsearch')
ES_PORT = int(os.getenv('ES_PORT', 9200))
es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}])
INDEX_NAME = 'waze_alerts'

# Función para indexar documentos en Elasticsearch
def index_alerts():
    alerts = list(collection.find())
    if not alerts:
        print("No hay nuevas alertas para indexar.")
        return
    actions = []
    for alert in alerts:
        # Convertir ObjectId a string y eliminarlo del documento
        doc_id = str(alert.get('_id'))
        alert.pop('_id', None)
        # Asegurar timestamp como float
        alert['timestamp'] = float(alert.get('timestamp', 0))
        actions.append({
            "_index": INDEX_NAME,
            "_id": doc_id,
            "_source": alert
        })
    # Log de confirmación: mostrar IDs de alertas a indexar
    ids_to_index = [action['_id'] for action in actions]
    print(f"Indexando {len(actions)} alertas con IDs: {ids_to_index}")
    helpers.bulk(es, actions)
    print(f"Indexadas {len(actions)} alertas en Elasticsearch.")

if __name__ == '__main__':
    while True:
        index_alerts()
        time.sleep(60)  # Intervalo de 1 minuto
