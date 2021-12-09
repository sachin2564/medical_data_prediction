#
# Worker server
#
import pickle
import platform
import io
import os
import sys
import pika
import redis
import hashlib
import json
import requests

hostname = platform.node()

##
## Configure test vs. production
##
redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"


print(f"Connecting to rabbitmq({rabbitMQHost}) and redis({redisHost})")

##
## Set up redis connections
##
db = redis.Redis(host=redisHost)

##
## Set up rabbitmq connection
##

rabbitMQ = pika.BlockingConnection(
          pika.ConnectionParameters(host=rabbitMQHost))
rabbitMQChannel = rabbitMQ.channel()
def main(): 
  rabbitMQChannel.queue_declare(queue='toWorker')
  rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
  rabbitMQChannel.basic_consume(queue='toWorker', auto_ack=False, on_message_callback=callback)
  rabbitMQChannel.start_consuming()



def callback(ch, method, properties, body):
    print( " [x] Received %r" % (body,) )
    body = body.decode("utf-8")
    json_data=json.loads(body)
    entries = json_data["instances"]
    new_entries = []
    # Remove the entries that are already present in DB 
    for entry in entries:
        caseID = entry["case_id"]
        patientID = entry["patientid"]
        key = str(patientID)+":"+str(caseID)
        if db.exists(key):
            continue
        new_entries.append(entry)
    json_data["instances"] = new_entries
    # Process the new entries 
    if(len(new_entries) > 0):
        URL = "https://us-central1-final-project-333720.cloudfunctions.net/prediction_service"
        response = requests.post(url = URL, data=json.dumps(json_data),headers={'Content-type': 'application/json'})
        response = response.json()
        print(response)
        response = response["predictions"]
        for entry in range(len(response)):
            predicted_entry = response[entry]
            scores = predicted_entry["scores"]
            max_score = max(scores)
            length_of_stay = scores.index(max_score)
            length_of_stay = predicted_entry["classes"][length_of_stay]
            caseID = new_entries[entry]["case_id"]
            entries[entry]["length_of_stay"] = length_of_stay
        for entry in new_entries:
            caseID = entry["case_id"]
            patientID = entry["patientid"]
            key = str(patientID)+":"+str(caseID)
            val = pickle.dumps(entry)
            db.set(key,val)
    rabbitMQChannel.basic_ack(delivery_tag = method.delivery_tag)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
