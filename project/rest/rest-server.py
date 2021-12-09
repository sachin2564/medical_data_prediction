#!/usr/bin/python3
from flask import Flask, request, Response, jsonify
import platform
import jsonpickle
import io, os, sys
import pika, redis
import hashlib, requests
import json
from io import StringIO
import pickle


##
## Configure test vs. production
##

redisHost = os.getenv("REDIS_HOST") or "localhost"
rabbitMQHost = os.getenv("RABBITMQ_HOST") or "localhost"

print("Connecting to rabbitmq({}) and redis({})".format(rabbitMQHost,redisHost))
r = redis.Redis(host=redisHost)

app = Flask(__name__)

@app.route('/medical/insert', methods=['POST'])
def insert():
     json_data = request.get_json()
     json_data = jsonpickle.encode(json_data)
     rabbitMQ = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitMQHost))
     print("Connect open")
     rabbitMQChannel = rabbitMQ.channel()
     rabbitMQChannel.queue_declare(queue='toWorker')
     rabbitMQChannel.exchange_declare(exchange='logs', exchange_type='topic')
     rabbitMQChannel.basic_publish(exchange='',routing_key='toWorker', body=str(json_data))
     rabbitMQ.close()
     return "Queued"

@app.route('/medical/forecast/lengthofstay')
def predicted_val():
    json_data = request.get_json()
    result_dict = {}
    patients = json_data["patientId"]
    for patientID in patients:
        key = str(patientID)+"*"
        cases = r.keys(key)
        print(str(cases))
        if len(cases) > 0:
            record = []
            for case in cases:
                case_record = {}
                entry = pickle.loads(r.get(case))
                case_record["caseID"] = str(entry["case_id"])
                if "length_of_stay" in entry:
                    case_record["length_of_stay"] = entry["length_of_stay"]
                else:
                    case_record["length_of_stay"] = "not predicted yet"
                record.append(case_record)
            result_dict[patientID]=record
        else:
            result_dict[patientID]="Not present in Record"
    response_pickled = jsonpickle.encode(result_dict)
    return Response(response=response_pickled, status=200, mimetype="application/json")

@app.route('/medical/forecast/dump')
def get_all_predicted_val():
    keys = r.keys('*')
    result_dict = {}
    for key in keys:
        patientID,caseID=str(key).split(":")
        if patientID in result_dict:
            record = result_dict[patientID]
        else:
            record = []
        entry = pickle.loads(r.get(key))   
        case_record = {}
        case_record["caseID"]=entry["case_id"]
        if "length_of_stay" in entry:
            case_record["length_of_stay"] = entry["length_of_stay"]
        else:
            case_record["length_of_stay"] = "not predicted yet"
        record.append(case_record)
        result_dict[patientID]=record
    response_pickled = jsonpickle.encode(result_dict)
    return Response(response=response_pickled, status=200, mimetype="application/json")
        

@app.route('/medical/fetch', methods=['GET'])
def retrive():
    json_data = request.get_json()
    val = {}
    data = json_data['patientId']
    for patientID in data:
       key = str(patientID)+"*"
       cases = r.keys(key)
       if len(cases) > 0:
           record = []
           for case in cases:
              record.append(pickle.loads(r.get(case)))
           val[patientID]=record
       else:
           val[patientID]="Not present in Record"
    response_pickled = jsonpickle.encode(val)
    return Response(response=response_pickled, status=200, mimetype="application/json")

 
@app.route('/medical/dump', methods=['GET'])
def dump():
    data = {}
    lst = []
    keys = r.keys('*')
    for key in keys:
        lst.append(pickle.loads(r.get(key)))
    data["patientID"]=lst
    response_pickled = jsonpickle.encode(data)
    return Response(response=response_pickled, status=200, mimetype="application/json")
    
if __name__ == '__main__':
   app.run(host="0.0.0.0", port=5000, debug=True)

