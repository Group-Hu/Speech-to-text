from flask import Flask
from flask_restful import Resource, Api, reqparse

from flask_cors import CORS
import pandas as pd
import ast

from kafka import KafkaConsumer
app = Flask(__name__)
api = Api(app)
CORS(app)

class TextService(Resource):
    def get(self):
        consumer = KafkaConsumer('text',
                         group_id='api',
                         bootstrap_servers=['localhost:9092'])
        messages = consumer.poll(timeout_ms=1000,max_records=1)
        for tp, mess in messages.items():
            message=mess[0]
            print ("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition,
                                                message.offset, message.key,
                                                message.value.decode('utf-8')))
            consumer.close()
            return {'key': str(message.key),'value':message.value.decode('utf-8')}, 200 

api.add_resource(TextService, '/text')


if __name__ == '__main__':
    app.run() 