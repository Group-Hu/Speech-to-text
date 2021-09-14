from flask import Flask,make_response,render_template
from flask_restful import Resource, Api, reqparse

from flask_cors import CORS
import ast

from kafka import KafkaConsumer
from kafka import KafkaProducer
import werkzeug
import json
import base64

app = Flask(__name__,static_folder="public", static_url_path='')
api = Api(app)
CORS(app)

servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9094"]

class TextService(Resource):
    def get(self):
        consumer = KafkaConsumer('groupHu_speech',
                         group_id='api',security_protocol="SSL",
                         bootstrap_servers=servers)
                         
        messages = consumer.poll(timeout_ms=10000,max_records=1)

        for tp, mess in messages.items():
            message=mess[0]
            print ("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition,
                                                message.offset, message.key,
                                                message.value.decode('utf-8')))
            consumer.close()
            return {'key': message.key.decode('utf-8'),'value':message.value.decode('utf-8')}, 200 

#Code adapted from https://stackoverflow.com/questions/28982974/flask-restful-upload-image

class HomeService(Resource):
    def get(self):
        headers = {'Content-Type': 'text/html'}
        return app.send_static_file('index.html')

class AudioService(Resource):
    def post(self):
        parse = reqparse.RequestParser()
        # f = request.files['file']
        # print(request)
        parse.add_argument('audio',type=werkzeug.datastructures.FileStorage, location='files')
        
        parse.add_argument('key',required=True)
        parse.add_argument('sampleRate')
        
        parse.add_argument('length')
        args= parse.parse_args()
        audio=args["audio"].read()
        sampleRate=args["sampleRate"]
        
        length=args["sampleRate"]
        key=args["key"]
        producer = KafkaProducer(bootstrap_servers=servers,security_protocol="SSL")
        # producer.send("groupHu_audio",key=str.encode(key),value=audio)
        object={
            "data":str(base64.b64encode(audio)),
            "sample_rate":sampleRate,
            "sample width":length
        }
        producer.send("groupHu_audio",key=str.encode(key),value=json.dumps(object).encode('utf-8'))
        return 200
        
api.add_resource(HomeService,'/')
api.add_resource(TextService, '/text')
api.add_resource(AudioService, '/api/audio')


if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0') 