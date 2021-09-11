// import { KafkaClient as Client } from 'kafka-node';



// import * as dotenv from 'dotenv';
// import { Server, Message, WebSocketClient } from 'ws';
// import * as express from 'express';



// const kafkaHost = 'localhost:9092';
// const client = new Client({ kafkaHost });

// dotenv.config();
function stringFromUTF8Array(data)
  {
    const extraByteMap = [ 1, 1, 1, 1, 2, 2, 3, 0 ];
    var count = data.length;
    var str = "";
    
    for (var index = 0;index < count;)
    {
      var ch = data[index++];
      if (ch & 0x80)
      {
        var extra = extraByteMap[(ch >> 3) & 0x07];
        if (!(ch & 0x40) || !extra || ((index + extra) > count))
          return null;
        
        ch = ch & (0x3F >> extra);
        for (;extra > 0;extra -= 1)
        {
          var chx = data[index++];
          if ((chx & 0xC0) != 0x80)
            return null;
          
          ch = (ch << 6) | (chx & 0x3F);
        }
      }
      
      str += String.fromCharCode(ch);
    }
    
    return str;
  }


// var kafkanode = require('kafka-node');
//     Consumer = kafkanode.Consumer;

const { Kafka } = require('kafkajs');

// let KafkaConsumerManager = require('kafka-consumer-manager');



var express = require("express");
var multer = require('multer');
var app = express();

// app.set('views', __dirname + '/views');
// app.set('view engine', 'jsx');
// app.engine('jsx', require('express-react-views').createEngine());

app.use(express.static('public'))

var storage = multer.diskStorage({
    destination: function (req, file, callback) {
        callback(null, './uploads');
    },
    filename: function (req, file, callback) {
        callback(null, file.fieldname + '-' + Date.now());
    }
});

var upload = multer({ storage: storage }).single('audio');

var fileupload = require("express-fileupload");
app.use(fileupload());

async function run(messages,res) {
    const kafka = new Kafka({ brokers: ["localhost:9092"] });
  
    const producer = kafka.producer();
    await producer.connect();
  
    await producer.send({
      topic: "example_topic",
     messages:messages

    });
    
    res.end("File is uploaded");
  }



app.get('/', function (req, res) {
    res.sendFile(__dirname + "/public/index.html");
});
app.get('/test', function (req, res) {
    res.sendFile(__dirname + "/public/test.html");
});


app.get('/text', function (req, res) {
            
    (async () => {
        
        const kafka = new Kafka({
            clientId: 'api-5',
            brokers: ['localhost:9092']
        })


    const consumer = kafka.consumer({ groupId: 'api' })
    await consumer.connect()

    await consumer.subscribe({ topic: 'text' })
    
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            consumer.resume([{ topic }])
            res.send( {key: message.key.toString(),
                value:message.value.toString()
            });
            
            consumer.pause([{ topic }])
            return
        },
    })
})()
    
//   let configuration = {
//     KafkaUrl: "localhost:9092",
//     GroupId: "api",
//     KafkaConnectionTimeout: 10000,
//     KafkaRequestTimeout: 30000,
//     KafkaOffsetDiffThreshold: 3,
    
//     AutoCommit:true,
//     Topics: ["text"],
//     ResumePauseIntervalMs: 30000,
//     ResumePauseCheckFunction: (consumer) => {
//         return shouldPauseConsuming(consumer)
//     },
//     MessageFunction: (msg) => { 
//         // return handleMessage(msg)
//         console.log(msg)
//          res.send("msg")
//      },
//     MaxMessagesInMemory: 1,
//     ResumeMaxMessagesRatio: 0.99,
//     CreateProducer: false,
//     StartOffset: "earliest",
// };

    // (async () => {
    //     let kafkaConsumerManager = new KafkaConsumerManager()
    //     await kafkaConsumerManager.init(configuration)
    //         .then(() => {})
    //         })()


    // client = new kafkanode.KafkaClient()
    // consumer = new Consumer(
    //     client,
    //     [
    //         { topic: 'text' },
    //     ],{
    //         groupId: 'api',//consumer group id, default `kafka-node-group`
    //         // Auto commit config
    //         autoCommit: true,
    //         autoCommitIntervalMs: 5000,
    //         // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
    //         fetchMaxWaitMs: 100,
    //         // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
    //         fetchMinBytes: 1,
    //         // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    //         fetchMaxBytes: 1024 * 1024,
    //         // If set true, consumer will fetch message from the given offset in the payloads
    //         fromOffset: false,
    //         highWaterOffset:1,
    //         maxtickmessages :1,
    //         // If set to 'buffer', values will be returned as raw buffer objects.
    //         encoding: 'utf8',
    //         keyEncoding: 'utf8'
    //     }
    // );

    // consumer.on('message', function (message) {
    //     // console.log(
    //         // new TextDecoder().decode(message.value))
    //     // new TextDecoder().decode(message.value); 
    //     // console.log(String(message.value));

    //     res.send(message.value)
        
    // });


    // client = new kafkanode.KafkaClient(),
    // offset = new kafkanode.Offset(client);
    // offset.fetch([
    //     { topic: 'text', time: Date.now(), maxNum: 1 }
    // ], function (err, data) {
    //     console.log(data)
    // });

});
app.post('/api/audio', function (req, res) {

    
    // const formData = req.body.formData
    // console.log(req.files.audio.data)
    // console.log(req.body)

    upload(req, res, function (err) {
        console.log("Upload Started")
        if (err) {

            console.log(err)
            return res.end("Error uploading file.");



        }
        message=[ { key: 'key1', value: req.files.audio.data }]
        run(message,res)
    });
});

app.listen(3000, function () {
    console.log("Working on port 3000");
});