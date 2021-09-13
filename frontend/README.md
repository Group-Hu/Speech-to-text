# Frontend for data collection

<p>The servers are two part. A node/express server that handles the post of the audio and a python/flask server that handles the serving of the transciptions.
</p>

## Kafka topics required
Two kafka topics are required text and audio_topic

## Node Server Instructions
insall the node modules
```
npm install
```
run the app with 

```
node .\app.js
```
## Flask server Instructions
Install the requirements.txt file
```
pip install requirements.txt
```
And run the server.py script
```
python .\server.py
```
