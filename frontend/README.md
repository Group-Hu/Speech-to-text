# Frontend for data collection

<p>There are two server options. A node/express server and a python/flask server our recommendation is the python server. It can be run from here manually or run using the docker contained in the parent directory. 
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
Change the ip address in ./scripts.py for the broker servers.
And run the server.py script
```
python .\server.py
```
