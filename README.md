<h1>Data Engineering: Speech-to-text data collection with Kafka, Airflow, and Spark</h1>

<h2>Overview</h2>

<h3>Business Need</h3>

* In building Speech-to-text transcription models, large and diverse training sets are valuable as they enable us to build and improve models which have the capacity to improve and transform peoples lives.
* Our client, 10 Academy, is a not-for-profit community-owned organization that has recognized the value of large data sets for speech-to-text models.

<h3>Objective of the project</h3>

* Design and build a robust, large scale, fault tolerant, highly available Kafka cluster that can be used to post a sentence and receive an audio file. 
* The tool that we create should be deployed to process posting and receiving text and audio files from and into a data lake, apply transformation in a distributed manner, and load it into a warehouse in a suitable format to train a speech-to-text model.  
