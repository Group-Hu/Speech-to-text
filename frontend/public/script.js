// Some of the code has been Adapted from https://medium.com/@bryanjenningz/how-to-record-and-play-audio-in-javascript-faa1b2b3e49b, https://developers.google.com/web/fundamentals/media/recording-audio

const transcript=document.getElementById("transcript")
let key=null
function reqListener () {
        let jso=JSON.parse(this.responseText)
        transcript.innerHTML=jso.value
        key=jso.key
}

    var oReq = new XMLHttpRequest();
    oReq.addEventListener("load", reqListener);
    oReq.open("GET", "/text",{mode:'cors'});
    oReq.send();
  

import { MediaRecorder,register  } from 'extendable-media-recorder';

// const mediaRecoder = new MediaRecorder(stream);


import { connect } from 'extendable-media-recorder-wav-encoder';


(async () => {
 
await register(await connect());

})();

const downloadLink = document.getElementById('download');
var recording=null;
var length=null;

const player = document.getElementById('player');
const recordAudio = () => {
    return new Promise(resolve => {
      navigator.mediaDevices.getUserMedia({ audio: true })
        .then(stream => {
          
          const mediaRecorder = new MediaRecorder(stream, {mimeType: 'audio/wav'});
          const audioChunks = [];
  
          mediaRecorder.addEventListener("dataavailable", event => {
            if (event.data.size > 0) audioChunks.push(event.data);
          });
  
          const start = () => {
            mediaRecorder.start();
          };
  
          const stop = () => {
            return new Promise(resolve => {
              mediaRecorder.addEventListener("stop", () => {
                const audioBlob = new Blob(audioChunks,{
                  tag: 'audio',
                  type: 'audio/wav',
                  ext: '.wav',
                  gUM: {audio: true}
                });
                // audio=audioBlob;
                const audioUrl = URL.createObjectURL(audioBlob);
                
                downloadLink.href = audioUrl;
                downloadLink.download = 'acetest.wav';
                player.src = audioUrl;

                const audio = new Audio(audioUrl);
                const play = () => {
                  // audio.play();
                };

                resolve({ audioBlob, audioUrl, play });
              });
  
              mediaRecorder.stop();
            });
          };
  
          resolve({ start, stop });
        });
    });
  };

  function sleep(time) {
    return new Promise(resolve => setTimeout(resolve, time));
 }

let recordButton=document.getElementById("record-button");
let submitButton=document.getElementById("submit-button")
var active=false;
var activeRecorder=null;
recordButton.addEventListener("click",(async () => {
    if (active==false && activeRecorder== null){
    const recorder = await recordAudio();
    activeRecorder=recorder
      
    recordButton.style.border="3px solid red"
    recorder.start();
    active=true
    }
    else{
        const audio = await activeRecorder.stop();
        // audio.play();
        recording=audio.audioBlob
        length=audio.size
        console.log(length)
        active=false;
        activeRecorder=null
      recordButton.style.border="1px solid rgb(133, 133, 133)"
    submitButton.disabled=false;
    }
  }))

  var unsubmited=true;
  submitButton.addEventListener("click",()=>{
      if (unsubmited){
        
        var AudioContext = window.AudioContext || window.webkitAudioContext;
        var audioCtx = new AudioContext();

        console.log(audioCtx.sampleRate);
        var xhr=new XMLHttpRequest();
          xhr.onload=function(e) {
            if(this.readyState === 4) {
                console.log("Server returned: ",e.target.responseText);
            }
          };
          var fd=new FormData();
          fd.append("audio",recording, "filename");
          let duration=length/(audioCtx.sampleRate * 2)
          fd.append("length",duration)
          fd.append("key",key);
          fd.append("sampleRate", audioCtx.sampleRate);
          xhr.open("POST","/api/audio",true);
          xhr.send(fd);
          alert("Audio Submited")
      }
      submitButton.disabled=true;
  })

// const downloadLink = document.getElementById('download');
// const stopButton = document.getElementById('record-button');


// const handleSuccess = function(stream) {
//   const options = {mimeType: 'audio/webm'};
//   const recordedChunks = [];
//   const mediaRecorder = new MediaRecorder(stream, options);

//   mediaRecorder.addEventListener('dataavailable', function(e) {
//     if (e.data.size > 0) recordedChunks.push(e.data);
//   });

//   mediaRecorder.addEventListener('stop', function() {
//     downloadLink.href = URL.createObjectURL(new Blob(recordedChunks));
//     downloadLink.download = 'acetest.wav';
//   });

//   stopButton.addEventListener('click', function() {
//     mediaRecorder.stop();
//   });

//   mediaRecorder.start();
// };

// navigator.mediaDevices.getUserMedia({ audio: true, video: false })
//     .then(handleSuccess);

