FROM python:alpine3.7 
COPY ./frontend .

# WORKDIR /frontend
# COPY ./ . 

RUN ls
RUN pip install -r requirements.txt 
EXPOSE 5000 
ENTRYPOINT [ "python" ] 
CMD [ "server.py" ]