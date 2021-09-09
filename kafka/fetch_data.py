import pandas as pd
import numpy as np
import json
import csv

def gen_data():
    data=pd.read_csv('../data/Clean_Amharic.txt',header=None)
    data.columns=['Text']
    data['Text']=data['Text'].apply(lambda x: x.replace("(",""))
    data['Text']=data['Text'].apply(lambda x: x.replace(")",""))
    data['Text']=data['Text'].apply(lambda x :x.replace("[",""))


    data['text_id']=data['Text'].apply(lambda x: x.split(" ")[-1])
    data['Text']=data['Text'].apply(lambda x: x.replace("sentence",""))
    data['Text']=data['Text'].apply(lambda x:x[:-2])

    data[:20].to_csv('amharic.csv',index=False)
    i=1
    corpus={}
    with open('amharic.csv') as csvFile:
        csvReader=csv.DictReader(csvFile)
        for rows in csvReader:
            key=rows['text_id']
            
            corpus[key]=rows['Text']
            i+=1


    print(corpus)
    with open('test.json', 'w', encoding='utf-8') as jsonf:
        

        jsonf.write(json.dumps(corpus, indent=4))
        
        f = open('test.json',)
  
        data_json = json.load(f)
    
    return data_json
    
    

    

# data.to_json("data.json", orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None)

# jsonfile=open("data.json")

# data = json.load(jsonfile)

# print(data)

# jsonfile.close()


