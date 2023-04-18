from pymongo.mongo_client import MongoClient
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

from bson import json_util
import json


from fastapi import FastAPI

# Craate the api
app = FastAPI()



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connect to MongoDB
uri = "mongodb+srv://egsoto:eg.soto@mamatengomiedo.zqfaa46.mongodb.net/?retryWrites=true&w=majority"
#df = pd.read_csv('Vessel_data_utf_8_bien.csv',sep=';')


client =  MongoClient(uri)
db = client['Entrega_2']

# ¿Cuál es el estado con mayor tráfico de botes en un periodo dado?
@app.get("/getRequest/P1")
def read_root():
    collection = db['P1']
    print(collection.find())
    one_rec=list(collection.find())
    doc_dict = json_util.dumps(one_rec[0])
    print(doc_dict)
    return {doc_dict}

# ¿Cuál es el tipo de carga más común por estado, en un periodo dado?
@app.get("/getRequest/P2")
def answer_question_two():
    collection = db['P2']
    print(collection.find())
    one_rec=list(collection.find())
    doc_dict = json_util.dumps(one_rec)
    print(doc_dict)
    return {doc_dict}

# ¿Qué tanto afectó la pandemia al tráfico de embarcaciones teniendo en cuenta el número de embarcaciones por mes en cada estado en años anteriores?
@app.get("/getRequest/P3")
def answer_question_two():
    collection = db['P3']
    print(collection.find())
    one_rec=list(collection.find())
    result = {}

    # Iterar sobre el arreglo de diccionarios
    for d in one_rec:
        year = d['year']
        month = d['month']
        
        # Si el año no existe en el diccionario de resultados, inicializarlo con una lista vacía
        if year not in result:
            result[year] = []
            
        # Agregar el número de mes y el valor del tráfico a la lista correspondiente al año
        result[year].append([month, d['trafico']])
        
    # Imprimir el diccionario de resultados
    for key in result:
        i = result[key]
        r=[]
        r.append(["Meses", "Num Embarcaciones"])
        sum1 = 0
        sum2 = 0
        sum3 = 0
        sum4 = 0
        sum5 = 0
        sum6 = 0
        sum7 = 0
        sum8 = 0
        sum9 = 0
        sum10 = 0
        sum11 = 0
        sum12 = 0
        for mes in i:
            if mes[0]==1:
                sum1+=mes[1]
            elif mes[0]==2:
                sum2+=mes[1]
            elif mes[0]==3:
                sum3+=mes[1]
            elif mes[0]==4:
                sum4+=mes[1]
            elif mes[0]==4:
                sum4+=mes[1]
            elif mes[0]==5:
                sum5+=mes[1]
            elif mes[0]==6:
                sum6+=mes[1]
            elif mes[0]==7:
                sum7+=mes[1]
            elif mes[0]==8:
                sum8+=mes[1]
            elif mes[0]==9:
                sum9+=mes[1]
            elif mes[0]==10:
                sum10+=mes[1]
            elif mes[0]==11:
                sum11+=mes[1]
            elif mes[0]==12:
                sum12+=mes[1]
        subr = {}
        for j in range(1,13):
            if j==1:
                subr[j]=(sum1)
            elif j==2:
                subr[j]=(sum2)
                
            elif j==3:
                subr[j]=(sum3)

            elif j==4:
                subr[j]=(sum4)
            elif j==5:
                subr[j]=(sum5)
            elif j==6:
                subr[j]=(sum6)
            elif j==7:
                subr[j]=(sum7)
            elif j==8:
                subr[j]=(sum8)
            elif j==9:
                subr[j]=(sum9)
            elif j==10:
                subr[j]=(sum10)
            elif j==11:
                subr[j]=(sum11)
            elif j==12:
                subr[j]=(sum12)
        for l in range(1,13):
            arreglo= []
            arreglo.append(l)
            arreglo.append(subr[l]) 
            r.append(arreglo)
        result[key] = r
    print(result)
    data = json_util.dumps(result)

    return {data}

# ¿Cómo es la distribución geográfica, en un periodo dado, de las embarcaciones?
@app.get("/getRequest/P5")
def answer_question_five():
    collection = db['P5_1']
    print(collection.find())
    one_rec=list(collection.find())
    doc_dict = json_util.dumps(one_rec)
    print(doc_dict)
    return {doc_dict}

# ¿Existe alguna relación entre el día de la semana y el tipo de carga en cada estado?¿La relación cambia por año?


@app.get("/getRequest/P6")
def answer_question_six():
    collection = db['P6']
    print(collection.find())
    one_rec=list(collection.find())
    df = pd.DataFrame(one_rec)
    print(df)
   
    doc_dict = json_util.dumps(one_rec)
    return {doc_dict}


@app.get("/getRequest/P7")
def answer_question_sseven():
    collection = db['P7']
    print(collection.find())
    one_rec=list(collection.find())
    doc_dict = json_util.dumps(one_rec)
    print(doc_dict)
    return {doc_dict}