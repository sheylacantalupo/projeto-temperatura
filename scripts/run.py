
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
from datetime import datetime


if __name__ == "__main__":
    # estabelece a conexão com a instância do MongoDB usando a URI fornecida. Ela retorna o cliente 
    # o MongoDB que permite interagir com o banco de dados.
    def connect_mongo(uri):

        client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
            return client
        except Exception as e:
            print(e)
            return e                                                

    # utiliza o(a) cliente do MongoDB para criar (se não existir) e conectar-se ao banco de dados especificado
    # pelo parâmetro db_name. Ela retorna o objeto de banco de dados que pode ser usado para interagir com as 
    # coleções dentro dele.    
    def create_connect_db(client):
        db = client["db_temperaturas"]
        return db


    # cria (se não existir) e conecta-se à coleção especificada pelo parâmetro col_name dentro do banco de dados 
    # fornecido. Ela retorna o objeto de coleção que pode ser usado para interagir com os documentos dentro dela.
    def create_connect_collection(db, col_name):

        collections = db[col_name]
        return collections

    #  extrai dados de uma API na URL fornecida e retorna os dados extraídos no formato JSON.
    def extract_api_data(arguments):
        if 'lat' not in arguments or 'lon' not in arguments or 'api_key' not in arguments:
            raise ValueError("Os argumentos 'lat', 'lon' e 'api_key' são obrigatórios.")

        lat = arguments['lat']
        lon = arguments['lon']
        api_key = arguments['api_key']

        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}")
        return response.json()


    # recebe uma coleção e os dados que serão inseridos nela. Ela deve adicionar todos os documentos 
    # à coleção e retornar a quantidade de documentos inseridos.
    def insert_data(col, data):
        col.insert_one(data)
        return "dados inseridos"

    # recebe os dados da chamada API do tempo atual, faz o tratamento dos dados e retorna um documento
    def editDoc(data):
        timestamp = data['dt']
        data_hora = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')
        temperatura_atual = data['main']['temp'] - 273.15
        sensacao_termica = data['main']['feels_like'] - 273.15
        temp_maxima = data['main']['temp_max'] - 273.15
        temp_minima = data['main']['temp_min'] - 273.15
        doc = {
            'data': data_hora.split()[0],
            'hora': data_hora.split()[1],
            'temperatura (°C)': temperatura_atual,
            'sensacao_termica (°C)': sensacao_termica,
            'temp_maxima (°C)': temp_maxima,
            'temp_minima (°C)': temp_minima
        }
        return doc
    

    client = connect_mongo("mongodb+srv://sheylacantalupo:@cluster-pipeline.ahm40a7.mongodb.net/?retryWrites=true&w=majority")
    db = create_connect_db(client)
    col_temp = create_connect_collection(db,"temp")
    col_All = create_connect_collection(db,"tempAll")

  
    params = {"lat": -7.1153, "lon": -34.861, "api_key": ""}
    data = extract_api_data(params)
    insert_data(col_All, data)

    doc = editDoc(data)
    insert_data(col_temp, doc)
    print(col_temp.count_documents({}))

    

    client.close()