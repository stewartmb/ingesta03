import boto3
from pymongo import MongoClient
import csv
import os

# Parámetros de conexión MongoDB
mongo_url = "mongodb://34.237.90.249:27017"
mongo_db_name = "reserva_y_espacios"
mongo_tables = ["Reserva", "Espacio"]

# Parámetros para S3
nombre_bucket = "proyecto-uni"
s3_client = boto3.client('s3')

def export_mongo_to_csv(collection_name):
    client = MongoClient(mongo_url)
    db = client[mongo_db_name]
    collection = db[collection_name]
    documentos = list(collection.find())

    fichero_upload = f"{collection_name}.csv"
    with open(fichero_upload, mode='w', newline='') as file:
        if documentos:
            writer = csv.DictWriter(file, fieldnames=documentos[0].keys())
            writer.writeheader()
            writer.writerows(documentos)

    client.close()

    # Definir la ruta de la carpeta en S3 donde se guardará el archivo
    s3_key = f"{collection_name}/{fichero_upload}"

    # Subir el archivo CSV a la carpeta correspondiente en el bucket S3
    s3_client.upload_file(fichero_upload, nombre_bucket, s3_key)
    print(f"Ingesta completada y archivo {fichero_upload} subido a S3 en la carpeta {collection_name}/.")

    # Eliminar el archivo local después de subirlo a S3 para ahorrar espacio
    os.remove(fichero_upload)

def main():
    for collection in mongo_tables:
        export_mongo_to_csv(collection)

if __name__ == "__main__":
    main()
