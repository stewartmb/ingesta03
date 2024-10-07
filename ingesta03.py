import boto3
from pymongo import MongoClient
import csv

# Parámetros de conexión MongoDB
mongo_uri = "mongodb://34.237.90.249:27017"
db_name = "reserva_y_espacios"
mongo_tables = ["Reserva", "Espacio"]

# Parámetros para S3
nombre_bucket = "proyecto-uni"
s3_client = boto3.client('s3')

def export_mongodb_to_csv(collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    documentos = list(collection.find())

    fichero_upload = f"{collection_name}_mongodb.csv"
    with open(fichero_upload, mode='w', newline='') as file:
        if documentos:
            writer = csv.DictWriter(file, fieldnames=documentos[0].keys())
            writer.writeheader()
            writer.writerows(documentos)
    
    client.close()
    
    # Subir archivo a S3
    s3_client.upload_file(fichero_upload, nombre_bucket, fichero_upload)
    print(f"Ingesta completada y archivo {fichero_upload} subido a S3.")

def main():
    for table in mongo_tables:
        export_mongodb_to_csv(table)

if __name__ == "__main__":
    main()
