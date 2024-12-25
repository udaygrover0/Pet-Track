import pandas as pd
import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient

def read_pet_data_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print("Message sent to Kafka:", message)

def insert_into_mongo(collection, message):
    # Convert 'Timestamp' to BSON UTC datetime format
    if 'Timestamp' in message:
        try:
            message['Timestamp'] = pd.to_datetime(message['Timestamp']).to_pydatetime()
        except Exception as e:
            print("Error converting Timestamp:", e)
            return

    collection.insert_one(message)
    print("Message inserted into MongoDB:", message)

def main():
    # Initialize Kafka Producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Read data from CSV
    pet_data = read_pet_data_from_csv('pet_wellness_next_2_days_10k.csv')
    
    # Initialize MongoDB client
    client = MongoClient("mongodb+srv://321057:7BUI93T94CPeg2no@pettrack.6mnfk.mongodb.net/" )  # Replace with your MongoDB connection string
    db = client['PeTrack']  # Database name
    collection = db['PeTrack']  # Collection name

    print("Producer started...")

    # Iterate through the dataset and send messages
    for index, row in pet_data.iterrows():
        message = {
            'Pet ID': row['Pet ID'],
            'Timestamp': row['Timestamp'],
            'Latitude': row['Latitude'],
            'Longitude': row['Longitude'],
            'Environmental Temperature': row['Environmental Temperature'],
            'Body Temperature': row['Body Temperature'],
            'Step Count': row['Step Count'],
            'Heart Rate (BPM)': row['Heart Rate (BPM)'],
            'Respiratory Rate (breaths/min)': row['Respiratory Rate (breaths/min)'],
            'Stress Level': row['Stress Level'],
        }

        print("Processing message:", message)

        # Send to Kafka
        send_to_kafka(producer, 'tracker', message)

        # Insert into MongoDB
        insert_into_mongo(collection, message)

        # Sleep for 1 second to simulate real-time streaming
        time.sleep(1)

if __name__ == "__main__":
    main()
