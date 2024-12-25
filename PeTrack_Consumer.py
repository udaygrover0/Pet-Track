from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from bson import ObjectId
from json import loads, dumps
from datetime import datetime

# Initialize Kafka consumer for the tracker topic
consumer = KafkaConsumer(
    'tracker',  # Topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='pettrack-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Custom serializer for ObjectId and datetime
def custom_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)  # Convert ObjectId to string
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO format
    raise TypeError(f"Type {type(obj)} not serializable")

# Initialize Kafka producer for the alerts topic
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x, default=custom_serializer).encode('utf-8')
)

# Connect to MongoDB
mongo_client = MongoClient("mongodb+srv://321057:7BUI93T94CPeg2no@pettrack.6mnfk.mongodb.net/")
db = mongo_client['PeTrack']  # Database name
pet_data_collection = db['PeTrack']  # Collection for pet data
alerts_collection = db['alerts']  # Collection for alerts

# Define base thresholds for alerts
TEMP_BASE_THRESHOLD = 38.0  # Environmental temperature alert above this value
BODY_TEMP_HIGH_THRESHOLD = 38.0  # Body temperature high alert
BODY_TEMP_LOW_THRESHOLD = 36.0  # Body temperature low alert
STRESS_BASE_THRESHOLD = 3  # Stress level alert if above this value
STRESS_ZERO_THRESHOLD = 10  # Stress level alert if 0 for these many readings
HEART_RATE_HIGH_THRESHOLD = 110  # Heart rate high alert
HEART_RATE_LOW_THRESHOLD = 80  # Heart rate low alert
LOCATION_ERROR_THRESHOLD = 10  # Consecutive unchanged latitude and longitude

# Define percentage thresholds for alerts
TEMP_PERCENTAGE_THRESHOLD = 10.0  # Environmental temperature percentage change
BODY_TEMP_PERCENTAGE_THRESHOLD = 5.0  # Body temperature percentage change
STRESS_LEVEL_PERCENTAGE_THRESHOLD = 200.0  # Stress level percentage change
HEART_RATE_PERCENTAGE_THRESHOLD = 30.0  # Heart rate percentage change

# Initialize tracker for stress and location errors
stress_zero_tracker = {}
location_tracker = {}

# Function to calculate percentage change
def calculate_percentage_change(new_value, old_value):
    if old_value == 0:  # Avoid division by zero
        return 100.0
    return ((new_value - old_value) / old_value) * 100

# Consumer processing loop
for message in consumer:
    data = message.value

    # Convert timestamp to BSON-compatible datetime
    try:
        data['Timestamp'] = datetime.strptime(data['Timestamp'], "%d-%m-%Y %H:%M")
    except Exception as e:
        print("Error parsing Timestamp:", e)
        continue

    # Insert real-time pet data into MongoDB
    pet_data_collection.insert_one(data)

    alerts = []

    # Retrieve the last recorded data for the same Pet ID
    previous_data = pet_data_collection.find_one(
        {"Pet ID": data['Pet ID']},
        sort=[("Timestamp", -1)]
    )

    # Environmental temperature alert
    if data['Environmental Temperature'] > TEMP_BASE_THRESHOLD:
        temp_change = calculate_percentage_change(data['Environmental Temperature'], previous_data['Environmental Temperature']) if previous_data else None
        if temp_change is None or abs(temp_change) > TEMP_PERCENTAGE_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Environmental Temperature",
                "Change Percentage": round(temp_change, 2) if temp_change else None,
                "Timestamp": data['Timestamp']
            })

    # Body temperature alert
    if data['Body Temperature'] > BODY_TEMP_HIGH_THRESHOLD or data['Body Temperature'] < BODY_TEMP_LOW_THRESHOLD:
        body_temp_change = calculate_percentage_change(data['Body Temperature'], previous_data['Body Temperature']) if previous_data else None
        if body_temp_change is None or abs(body_temp_change) > BODY_TEMP_PERCENTAGE_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Body Temperature",
                "Change Percentage": round(body_temp_change, 2) if body_temp_change else None,
                "Timestamp": data['Timestamp']
            })

    # Stress level alert
    if data['Stress Level'] > STRESS_BASE_THRESHOLD:
        stress_change = calculate_percentage_change(data['Stress Level'], previous_data['Stress Level']) if previous_data else None
        if stress_change is None or abs(stress_change) > STRESS_LEVEL_PERCENTAGE_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Stress Level",
                "Change Percentage": round(stress_change, 2) if stress_change else None,
                "Timestamp": data['Timestamp']
            })
    elif data['Stress Level'] == 0:
        if data['Pet ID'] not in stress_zero_tracker:
            stress_zero_tracker[data['Pet ID']] = 1
        else:
            stress_zero_tracker[data['Pet ID']] += 1
        if stress_zero_tracker[data['Pet ID']] >= STRESS_ZERO_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Stress Level",
                "Change Percentage": None,
                "Timestamp": data['Timestamp']
            })
            stress_zero_tracker[data['Pet ID']] = 0

    # Heart rate alert
    if data['Heart Rate (BPM)'] > HEART_RATE_HIGH_THRESHOLD or data['Heart Rate (BPM)'] < HEART_RATE_LOW_THRESHOLD:
        heart_rate_change = calculate_percentage_change(data['Heart Rate (BPM)'], previous_data['Heart Rate (BPM)']) if previous_data else None
        if heart_rate_change is None or abs(heart_rate_change) > HEART_RATE_PERCENTAGE_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Heart Rate",
                "Change Percentage": round(heart_rate_change, 2) if heart_rate_change else None,
                "Timestamp": data['Timestamp']
            })

    # Constant location alert
    if data['Pet ID'] not in location_tracker:
        location_tracker[data['Pet ID']] = {
            "Latitude": {"value": data["Latitude"], "count": 1},
            "Longitude": {"value": data["Longitude"], "count": 1}
        }
    else:
        # Check if latitude and longitude remain constant
        if location_tracker[data['Pet ID']]["Latitude"]["value"] == data["Latitude"]:
            location_tracker[data['Pet ID']]["Latitude"]["count"] += 1
        else:
            location_tracker[data['Pet ID']]["Latitude"] = {"value": data["Latitude"], "count": 1}

        if location_tracker[data['Pet ID']]["Longitude"]["value"] == data["Longitude"]:
            location_tracker[data['Pet ID']]["Longitude"]["count"] += 1
        else:
            location_tracker[data['Pet ID']]["Longitude"] = {"value": data["Longitude"], "count": 1}

        # Trigger alert if constant for more than LOCATION_ERROR_THRESHOLD
        if location_tracker[data['Pet ID']]["Latitude"]["count"] >= LOCATION_ERROR_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Tracker Error: Constant Latitude",
                "Change Percentage": None,
                "Timestamp": data['Timestamp']
            })
            location_tracker[data['Pet ID']]["Latitude"]["count"] = 0

        if location_tracker[data['Pet ID']]["Longitude"]["count"] >= LOCATION_ERROR_THRESHOLD:
            alerts.append({
                "Pet ID": data['Pet ID'],
                "Type of Alert": "Tracker Error: Constant Longitude",
                "Change Percentage": None,
                "Timestamp": data['Timestamp']
            })
            location_tracker[data['Pet ID']]["Longitude"]["count"] = 0

    # Insert alerts into MongoDB and publish to Kafka
    if alerts:
        alerts_collection.insert_many(alerts)
        for alert in alerts:
            producer.send('pettrack-alerts', alert)
            print(alert)
