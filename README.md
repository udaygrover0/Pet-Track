
# PeTrack: Real-Time Pet Health Monitoring System

## Overview

PeTrack is a robust system designed to monitor and analyze the health and activity patterns of pets in real-time. Using Kafka for data streaming and MongoDB for storage, PeTrack processes pet health metrics and generates actionable insights and alerts. This repository includes code for both the producer and consumer components of the system, along with visualizations and reports.

## Features

1. **Real-Time Data Processing**:
   - Streams health metrics such as heart rate, body temperature, respiratory rate, and stress levels.
   - Tracks location coordinates for movement and activity monitoring.

2. **Health Monitoring**:
   - Alerts for anomalies in body temperature, heart rate, stress levels, and environmental conditions.
   - Generates insights based on activity and health data trends.

3. **Custom Thresholds and Alerts**:
   - User-defined thresholds for various health metrics.
   - Alerts for sudden changes or sustained anomalies, ensuring timely interventions.

4. **Data Storage**:
   - Uses MongoDB to store raw data and alerts for historical analysis.
   - Leverages BSON format for timestamps and ObjectId compatibility.

5. **Visualization**:
   - Charts showcasing health trends, activity patterns, and alert distributions.
   - Correlations between environmental factors and pet stress levels.

## Components

### 1. **Producer (`PeTrack_Producer.py`)**  
   - Reads pet health data from a CSV file.
   - Streams data to Kafka topics (`tracker`) and stores it in MongoDB.
   - Simulates real-time data ingestion with a delay mechanism.

### 2. **Consumer (`PeTrack_Consumer.py`)**  
   - Processes real-time data from Kafka topics.
   - Stores processed data and generates alerts based on defined thresholds.
   - Sends alerts to a dedicated Kafka topic (`pettrack-alerts`) and MongoDB collection.

### 3. **Visualizations and Reports (`Pet Health.pdf`)**
   - Monthly average heart rates and body temperatures.
   - Weekly activity patterns and temperature-stress correlations.
   - Recent alerts for body temperature and other anomalies.

## Installation and Usage

### Prerequisites
- **Python**: Ensure Python 3.7+ is installed.
- **Kafka**: Set up a Kafka cluster and configure the necessary topics (`tracker`, `pettrack-alerts`).
- **MongoDB**: Configure a MongoDB instance and replace connection strings in the scripts.

### Steps
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/PeTrack.git
   cd PeTrack
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Set Up Kafka and MongoDB**:
   - Replace `localhost:9092` and MongoDB connection strings in the scripts.
4. **Run the Producer**:
   ```bash
   python PeTrack_Producer.py
   ```
5. **Run the Consumer**:
   ```bash
   python PeTrack_Consumer.py
   ```
6. **Explore Visualizations**:
   Open `Pet Health.pdf` to view the analysis and reports.

## Repository Structure

```
PeTrack/
├── PeTrack_Producer.py     # Producer code for data ingestion
├── PeTrack_Consumer.py     # Consumer code for processing and alerts
├── Pet Health.pdf          # Analysis and visualizations
├── README.md               # Project documentation
└── requirements.txt        # Dependencies
```

## Future Enhancements
- Integration with cloud services for scalable deployment.
- Enhanced visualizations with real-time dashboards.
- Machine learning models for predictive health insights.

## Acknowledgments
This project was developed to demonstrate real-time data processing using Kafka and MongoDB with practical applications in pet health monitoring.
