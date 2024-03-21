# Realtime Election Voting System

This collection houses the code for an election voting system that operates in real-time. Constructed with Python, Kafka, Spark Streaming, Postgres, and Streamlit, the system utilizes Docker Compose for effortless setup of necessary services within Docker containers.

## System Architecture

![alt text](images/system_architecture.jpg)

## System Flow

![alt text](images/system_flow.jpg)

## System Components
- **main.py** :  This primary Python script is responsible for generating essential tables in PostgreSQL, namely `candidates`, `voters`, and `votes`. It also creates the Kafka topic and creates a copy of the `votes` table in the Kafka topic. It also contains the logic to consume the votes from the Kafka topic and produce data to `voters_topic` on Kafka.
- **voting.py** : This Python script hosts the logic for consuming votes from the Kafka topic (`voters_topic`), generating voting data, and transmitting this data to the `votes_topic` on Kafka.
- **spark-streaming.py** : This is the Python script that contains the logic to consume the votes from the Kafka topic (`votes_topic`), enrich the data from postgres and aggregate the votes and produce data to specific topics on Kafka.
- **streamlit-app.py** : This is the Python script that contains the logic to consume the aggregated voting data from the Kafka topic as well as postgres and display the voting data in realtime using Streamlit.
