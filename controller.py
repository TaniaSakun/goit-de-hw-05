from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from configs import kafka_config
import json
import uuid
import time
from datetime import datetime
import random
import signal
import sys

def get_admin_client():
    try:
        return KafkaAdminClient(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password']
        )
    except Exception as e:
        print(f"Error creating Kafka Admin Client: {e}")
        sys.exit(1)

def create_topics(admin_client, my_name):
    topics = [f'{my_name}_building_sensors', f'{my_name}_temperature_alerts', f'{my_name}_humidity_alerts']
    new_topics = [NewTopic(name=topic, num_partitions=3, replication_factor=1) for topic in topics]  # Increased partitions
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Topics created successfully: {', '.join(topics)}")
    except Exception as e:
        print(f'Error creating topics: {e}')

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: json.dumps(k).encode('utf-8')
        )
    except Exception as e:
        print(f"Error creating Kafka Producer: {e}")
        sys.exit(1)

def produce_sensor_data(producer, topic_name, count=20):
    for i in range(count):
        try:
            sensor_id = random.randint(1, 20)
            data = {
                "sensor_id": sensor_id,
                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "temperature": round(25 + random.random() * (45 - 25), 2),
                "humidity": round(15 + random.random() * (85 - 15), 2)
            }
            producer.send(topic_name, key=str(sensor_id), value=data)  # Using sensor_id as key
            producer.flush()
            print(f"Message {i} sent successfully to topic {topic_name}.")
            time.sleep(2)
        except Exception as e:
            print(f"Error sending message: {e}")

def get_consumer(group_id, topics):
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id
        )
        consumer.subscribe(topics)
        return consumer
    except Exception as e:
        print(f"Error creating Kafka Consumer: {e}")
        sys.exit(1)

def process_sensor_data(consumer, producer, temp_alert_topic, humidity_alert_topic):
    def signal_handler(sig, frame):
        print("Gracefully shutting down consumer...")
        consumer.close()
        producer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
            data = message.value
            
            # Temperature Alerts
            if data["temperature"] > 40 or data["temperature"] < 0:
                print(f"🚨 Critical temperature level!!! Temperature: {data['temperature']}")
                send_alert(producer, temp_alert_topic, data, "🚨 Critical temperature level!!!")
            elif data["temperature"] > 0 and data["temperature"] < 40:
                print(f"✅ Temperature {data['temperature']} is within normal range, no alert sent.")
            
            # Humidity Alerts
            if data["humidity"] > 80 or data["humidity"] < 20:
                print(f"🚨 Critical humidity level!!! Humidity: {data['humidity']}")
                send_alert(producer, humidity_alert_topic, data, "🚨 Critical humidity level!!!")
            elif data["humidity"] > 20 and data["humidity"] < 80:
                print(f"✅ Humidity {data['humidity']} is within normal range, no alert sent.")
                
            print("") 
    except Exception as e:
        print(f"Error processing message: {e}")

def send_alert(producer, topic, data, alert_msg):
    alert_data = {
        "sensor_id": data["sensor_id"],
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "message": alert_msg
    }
    try:
        producer.send(topic, key=str(uuid.uuid4()), value=alert_data)
        producer.flush()
        print(f"Alert sent to {topic} successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"Error sending alert: {e}")