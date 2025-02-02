from controller import get_admin_client, create_topics, get_producer, produce_sensor_data, get_consumer, process_sensor_data

if __name__ == "__main__":
    my_name = "tet"
    
    admin_client = get_admin_client()
    create_topics(admin_client, my_name)
    admin_client.close() 

    producer = get_producer()
    produce_sensor_data(producer, f"{my_name}_building_sensors")
    producer.close()

    consumer = get_consumer("my_consumer_group_3", [f"{my_name}_building_sensors"])
    producer = get_producer()

    try:
        process_sensor_data(consumer, producer, f"{my_name}_temperature_alerts", f"{my_name}_humidity_alerts")
    finally:
        producer.close()
