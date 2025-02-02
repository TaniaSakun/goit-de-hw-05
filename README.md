# goit-de-hw-05
The repository for the 5th GoItNeo Data Engineering homework

# Task Description:

1. Creating Topics in Kafka:

Create three topics in Kafka:

_building_sensors_ — to store data from all sensors.
_temperature_alerts_ — to store notifications about exceeding the allowable temperature threshold.
_humidity_alerts_ — to store notifications about exceeding or falling below the allowable humidity range.

2. Sending Data to Topics:

Write a Python script that simulates a sensor and periodically sends randomly generated data (temperature and humidity) to the building_sensors topic.

The data should include the sensor ID, timestamp, and corresponding readings.
One run of the script should correspond to a single sensor. In order to simulate multiple sensors, the script needs to be executed multiple times.
The sensor ID can be a random number but should remain constant for each script run. When the script is run again, the sensor ID can change.

Temperature should be a random value between 25 and 45.
Humidity should be a random value between 15 and 85.

3. Data Processing:

Write a Python script that subscribes to the building_sensors topic, reads messages, and checks the received data:

If the temperature exceeds 40°C, generate a notification and send it to the temperature_alerts topic.
If the humidity exceeds 80% or falls below 20%, generate a notification and send it to the humidity_alerts topic.
The notifications should contain the sensor ID, the readings, timestamp, and a message about exceeding the threshold.

4. Final Data:

Write a Python script that subscribes to the temperature_alerts and humidity_alerts topics, reads the notifications, and displays the messages on the screen.

# Task Results:

**Task 1.** Topics created successfully: tet_building_sensors, tet_temperature_alerts, tet_humidity_alerts
**Task 2.** Message 0-19 sent successfully to topic tet_building_sensors.
**Task 3-5.**
Received message: {'sensor_id': 10, 'time': '2025-02-02 08:39:10', 'temperature': 36.07, 'humidity': 81.24}
✅ Temperature 36.07 is within normal range, no alert sent.
🚨 Critical humidity level!!! Humidity: 81.24
Alert sent to tania_humidity_alerts successfully.

Received message: {'sensor_id': 9, 'time': '2025-02-02 08:39:14', 'temperature': 34.64, 'humidity': 28.58}
✅ Temperature 34.64 is within normal range, no alert sent.
✅ Humidity 28.58 is within normal range, no alert sent.

Received message: {'sensor_id': 6, 'time': '2025-02-02 08:39:18', 'temperature': 38.3, 'humidity': 67.35}
✅ Temperature 38.3 is within normal range, no alert sent.
✅ Humidity 67.35 is within normal range, no alert sent.

Received message: {'sensor_id': 17, 'time': '2025-02-02 08:39:23', 'temperature': 38.86, 'humidity': 73.48}
✅ Temperature 38.86 is within normal range, no alert sent.
✅ Humidity 73.48 is within normal range, no alert sent.

Received message: {'sensor_id': 4, 'time': '2025-02-02 08:39:25', 'temperature': 33.64, 'humidity': 67.88}
✅ Temperature 33.64 is within normal range, no alert sent.
✅ Humidity 67.88 is within normal range, no alert sent.

Received message: {'sensor_id': 10, 'time': '2025-02-02 08:39:27', 'temperature': 30.25, 'humidity': 25.48}
✅ Temperature 30.25 is within normal range, no alert sent.
✅ Humidity 25.48 is within normal range, no alert sent.

Received message: {'sensor_id': 19, 'time': '2025-02-02 08:39:31', 'temperature': 34.86, 'humidity': 31.91}
✅ Temperature 34.86 is within normal range, no alert sent.
✅ Humidity 31.91 is within normal range, no alert sent.

Received message: {'sensor_id': 1, 'time': '2025-02-02 08:39:33', 'temperature': 36.65, 'humidity': 46.27}
✅ Temperature 36.65 is within normal range, no alert sent.
✅ Humidity 46.27 is within normal range, no alert sent.

Received message: {'sensor_id': 4, 'time': '2025-02-02 08:39:35', 'temperature': 44.86, 'humidity': 46.69}
🚨 Critical temperature level!!! Temperature: 44.86
Alert sent to tania_temperature_alerts successfully.
✅ Humidity 46.69 is within normal range, no alert sent.

Received message: {'sensor_id': 12, 'time': '2025-02-02 08:39:37', 'temperature': 32.32, 'humidity': 61.74}
✅ Temperature 32.32 is within normal range, no alert sent.
✅ Humidity 61.74 is within normal range, no alert sent.

Received message: {'sensor_id': 6, 'time': '2025-02-02 08:39:45', 'temperature': 39.27, 'humidity': 54.34}
✅ Temperature 39.27 is within normal range, no alert sent.
✅ Humidity 54.34 is within normal range, no alert sent.

Received message: {'sensor_id': 17, 'time': '2025-02-02 08:39:47', 'temperature': 31.63, 'humidity': 23.78}
✅ Temperature 31.63 is within normal range, no alert sent.
✅ Humidity 23.78 is within normal range, no alert sent.

Received message: {'sensor_id': 3, 'time': '2025-02-02 08:39:12', 'temperature': 40.15, 'humidity': 47.5}
🚨 Critical temperature level!!! Temperature: 40.15
Alert sent to tania_temperature_alerts successfully.
✅ Humidity 47.5 is within normal range, no alert sent.

Received message: {'sensor_id': 14, 'time': '2025-02-02 08:39:20', 'temperature': 31.79, 'humidity': 77.52}
✅ Temperature 31.79 is within normal range, no alert sent.
✅ Humidity 77.52 is within normal range, no alert sent.

Received message: {'sensor_id': 14, 'time': '2025-02-02 08:39:49', 'temperature': 43.37, 'humidity': 75.19}
🚨 Critical temperature level!!! Temperature: 43.37
Alert sent to tania_temperature_alerts successfully.
✅ Humidity 75.19 is within normal range, no alert sent.

Received message: {'sensor_id': 2, 'time': '2025-02-02 08:39:16', 'temperature': 34.09, 'humidity': 48.56}
✅ Temperature 34.09 is within normal range, no alert sent.
✅ Humidity 48.56 is within normal range, no alert sent.

Received message: {'sensor_id': 16, 'time': '2025-02-02 08:39:29', 'temperature': 44.61, 'humidity': 16.75}
🚨 Critical temperature level!!! Temperature: 44.61
Alert sent to tania_temperature_alerts successfully.
🚨 Critical humidity level!!! Humidity: 16.75
Alert sent to tania_humidity_alerts successfully.

Received message: {'sensor_id': 18, 'time': '2025-02-02 08:39:39', 'temperature': 34.59, 'humidity': 40.69}
✅ Temperature 34.59 is within normal range, no alert sent.
✅ Humidity 40.69 is within normal range, no alert sent.

Received message: {'sensor_id': 2, 'time': '2025-02-02 08:39:41', 'temperature': 36.03, 'humidity': 74.78}
✅ Temperature 36.03 is within normal range, no alert sent.
✅ Humidity 74.78 is within normal range, no alert sent.

Received message: {'sensor_id': 2, 'time': '2025-02-02 08:39:43', 'temperature': 30.35, 'humidity': 35.55}
✅ Temperature 30.35 is within normal range, no alert sent.
✅ Humidity 35.55 is within normal range, no alert sent.