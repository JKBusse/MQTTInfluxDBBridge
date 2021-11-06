import re
from typing import NamedTuple
from tendo import singleton
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from datetime import datetime
import json

# Is used to prevent multiple instances of this script running simulaneously
me = singleton.SingleInstance()

# Influx config
INFLUXDB_ADDRESS = '127.0.0.1'
INFLUXDB_USER = 'INFLUX_USER'
INFLUXDB_PASSWORD = 'INFLUX_PASSWORD'
INFLUXDB_DATABASE = 'INFLUX_DATABASE'

# mqtt config
MQTT_ADDRESS = 'BROKER_URL'
MQTT_USER = 'MQTT_USER'
MQTT_PASSWORD = 'MQTT_PASSWORD'
MQTT_TOPIC = '/user/+/grafana/+/+'
MQTT_REGEX = '/user/([^/]+)/grafana/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

class SensorData(NamedTuple):
    user: str
    device: str
    measurement: str
    value: float
    json: str

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the mqtt server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def is_num(json):
    """Checks if the payload is a valid float-number."""
    try:
        float(json)
    except ValueError as e:
        return False
    return True

def _parse_mqtt_message(topic, payload):
    """Creates SensorData object from MQTT payload. Supports float values or JSON"""
    match = re.match(MQTT_REGEX, topic)
    if match:
        user = match.group(1)
        device = match.group(2)
        measurement = match.group(3)
        if device == 'status':
            return None
        if(is_num(payload)):
            return SensorData(user, device, measurement, float(payload), "")
        else:
            return SensorData(user, device, measurement, 0.0, payload)
    else:
        return None

def _send_sensor_data_to_influxdb(sensor_data):
    """Creates a json object to be sent to the influxdb. If sensor_data contains a json object load it directly, otherwise only send the value"""
    json_body = ""
    try:
        if(len(sensor_data.json) == 0):
            json_body = [
                {
                    'measurement': sensor_data.measurement,
                    'tags': {
                        'device': sensor_data.device,
                        'user': sensor_data.user
                    },
                    'fields': {
                        'value': sensor_data.value
                    }
                }
            ]
        else:
            json_body = [
                {
                    'measurement': sensor_data.measurement,
                    'tags': {
                        'device': sensor_data.device,
                        'user': sensor_data.user
                    },
                    'fields': json.loads(sensor_data.json)
                }
            ]
    except Exception as e:
        print("Encountered exception while creating json object for the influxdb")
        print(e)
    else:
        influxdb_client.write_points(json_body)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the mqtt server."""
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    #print(current_time + ' ' + msg.topic + ' ' + str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)

def _init_influxdb_database():
    """Checks if the given INFLUXDB_DATABASE exists on the server, creates one if it does not"""
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

def main():
    # Init influxdb connection
    _init_influxdb_database()

    # Init mqtt connection
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
