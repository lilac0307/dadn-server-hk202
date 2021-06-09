import socket
import http.server
import threading
import mysql.connector
import json
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from datetime import datetime
import requests

PORT = 8080
mqttClient = {}

mqttUser = "CSE_BBC"
mqttKey = ""
mqttUser1 = "CSE_BBC1"
mqttKey1 = ""
def getMqttKey():
    global mqttKey
    global mqttKey1
    getURL = "http://dadn.esp32thanhdanh.link"
    res = requests.get(url = getURL)
    data = res.json()
    mqttKey = data["keyBBC"]
    mqttKey1 = data["keyBBC1"]

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection.")

def on_message(client, userdata, msg):  
    mqtt_name = msg.topic.split('/')[0]
    feed_name = msg.topic.split('/')[2]
    username = mqttClient[mqtt_name][2]
    data = json.loads(msg.payload)
    #relay is both control device and sensor, so as to record activity

    cursor = db.cursor()
    if data["name"] == "RELAY":
        cursor.execute("SELECT id FROM control_device WHERE id = %s", (data["id"],))
        result = cursor.fetchall()
        if not result:
            cursor.execute("INSERT INTO control_device (id, name, feed_username, username) VALUES (%s, %s, %s, %s)", (data["id"], "default name", mqtt_name, username))
    else:
        cursor.execute("SELECT lowest_stats, highest_stats, id FROM control_device WHERE sensor_id = %s", (data["id"], ))
        result = cursor.fetchall()
        if result: #if there is a control device associated with this sensor
            lowest_stats = result[0][0]
            highest_stats = result[0][1]
            control_device_id = result[0][2]
            turn_device_on = "0"
            if float(data["data"]) < lowest_stats or float(data["data"]) > highest_stats:
                turn_device_on = "1"
            
            #only send if there is a state change
            cursor.execute('''
                SELECT value FROM sensor_stat 
                WHERE sensor_type = %s AND time = (
                    SELECT MAX(time) FROM sensor_stat
                    WHERE sensor_type = %s AND sensor_id = %s
                )
            ''', ("RELAY", "RELAY", control_device_id))
            result = cursor.fetchall()
            print(result)
            print(turn_device_on)
            if result:
                if result[0][0] != turn_device_on:
                    auth = {}
                    auth["username"] = "CSE_BBC1"
                    auth["password"] = mqttClient["CSE_BBC1"][1]
                    payload = "{\"id\":\""+str(control_device_id)+"\",\"name\":\"RELAY\",\"data\":\""+turn_device_on+"\",\"unit\":\"\"}"
                    publish.single(topic="CSE_BBC1/feeds/bk-iot-relay", payload=payload, qos=0, retain=False, hostname="io.adafruit.com", auth=auth)
            else:
                cursor.execute("SELECT id FROM sensor WHERE username = %s AND id = %s AND sensor_type = %s", (username, str(control_device_id), "RELAY"))
                result = cursor.fetchall()
                if not result: #if receive data from sensor but sensor not in database
                    if data["name"] == "LIGHT" or data["name"] == "RELAY":
                        mqtt_name = "CSE_BBC1"
                    cursor.execute("INSERT INTO sensor (id, feed_name, feed_username, username ,sensor_type) VALUES (%s, %s, %s, %s, %s)",(str(control_device_id), "bk-iot-relay", "CSE_BBC1", username, "RELAY"))
                now = datetime.now()
                cursor.execute("INSERT INTO sensor_stat (sensor_id, time, sensor_type, value, unit) VALUES (%s, %s, %s, %s, %s)", (str(control_device_id), now.strftime('%Y-%m-%d %H:%M:%S'), "RELAY", turn_device_on, ""))

    cursor.execute("SELECT id FROM sensor WHERE username = %s AND id = %s AND sensor_type = %s", (username, data["id"], data["name"]))
    result = cursor.fetchall()
    if not result: #if receive data from sensor but sensor not in database
        #if data["name"] == "LIGHT" or data["name"] == "RELAY":
        #    mqtt_name = "CSE_BBC1"
        cursor.execute("INSERT INTO sensor (id, feed_name, feed_username, username ,sensor_type) VALUES (%s, %s, %s, %s, %s)",(data["id"], feed_name, mqtt_name, username, data["name"]))
    now = datetime.now()
    #insert into stats of corresponding sensor
    cursor.execute("INSERT INTO sensor_stat (sensor_id, time, sensor_type, value, unit) VALUES (%s, %s, %s, %s, %s)", (data["id"], now.strftime('%Y-%m-%d %H:%M:%S'), data["name"], data["data"], data["unit"]))

    db.commit()

def mqttLoop(mqtt_info):
    client = mqtt.Client()
    mqttClient[mqtt_info[0]] = (client, mqtt_info[1], mqtt_info[2])
    client.username_pw_set(mqtt_info[0], mqtt_info[1])
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect("io.adafruit.com")
    client.subscribe(mqtt_info[0] + "/feeds/bk-iot-relay")
    client.subscribe(mqtt_info[0] + "/feeds/bk-iot-light")
    client.subscribe(mqtt_info[0] + "/feeds/bk-iot-soil")
    client.subscribe(mqtt_info[0] + "/feeds/bk-iot-temp-humid")
    
    client.loop_forever()

def runMqtt():
    cursor = db.cursor()
    #cursor.execute("SELECT mqtt_name, mqtt_key, username FROM garden")
    #result = cursor.fetchall()
    #cursor.execute("SELECT username FROM user")
    #cursor_result = cursor.fetchall()
    cursor.execute('''
        SELECT DISTINCT feed_username, feed_key, sensor.username
        FROM sensor JOIN mqtt_feed
        ON mqtt_feed.username = sensor.feed_username
    ''')
    result = cursor.fetchall()
    print(result)
    for item in result:
        threading.Thread(target=mqttLoop, args=(item, )).start()
        #threading.Thread(target=mqttLoop, args=((mqttUser, mqttKey, item[0]), )).start()
        #threading.Thread(target=mqttLoop, args=((mqttUser1, mqttKey1, item[0]), )).start()



def connectDatabase():
    return mysql.connector.connect(host="localhost",user="root",password="binh",database="dadn")

class myHandler(http.server.BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def _html(self, message):
        content = f"<html><body><h1>{message}</h1></body></html>"
        return content.encode("utf8")  # NOTE: must return a bytes object!

    def do_GET(self):
        #all requests received must contain username (and maybe key/password later)
        self._set_headers()
        separator_index = self.path.find("?")
        if separator_index != -1:
            request = self.path[:separator_index]
            param_list = self.path[separator_index + 1:].split("&")
        else:
            request = self.path
            param_list = []
        params = {}
        for pair in param_list:
            key_value = pair.split("=")
            params[key_value[0]] = key_value[1]

        if request =='/get_device_states':
            cursor = db.cursor()
            cursor.execute('''
                SELECT id, control_device_type FROM control_device
                WHERE feed_username = %s
            ''', (params["mqtt_name"], ))
            id_name_list = cursor.fetchall()
            result = [] #list of (id, name, state)
            for id_name in id_name_list:
                cursor.execute('''
                    SELECT value FROM sensor_stat 
                    WHERE sensor_type = %s AND time = (
                        SELECT MAX(time) FROM sensor_stat
                        WHERE sensor_type = %s AND sensor_id = %s
                    )
                ''', ("RELAY", "RELAY", id_name[0]))
                data = cursor.fetchall()
                if data:
                    result.append(id_name + data[0])
                else:
                    result.append(id_name + ("0", ))

            self.wfile.write(json.dumps(result).encode("utf8"))

        elif request == '/get_mqtt_info':
            cursor = db.cursor()
            cursor.execute('''
                SELECT username, feed_key FROM mqtt_feed
                WHERE username IN (
                    SELECT feed_username FROM sensor
                    WHERE username = %s
                )
            ''', (params["username"],))
            result = cursor.fetchall()
            #result = []
            #getMqttKey()
            #result.append((mqttUser, mqttKey))
            #result.append((mqttUser1, mqttKey1))
            self.wfile.write(json.dumps(result).encode("utf8"))

        elif request == '/auth':
            cursor = db.cursor()
            cursor.execute("SELECT username FROM user WHERE username = %s AND password = %s", (params["username"], params["password"]))
            result = cursor.fetchall()
            if result:
                self.wfile.write("valid".encode("utf8"))
            else:
                self.wfile.write("invalid".encode("utf8"))

        elif request == '/get_control_device_info':
            response = []
            cursor = db.cursor()
            cursor.execute("SELECT sensor_id, control_device_type FROM control_device WHERE id = %s", (params["control_device_id"],))
            result = cursor.fetchall()
            response.append(result[0][0])
            response.append(result[0][1])
            cursor.execute("SELECT start_time, end_time FROM control_device_time WHERE control_device_id = %s", (params["control_device_id"],))
            result = cursor.fetchall()
            #response.append(result)
            self.wfile.write(json.dumps(response).encode("utf8"))

    def do_POST(self):
        self._set_headers()
        content_length = int(self.headers['Content-Length']) # size of data
        data_bytes = self.rfile.read(content_length) # data (type: bytes)
        data_str = data_bytes.decode("utf-8")
        cursor = db.cursor()
        
        if self.path == '/send_data':
            pass

def runHttpServer(server_class=http.server.HTTPServer, handler_class=myHandler):
    server_address = ('', PORT)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

db = connectDatabase()
runMqtt()
threading.Thread(target=runHttpServer).start()
