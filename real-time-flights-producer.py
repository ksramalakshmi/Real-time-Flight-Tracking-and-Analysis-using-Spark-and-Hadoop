import json
import time
import pymysql
import urllib.request
from urllib.request import Request, urlopen

from kafka import KafkaProducer

from kafka.admin import KafkaAdminClient, NewTopic

# Create an admin client
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

# Create topics
topics = ['UK', 'speed800', 'alt5000']
new_topic_list = []

for topic in topics:
	num_partitions = 1
	replication_factor = 1
	new_topic = NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
	new_topic_list.append(new_topic)

# Create the topic on the Kafka broker
try:
	admin_client.create_topics(new_topics=new_topic_list)
except:
	pass

# Connect to the database
conn = pymysql.connect(host='localhost', user='root', password='Password123#@!', database='stream_dbt')

# Create a table to store the key-value pairs
with conn.cursor() as cursor:
    cursor.execute('CREATE TABLE IF NOT EXISTS flights(event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, hex VARCHAR(20) NOT NULL,reg_number VARCHAR(20) NOT NULL,flag VARCHAR(20) NOT NULL,lat FLOAT(20,8) NOT NULL,lng FLOAT(20,8) NOT NULL,alt FLOAT(20,8) NOT NULL,dir FLOAT(20,8) NOT NULL,speed FLOAT(20,8) NOT NULL,squawk VARCHAR(8) NOT NULL,flight_number VARCHAR(10) NOT NULL,flight_icao VARCHAR(20) NOT NULL,flight_iata VARCHAR(20) NOT NULL,dep_icao VARCHAR(20) NOT NULL,dep_iata VARCHAR(20) NOT NULL,arr_icao VARCHAR(20) NOT NULL,arr_iata VARCHAR(20) NOT NULL,airline_icao VARCHAR(20) NOT NULL,airline_iata VARCHAR(20) NOT NULL,aircraft_icao VARCHAR(20) NOT NULL,updated FLOAT(20,8) NOT NULL,status VARCHAR(40) NOT NULL,PRIMARY KEY (hex))')

# Define a function to insert data into the table
def insert_data(hex, reg_number, flag, lat, lng, alt, dir, speed, squawk, flight_number, flight_icao, flight_iata, dep_icao, dep_iata, arr_icao, arr_iata, airline_icao, airline_iata, aircraft_icao, updated, status):
    with conn.cursor() as cursor:
        cursor.execute('INSERT INTO flights (hex, reg_number, flag, lat, lng, alt, dir, speed, squawk, flight_number, flight_icao, flight_iata, dep_icao, dep_iata, arr_icao, arr_iata, airline_icao, airline_iata, aircraft_icao, updated, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',(hex, reg_number, flag, lat, lng, alt, dir, speed, squawk, flight_number, flight_icao, flight_iata, dep_icao, dep_iata, arr_icao, arr_iata, airline_icao, airline_iata, aircraft_icao, updated, status)) 
    conn.commit()

API_KEY = "dc019782-c207-4c85-85d3-bb4d4e1d0845"

url = "https://airlabs.co/api/v9/flights?api_key={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")
while True:

 response = urllib.request.Request(url,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0'})
 flights = json.loads(urlopen(response).read().decode())
 
 for flight in flights["response"]:
  print(flight)
  # Insert the data into the table
  try:
  	insert_data(flight['hex'], flight['reg_number'],flight['flag'],flight['lat'],flight['lng'],flight['alt'],flight['dir'],flight['speed'],flight['squawk'],flight['flight_number'],flight['flight_icao'],flight['flight_iata'],flight['dep_icao'],flight['dep_iata'],flight['arr_icao'],flight['arr_iata'],flight['airline_icao'],flight['airline_iata'],flight['aircraft_icao'],flight['updated'],flight['status'])  
  except:
  	continue
  if flight['flag']=='UK':
  	producer.send('UK', json.dumps(flight).encode())
  elif flight['speed']>800:
  	producer.send('speed800', json.dumps(flight).encode())  	
  elif flight['alt']<5000:
  	producer.send('alt5000', json.dumps(flight).encode())  
  	
  #producer.send("flight-realtime", json.dumps(flight).encode())
  print("{} Produced {} station records".format(time.time(), len(flights)))
  time.sleep(1)

# Close the database connection
conn.close()

