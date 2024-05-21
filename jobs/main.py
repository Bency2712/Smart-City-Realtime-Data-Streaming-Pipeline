from datetime import timedelta
import datetime
import os
import random
import time
import uuid
from confluent_kafka import SerializingProducer #because we need to produce record
import simplejson as json

#Latitude and longitude For London and  Birmingham 
LONDON_COORDINATES = {"latitude":51.5074, "longitude": -0.1278} # you can get it online
BIRMINGHAM_COORDINATES = {"latitude":52.4862, "longitude": -1.8904}

#movement increment for both latitude and longitude
#here we are finding the difference between Birmingham's and London's Latitude and Longitude and dividing it by 100.
# By breaking down the journey into smaller increments, you can simulate or monitor the journey in finer detail.
# This can help in updating the system more frequently with the current location and other collected data.
#This simulates the journey in smaller steps.
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100


# Environment variables for configuration
# These environment variables provide configuration details for the Kafka producer and topics.
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')  # Kafka bootstrap server address

#Configuration for Kafka bootstrap servers and topics is done using environment variables with default values provided.
# Topics for various types of live data being collected from the vehicle, map, camera, etc.
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# Record the start time and location
start_time = datetime.datetime.now()
start_location = LONDON_COORDINATES.copy()


#HELPER FUNCTIONS

def get_next_time():
    #increments the start time by a random amount between 30 and 60 seconds
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) #update frequency
    return start_time

#updates gps data information
def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40), #km/h
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

    #generate traffic camera data
def generate_traffic_camera_data(device_id, timestamp,location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

#weather data 
def generate_weather_data(device_id,timestamp, location):
    return{
         'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0,25),
        'windSpeed': random.uniform(0,100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500)
    }

#emergency data
def generate_emergency_incident_data(device_id, timestamp, location):
    return{
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police','None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active','Resolved']),
        'description': 'Description of the Incident'
        }    

#updates the vehicle's latitude and longitude by the predefined increments and adds some randomness to simulate realistic movement
def simulate_vehicle_movement():
    global start_location
    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #add some randomness to simulate the actual road travel #So it doesn't spike above the given range
    start_location['latitude'] += random.uniform(-0.0005,0.0005)
    start_location['longitude'] += random.uniform(-0.0005,0.0005)

    return start_location

#generate_vehicle_data creates a dictionary with simulated vehicle data, including ID, 
#device ID, timestamp, location, speed, direction, make, model, year, and fuel type. 
#Note that uuid needs to be imported for generating unique IDs.

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement() # Get the next location
    return {
        'id': uuid.uuid4(), # Unique ID for the record
        'deviceId' : device_id, # Device ID
        'timestamp' : get_next_time().isoformat(), # Timestamp of the record
        'location': (location['latitude'], location['longitude']), # Current location
        'speed': random.uniform(10,40), # Random speed between 10 and 40
        'direction': 'North-East', # Static direction
        'make': 'BMW', # Static make of the vehicle
        'model': 'CS500', # Static model of the vehicle
        'year': 2024, # Static year of the vehicle
        'fuelType': 'Hybrid' # Static fuel type

    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()    #for the data to get delivered
##########################Main Function################################

#simulate_journey continuously generates vehicle data and prints it. 
#The loop breaks immediately,which seems like a placeholder for actual continuous simulation.
def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)     # Generate vehicle data
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])  #to get the gps data of the vehicle
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Nikon-Cam123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        
        #if the vehicle reaches Birmingham
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] 
        and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending..')
            break

        #print(vehicle_data)     # Print the data (for demonstration purposes)
        #print(gps_data)
        #print(traffic_camera_data)
        #print(weather_data)
        #print(emergency_incident_data)


        #get the producer to produce the data, we can create a function for that
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)

        #break   # Exit the loop (this should be removed for continuous simulation)

#Entry Point
if __name__ == "__main__":
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # Kafka bootstrap servers
        'error_cb': lambda err: print(f'Kafka error: {err}')  # Error callback function for Kafka
    }

    producer = SerializingProducer(producer_config)  # Creating a Kafka producer object

   #this simulate journey is where we are going to have all the information

    try:
        simulate_journey(producer, 'Vehicle-Bency-123')  # Start simulating the journey for a specific vehicle
    except KeyboardInterrupt:
        print('Simulation ended by the user')  # If simulation is ended by the user
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')  # Handling unexpected errors