# import pandas as pd
# from extracter import main as mn
# import json
# file= open('/Users/shubhamkgupta/Desktop/Learning/KAFKA/Kafka_project/files/HealthAuth-20220915 2.txt','r')
# print(file.readline())
#
# while True:
#     mydict={}
#     for i in range(4):
#         text=file.readline()
#         mydict.update(mn(text))
#         mydict1=json.dumps(mydict)
#     print(type(mydict1))
#
#

# #!/usr/bin/env python


from confluent_kafka import Producer
from extracter import main as mn
import json

if __name__ == '__main__':

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092',
        'sasl.username':     'BB3OPU4HS6CO74YZ',
        'sasl.password':     'MehYB+jrmAUB7o7qR0SbojHEI6kvwYhJWQPgFGYEPgT9bQg7OwbEXHXy+3jlXaUL',

        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key} ".format(
                topic=msg.topic(), key=msg.value()))

    # Produce data by selecting random values from these lists.
    topic = "KAFKA_TRAINING"
    file = open('files/HealthAuth-20220915 2.txt', 'r')
    temp=file.readline()

    while True:
        mydict = {}
        try:
            for i in range(4):
                text = file.readline()
                mydict.update(mn(text))
                mydict1=json.dumps(mydict)
            producer.produce(topic, mydict1, callback=delivery_callback)
        except TypeError:
            break





    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()



