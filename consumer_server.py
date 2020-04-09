import json

from kafka import KafkaConsumer

def deserilize(bytesstr):
    return bytesstr.decode('utf-8')

def consume():
    consumer = KafkaConsumer('com.udacity.ds.proj2.crimes',
                             group_id='my-group',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='latest',
                             key_deserializer=deserilize,
                             value_deserializer=lambda x: json.loads(deserilize(x)))
    for message in consumer:
        print(f'key {message.key}, value {message.value}')
    

if __name__ == '__main__':
    consume()