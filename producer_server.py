import json
import logging
import time

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            for crime in json.load(f):
                message = self.dict_to_binary(crime)
                # TODO send the correct data
                future = self.send(self.topic, value=message, key=crime['crime_id'].encode('utf-8'))
                try:
                    record_metadata = future.get(timeout=10)
                    logging.debug(f'send 1 message to topic {self.topic} ' + 
                                 f'partition {record_metadata.partition} ' +
                                 f'at offset {record_metadata.offset}')
                except KafkaError:
                    logging.exception("failed to send 1 message")
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
        