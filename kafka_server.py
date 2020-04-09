import os

import producer_server


def run_kafka_server():
	# TODO get the json file path
    input_file = os.path.join(os.path.dirname(__file__), "police-department-calls-for-service.json")

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.ds.proj2.crimes",
        bootstrap_servers="localhost:9092",
        client_id=""
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
