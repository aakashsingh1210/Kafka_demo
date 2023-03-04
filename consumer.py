from kafka import KafkaConsumer
import json

if __name__=="__main__":
    consumer=KafkaConsumer(
        "tweets",                  # name of topic
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='consumer_group_a',      # we need to specify group id
    )
    print("starting the consumer")
    for msg in consumer:
        print("Registered User = {}".format(json.loads(msg.value)))  # we need to de-serialize message