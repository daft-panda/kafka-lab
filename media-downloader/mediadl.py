import urllib2
import base64
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import jsonpickle

consumer = KafkaConsumer(bootstrap_servers="localhost:9092", enable_auto_commit="false")
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')


def download_media(url):
    try:
        response = urllib2.urlopen(url)
        return base64.b64encode(response.read())
    except urllib2.HTTPError as e:
        print("Failed to download {0}: {1}".format(url, e.message))
        return False


class CRL(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        return

    def on_partitions_assigned(self, assigned):
        print(assigned)
        consumer.seek(TopicPartition("Panda_Tweets", 0), 0)
        return

consumer.subscribe(["Panda_Tweets"], listener=CRL())

for msg in consumer:
    twete = jsonpickle.decode(msg.value)
    if 'media' in twete.entities:
        for media in twete.entities['media']:
            print(media['media_url'] + "\n")
            media['data'] = download_media(media['media_url'])
            if media['data'] == False:
                continue
            else:
                producer.send("Panda_Media", jsonpickle.encode(twete).encode('UTF-8'), str(twete.id).encode('UTF-8'))

    consumer.commit_async()


producer.close()
consumer.close()

