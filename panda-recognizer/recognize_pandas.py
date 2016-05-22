from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from classify_image import maybe_download_and_extract, run_inference_on_image, NodeLookup, create_graph
import base64
import jsonpickle

consumer = KafkaConsumer(bootstrap_servers="localhost:9092", enable_auto_commit="false")
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')


def is_jpg(raw):
    data = raw[0:11]
    if data[:4] != '\xff\xd8\xff\xe0': return False
    if data[6:] != 'JFIF\0': return False
    return True


class CRL(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        return

    def on_partitions_assigned(self, assigned):
        print(assigned)
        consumer.seek(TopicPartition("Panda_Media", 0), 0)
        return

maybe_download_and_extract()

consumer.subscribe(["Panda_Media"], listener=CRL())

# Creates graph from saved GraphDef.
create_graph()

# Creates node ID --> English string lookup.
node_lookup = NodeLookup()

# read messages
for msg in consumer:
    # deserialize from json
    twete = jsonpickle.decode(msg.value)

    # for each media object in tweet
    for media in twete.entities['media']:
        # base64 jpg string to bytes
        image_data = base64.b64decode(media['data'])

        # make sure image is jpeg
        if is_jpg(image_data) == False:
            print("Invalid panda {0}".format(msg.offset))
            continue

        # run tensorflow image recognition
        predictions, top_k = run_inference_on_image(image_data)

        # determine if there is a panda match, if so how good the match is (score should be > 0.5)
        for node_id in top_k:
            # giant panda = 169
            # red panda = 7
            # human_string = node_lookup.id_to_string(node_id)
            score = predictions[node_id]

            if node_id in [7, 169] and score > 0.5:
                # WE HAVE PANDA
                media['panda_node_id'] = str(node_id)

                producer.send("Panda_Image_Tweets", jsonpickle.encode(twete).encode('UTF-8'), str(twete.id).encode('UTF-8'))
                break

            # print('%i : %s (score = %.5f)' % (node_id, human_string, score))

    consumer.commit_async()

producer.close()
consumer.close()

