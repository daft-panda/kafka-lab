from kafka import KafkaProducer
import jsonpickle

producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

with open("../downloader/tweets.txt", 'r') as tweetsfile:
    for tweet in tweetsfile:
        clean = tweet.strip('\n')
        twete = jsonpickle.decode(clean)
        producer.send("Panda_Tweets", clean.encode('utf-8'), str(twete).encode('utf-8'))

producer.flush()

producer.close()
