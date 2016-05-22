from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener

# aanmaken consumer, we gaan de offsets van de messages waarmee we klaar zijn zelf committen
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", enable_auto_commit="false")


# aanmaken van de ConsumerRebalancelistener
class CRL(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        # acties wanneer er partities worden verwijderd
        return

    def on_partitions_assigned(self, assigned):
        # acties wanneer er partities worden toegewezen aan deze consumer
        print(assigned)
        consumer.seek(TopicPartition("Panda_Tweets", 0), 0)
        return


# subscribe de juiste topic, koppel de ConsumerRebalanceListener
consumer.subscribe(["Panda_Tweets"], listener=CRL())

# voor elke message dat wordt ontvangen ...
for msg in consumer:
    # doe iets met de msg
    print(msg.value)
    # markeer de offset van de message in de partitie als gedaan, indien er een andere consumer deze partitie uitleest (en niet handmatig vanaf een andere offset leest), zal vanaf de volgende message terug worden begonnen met werken
    consumer.commit_async()

consumer.close()
