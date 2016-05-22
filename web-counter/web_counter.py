import sys
from threading import Thread

from autobahn.twisted.websocket import WebSocketServerFactory, \
    WebSocketServerProtocol, \
    listenWS
from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from twisted.internet import reactor
from twisted.python import log
from twisted.web.server import Site
from twisted.web.static import File

consumer = KafkaConsumer(bootstrap_servers="localhost:9092", enable_auto_commit="false")


class CRL(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        return

    def on_partitions_assigned(self, assigned):
        print(assigned)
        consumer.seek(TopicPartition("Panda_Image_Tweets", 0), 0)
        return


class BroadcastServerProtocol(WebSocketServerProtocol):
    def onOpen(self):
        self.factory.register(self)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            msg = "{} from {}".format(payload.decode('utf8'), self.peer)
            # self.factory.broadcast(msg)

    def connectionLost(self, reason):
        WebSocketServerProtocol.connectionLost(self, reason)
        self.factory.unregister(self)


class BroadcastServerFactory(WebSocketServerFactory):
    """
    Simple broadcast server broadcasting any message it receives to all
    currently connected clients.
    """

    def __init__(self, url):
        WebSocketServerFactory.__init__(self, url)
        self.clients = []
        self.tickcount = 0
        self.tick()

    def tick(self):
        self.tickcount += 1
        # self.broadcast("tick %d from server" % self.tickcount)
        # reactor.callLater(1, self.tick)

    def register(self, client):
        if client not in self.clients:
            print("registered client {}".format(client.peer))
            self.clients.append(client)

    def unregister(self, client):
        if client in self.clients:
            print("unregistered client {}".format(client.peer))
            self.clients.remove(client)

    def broadcast(self, msg):
        print("broadcasting message '{}' ..".format(msg))
        for c in self.clients:
            c.sendMessage(msg.encode('utf8'))
            print("message sent to {}".format(c.peer))


factory = BroadcastServerFactory(u"ws://127.0.0.1:8081")
factory.protocol = BroadcastServerProtocol


def read_from_kafka():
    consumer.subscribe(["Panda_Image_Tweets"], listener=CRL())

    print('mimimi')

    # read messages
    for msg in consumer:
        factory.broadcast(msg.value)
        consumer.commit_async()


def serve_web():
    listenWS(factory)

    webdir = File(".")
    web = Site(webdir)
    reactor.listenTCP(8080, web)

    reactor.run()


if __name__ == '__main__':
    log.startLogging(sys.stdout)

    Thread(target=read_from_kafka).start()
    Thread(target=serve_web()).start()
