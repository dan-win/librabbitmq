import _pyrabbitmq

ConnectionError = _pyrabbitmq.ConnectionError
ChannelError = _pyrabbitmq.ChannelError

__version__ = "0.0.1"
__all__ = ["Connection", "Message", "ConnectionError", "ChannelError"]


class Message(object):
    _props = ("content_type", "content_encoding",
              "priority", "delivery_mode")

    def __init__(self, body, content_type=None, content_encoding=None,
            priority=None, delivery_mode=None):
        self.body = body
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.priority = priority

    @property
    def properties(self):
        return dict((k, getattr(self, k)) for k in self._props
                    if getattr(self, k, None) is not None)


class Channel(object):

    def __init__(self, conn, chanid):
        self.conn = conn
        self.chanid = chanid

    def basic_publish(self, message, exchange=None, routing_key=None,
            mandatory=False, immediate=False):
        self.conn._basic_publish(exchange=exchange, routing_key=routing_key,
                                 message=message.body,
                                 properties=message.properties,
                                 channel=self.chanid,
                                 mandatory=mandatory, immediate=immediate)

    def close(self):
        self.conn._remove_channel(self)



class Connection(_pyrabbitmq.connection):
    curchan = 0
    channels = set()

    def __init__(self, hostname="localhost", port=5672, userid="guest",
            password="guest", vhost="/"):
        self.hostname = hostname
        self.port = port
        self.userid = userid
        self.password = password
        self.vhost = vhost
        super(Connection, self).__init__(hostname=hostname, port=port,
                                     userid=userid, password=password,
                                     vhost=vhost)

    def channel(self):
        # TODO need to reuse channel numbers.
        self.curchan += 1
        self._channel_open(self.curchan)
        channel = Channel(self, self.curchan)
        self.channels.add(channel)
        return channel

    def _remove_channel(self, channel):
        self._channel_close(channel.chanid)
        self.channels.remove(channel)

