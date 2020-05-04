# coding: utf-8
from __future__ import absolute_import

from six.moves import xrange

import socket
import unittest

from librabbitmq import (
    Message, 
    Connection, 
    ConnectionError, 
    ChannelError,

    StateConflict,
    CannotCreateSocket,
    CannotOpenSocket,
    ExchangeOrQueueNotFound,
    LoginError,
    )
TEST_QUEUE = 'pyrabbit.testq'
RABBIT_MQ_PORT = 15672

class test_Channel(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(host='localhost:%s' % RABBIT_MQ_PORT, userid='guest',
                                     password='guest', virtual_host='/')
        self.channel = self.connection.channel()
        self.channel.queue_delete(TEST_QUEUE)
        self._queue_declare()

    def test_send_message(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.assertGreater(self.channel.queue_purge(TEST_QUEUE), 2)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

    def test_nonascii_headers(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8',
                            headers={'key': r'¯\_(ツ)_/¯'}))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

    def _queue_declare(self):
        self.channel.exchange_declare(TEST_QUEUE, 'direct')
        x = self.channel.queue_declare(TEST_QUEUE)
        self.assertEqual(x.message_count, x[1])
        self.assertEqual(x.consumer_count, x[2])
        self.assertEqual(x.queue, TEST_QUEUE)
        self.channel.queue_bind(TEST_QUEUE, TEST_QUEUE, TEST_QUEUE)

    def test_basic_get_ack(self):
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)
        while True:
            x = self.channel.basic_get(TEST_QUEUE)
            if x:
                break
        self.assertIs(self.channel, x.channel)
        self.assertIn('message_count', x.delivery_info)
        self.assertIn('redelivered', x.delivery_info)
        self.assertEqual(x.delivery_info['routing_key'], TEST_QUEUE)
        self.assertEqual(x.delivery_info['exchange'], TEST_QUEUE)
        self.assertTrue(x.delivery_info['delivery_tag'])
        self.assertTrue(x.properties['content_type'])
        self.assertTrue(x.body)
        x.ack()

    def test_timeout_burst(self):
        """Check that if we have a large burst of messages in our queue
        that we can fetch them with a timeout without needing to receive
        any more messages."""

        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        for i in xrange(100):
            self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

        messages = []

        def cb(x):
            messages.append(x)
            x.ack()

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        for i in xrange(100):
            self.connection.drain_events(timeout=0.2)

        self.assertEqual(len(messages), 100)

    def test_timeout(self):
        """Check that our ``drain_events`` call actually times out if
        there are no messages."""
        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        self.channel.basic_publish(message, TEST_QUEUE, TEST_QUEUE)

        messages = []

        def cb(x):
            messages.append(x)
            x.ack()

        self.channel.basic_consume(TEST_QUEUE, callback=cb)
        self.connection.drain_events(timeout=0.1)

        with self.assertRaises(socket.timeout):
            self.connection.drain_events(timeout=0.1)
        self.assertEqual(len(messages), 1)

    def tearDown(self):
        if self.channel and self.connection.connected:
            self.channel.queue_purge(TEST_QUEUE)
            self.channel.close()
        if self.connection:
            try:
                self.connection.close()
            except ConnectionError:
                pass


class test_Delete(unittest.TestCase):

    def setUp(self):
        self.connection = Connection(host='localhost:%s' % RABBIT_MQ_PORT, userid='guest',
                                     password='guest', virtual_host='/')
        self.channel = self.connection.channel()
        self.TEST_QUEUE = 'pyrabbitmq.testq2'
        self.channel.queue_delete(self.TEST_QUEUE)

    def test_delete(self):
        """Test that we can declare a channel delete it, and then declare with
        different properties"""

        self.channel.exchange_declare(self.TEST_QUEUE, 'direct')
        self.channel.queue_declare(self.TEST_QUEUE)
        self.channel.queue_bind(
            self.TEST_QUEUE, self.TEST_QUEUE, self.TEST_QUEUE,
        )

        # Delete the queue
        self.channel.queue_delete(self.TEST_QUEUE)

        # Declare it again
        x = self.channel.queue_declare(self.TEST_QUEUE, durable=True)
        self.assertEqual(x.queue, self.TEST_QUEUE)

        self.channel.queue_delete(self.TEST_QUEUE)

    def test_delete_empty(self):
        """Test that the queue doesn't get deleted if it is not empty"""
        self.channel.exchange_declare(self.TEST_QUEUE, 'direct')
        self.channel.queue_declare(self.TEST_QUEUE)
        self.channel.queue_bind(self.TEST_QUEUE, self.TEST_QUEUE,
                                self.TEST_QUEUE)

        message = Message(
            channel=self.channel,
            body='the quick brown fox jumps over the lazy dog',
            properties=dict(content_type='application/json',
                            content_encoding='utf-8'))

        self.channel.basic_publish(message, self.TEST_QUEUE, self.TEST_QUEUE)

        with self.assertRaises(ChannelError):
            self.channel.queue_delete(self.TEST_QUEUE, if_empty=True)

        # We need to make a new channel after a ChannelError
        self.channel = self.connection.channel()

        x = self.channel.basic_get(self.TEST_QUEUE)
        self.assertTrue(x.body)

        self.channel.queue_delete(self.TEST_QUEUE, if_empty=True)

    def tearDown(self):
        if self.channel and self.connection.connected:
            self.channel.queue_purge(TEST_QUEUE)
            self.channel.close()
        if self.connection:
            try:
                self.connection.close()
            except ConnectionError:
                pass

class test_Exceptions(unittest.TestCase):

    def test_exceptions_relationship(self):
        self.assertTrue(issubclass(StateConflict, ConnectionError))
        self.assertTrue(issubclass(CannotCreateSocket, ConnectionError))
        self.assertTrue(issubclass(CannotOpenSocket, ConnectionError))
        self.assertTrue(issubclass(LoginError, ConnectionError))

        self.assertTrue(issubclass(ExchangeOrQueueNotFound, ChannelError))


    def test_broker_is_down(self):
        exc = None
        connection = None
        exc_raised = False
        host = 'no-such-host:%s' % RABBIT_MQ_PORT
        try:
            connection = Connection(
                host=host, 
                userid='guest',
                password='guest', 
                virtual_host='/')
        except CannotOpenSocket as e:
            exc_raised = True
        except Exception as e:
            print("We should not be here!", e)
            pass
        finally:
            if connection:
                connection.close()

        self.assertTrue(exc_raised, "Should assert CannotOpenSocket when broker not responds")


    def test_login_error(self):
        exc = None
        connection = None
        exc_raised = False
        host = 'localhost:%s' % RABBIT_MQ_PORT
        userid = 'no-such-user'
        try:
            connection = Connection(
                host=host, 
                userid=userid,
                password='guest', 
                virtual_host='/')
        except LoginError as e:
            exc_raised = True
        except Exception as e:
            print("We should not be here!", e)
            pass
        finally:
            if connection is not None:
                connection.close()

        self.assertTrue(exc_raised, "Should raise LoginError when credentials are invalid")

    def test_404_basic_get(self):
        """Ability to detect 404 error when trying to use excange or declare which as not declared before
        """
        exc = None
        connection = None
        exc_raised = False
        host = 'localhost:%s' % RABBIT_MQ_PORT
        queue = 'no-such-queue'
        try:
            with Connection(
                host=host, 
                userid='guest',
                password='guest', 
                virtual_host='/') as connection:
                with connection.channel() as channel:
                    channel.basic_get(queue=queue)

        except ExchangeOrQueueNotFound as e:
            exc_raised = True
        except Exception as e:
            print("We should not be here!", e.__class__, e)
            exc = e
            pass
        finally:
            if connection is not None:
                connection.close()

        self.assertTrue(exc_raised, 
            "Should assert ExchangeOrQueueNotFound instead of %s" % exc)


