import grpc

from threading import Semaphore
from proximo import proximo_pb2 as proximo, proximo_pb2_grpc as proximo_grpc


class AlreadyStartedException(Exception):
    """Exception signifying that message consumption has already started."""
    pass


class AutomaticAcknowledgmentException(Exception):
    """Exception signifying that acknowledge method was called in automatic acknowledgement mode."""
    pass


class ProximoConsumeClient(object):
    """Client for consuming messages from proximo."""

    def __init__(self, address, topic, consumer, grpc_certs=None, grpc_options=None):
        """
        Args:
            address (str): address of the proximo instance that the client should connect to
            topic (str): topic from which the client should read messages
            consumer (str): consumer id
        Kwargs:
            grpc_certs (str): The PEM-encoded root certificates as a byte string to be used to
                communicate with the server. If not set insecure connection will be used.
            grpc_options (dict): An optional list of key-value pairs (channel args
                in gRPC Core runtime) to configure the gRPC channel.
        """
        self._address = address
        self._topic = topic
        self._consumer = consumer
        self._options = grpc_options
        self._credentials = None

        if grpc_certs is not None:
            self._credentials = grpc.ssl_channel_credentials(root_certificates=grpc_certs)

        self._set_init_state()

    def _set_init_state(self):
        self._auto_acknowledge = False
        # Read semaphore is set to 1 and confirm to 0 as we first
        # need to read a message, before acknowledging it.
        self._read_semaphore = Semaphore(value=1)
        self._confirm_semaphore = Semaphore(value=0)
        self._confirm_id = None

        self._channel = None
        self._stub = None
        self._stream = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """"Closes the underlying gRPC connection and cleans up the object.
        It only needs to be called when not using the client as a context manager."""
        self._confirm_semaphore.release()  # Terminate sending of acknowledgements
        self._channel.close()  # Close the gRPC connection
        self._set_init_state()  # Reset state of the client, so that it can be reused

    def _connect(self):
        if self._channel is None:
            if self._credentials is None:
                self._channel = grpc.insecure_channel(self._address, options=self._options)
            else:
                self._channel = grpc.secure_channel(self._address, self._credentials, options=self._options)

        self._stub = proximo_grpc.MessageSourceStub(self._channel)

    def _subscribe_and_acknowledge(self):
        yield proximo.ConsumerRequest(
            startRequest=proximo.StartConsumeRequest(
                topic=self._topic,
                consumer=self._consumer,
            )
        )

        while True:
            self._confirm_semaphore.acquire()
            if self._confirm_id is None:
                break

            yield proximo.ConsumerRequest(confirmation=proximo.Confirmation(
                msgID=self._confirm_id,
            ))
            self._confirm_id = None
            self._read_semaphore.release()

    def acknowledge(self, message):
        """"Acknowledges the provided message. This method should only be called
        when reading messages using the consume method."""
        if self._auto_acknowledge:
            raise AutomaticAcknowledgmentException

        self._read_semaphore.acquire()
        self._confirm_id = message.id
        self._confirm_semaphore.release()

    def consume(self):
        """"This is a generator that consumes messages from the stream.
        Yields:
            message: message consumed from the stream
        """
        self._connect()
        if self._stream is not None:
            raise AlreadyStartedException

        self._stream = self._stub.Consume(self._subscribe_and_acknowledge())
        for message in self._stream:
            yield message

    def consume_with_callback(self, callback):
        """"Consumes messages from the stream then invokes the provided callback function
        and finally automatically acknowledges the message after the callback returns.
        Calling the acknowledge function in the callback will result in an exception.

        Args:
            callback function(message): callback function invoked for every consumed message
        """
        self._auto_acknowledge = True

        for message in self.consume():
            self._read_semaphore.acquire()
            callback(message)

            self._confirm_id = message.id
            self._confirm_semaphore.release()
