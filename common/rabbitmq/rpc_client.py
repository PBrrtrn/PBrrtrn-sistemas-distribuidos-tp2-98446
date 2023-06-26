import uuid
import pika


class RPCClient:
    def __init__(self, rpc_queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare('', exclusive=True)
        self.response_queue_name = result.method.queue

        self.channel.basic_consume(
            queue=self.response_queue_name,
            on_message_callback=self.on_response,
            auto_ack=False
        )

        self.rpc_queue_name = rpc_queue_name

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, message):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.rpc_queue_name,
            properties=pika.BasicProperties(
                reply_to=self.response_queue_name,
                correlation_id=self.corr_id
            ),
            body=message
        )

        self.connection.process_data_events(time_limit=None)
        return self.response
