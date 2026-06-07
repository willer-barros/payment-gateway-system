import os 
import json
import pika

class MessageBroker:
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@message-broker:5672//")
        self.exchange_name = "payment.v1.events"
        self.queue_name = "payment.v1.process-transaction"


    def pusblish_transaction_event(self, transaction_data: dict):
        parameters = pika.URLParameters(self.rabbitmq_url)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="direct",
            durable=True
        )

        channel.queue_declare(queue=self.queue_name, durable=True)

        channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_name,
            routing_key="transaction.created"
        )

        payload =  json.dumps(transaction_data)
        channel.basic_publish(
            exchange=self.exchange_name,
            routing_key="transaction.created",
            body=payload,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json"
            )
        )

        connection.close()