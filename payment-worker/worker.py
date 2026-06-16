import asyncio
import json
import os
import sys
from aio_pika import connect_robust, Message, ExchangeType

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@message-broker:5672//")

async def process_payment(message):
    async with message.process(requeue=False):
        payload = json.loads(message.body.decode())
        print(f"[Worker] Received transaction: {payload['transaction_id']} for amount ${payload['amount']}")
        
        if payload["amount"] > 100:
            print(f"[Gateway Error] Temporary failure processing transaction {payload['transaction_id']}.")
            raise Exception("Gateway communication timeout")
            
        print(f"✅ [Success] Transaction {payload['transaction_id']} processed successfully!")

async def main():
    connection = await connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    
    main_exchange = await channel.declare_exchange("payment.v1.events", ExchangeType.DIRECT, durable=True)
    retry_exchange = await channel.declare_exchange("payment.v1.retry", ExchangeType.DIRECT, durable=True)
    dlq_exchange = await channel.declare_exchange("payment.v1.dlx", ExchangeType.DIRECT, durable=True)

    dlq_queue = await channel.declare_queue("payment.v1.dead-letter", durable=True)
    await dlq_queue.bind(dlq_exchange, routing_key="transaction.failed")

    retry_queue = await channel.declare_queue(
        "payment.v1.retry-5s",
        durable=True,
        arguments={
            "x-message-ttl": 5000, # Tempo em milissegundos que a mensagem fica aguardando
            "x-dead-letter-exchange": "payment.v1.events", 
            "x-dead-letter-routing-key": "transaction.created"
        }
    )
    await retry_queue.bind(retry_exchange, routing_key="transaction.retry")

    main_queue = await channel.declare_queue(
        "payment.v1.process-transaction",
        durable=True,
        arguments={
            "x-dead-letter-exchange": "payment.v1.retry",
            "x-dead-letter-routing-key": "transaction.retry"
        }
    )
    await main_queue.bind(main_exchange, routing_key="transaction.created")

    print("🚀 [Worker] Async Payment Worker is up and routing mechanisms configured. Waiting for messages...")

    await main_queue.consume(process_payment)

    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped.")
        sys.exit(0)