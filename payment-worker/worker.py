import asyncio
import json
import os
import sys
from aio_pika import connect_robust, Message, ExchangeType

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
MAX_RETRIES = 3

def get_retry_count(message) -> int:
    """Extrai a quantidade de vezes que a mensagem já passou por uma DLX."""
    headers = message.headers
    if headers and "x-death" in headers:
        return headers["x-death"][0].get("count", 0)
    return 0

async def process_payment(message, channel):
    try:
        payload = json.loads(message.body.decode())
        transaction_id = payload.get("transaction_id")
        amount = payload.get("amount")
        
        print(f"\n[Worker] Received transaction: {transaction_id} for amount ${amount}")
        
        retry_count = get_retry_count(message)
        if retry_count >= MAX_RETRIES:
            print(f"🚨 [DLQ] Transaction {transaction_id} exceeded max retries ({retry_count}). Sending to DLQ.")
            
            dlq_exchange = await channel.get_exchange("payment.v1.dlx")
            await dlq_exchange.publish(
                Message(
                    body=message.body,
                    content_type="application/json",
                    headers={"reason": "max_retries_exceeded", "original_errors": retry_count}
                ),
                routing_key="transaction.failed"
            )
            await message.ack()
            
            
            return

        if amount > 100:
            print(f"⚠️ [Gateway Error] Temporary failure for transaction {transaction_id}. (Retry count: {retry_count + 1}/{MAX_RETRIES})")
            raise Exception("Gateway communication timeout")
            
        print(f"✅ [Success] Transaction {transaction_id} processed successfully!")
        await message.ack()
        
        # TODO: Config para o banco

    except Exception as e:
        if "Gateway communication timeout" in str(e):
            await message.nack(requeue=False)
        else:
            print(f"💥 [Critical Error] Unhandled exception: {e}. Dropping message to prevent loop.")
            await message.nack(requeue=False)

async def main():
    connection = await connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    
    await channel.set_qos(prefetch_count=1)
    
    main_exchange = await channel.declare_exchange("payment.v1.events", ExchangeType.DIRECT, durable=True)
    retry_exchange = await channel.declare_exchange("payment.v1.retry", ExchangeType.DIRECT, durable=True)
    dlq_exchange = await channel.declare_exchange("payment.v1.dlx", ExchangeType.DIRECT, durable=True)

    dlq_queue = await channel.declare_queue("payment.v1.dead-letter", durable=True)
    await dlq_queue.bind(dlq_exchange, routing_key="transaction.failed")

    retry_queue = await channel.declare_queue(
        "payment.v1.retry-5s",
        durable=True,
        arguments={
            "x-message-ttl": 5000, 
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

    await main_queue.consume(lambda msg: process_payment(msg, channel))

    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped.")
        sys.exit(0)