#!/usr/bin/env python
import pika

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='payment_logs', exchange_type='direct')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='payment_logs', queue=queue_name, routing_key='success')

    print(f"Subscribed to the queue '{queue_name}'")

    def callback(ch, method, properties, body):
        print(f"Received message: {body.decode('utf-8')}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print("Waiting for messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Exiting...")
        connection.close()

if __name__ == '__main__':
    main()