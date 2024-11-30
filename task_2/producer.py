#!/usr/bin/env python
import pika
import random
import time
import threading

def create_task(time_to_sleep_max, routing_key):
    def task():
        counter = 0
        while True:
            time_to_sleep = random.randint(1000, time_to_sleep_max) / 1000.0  
            time.sleep(time_to_sleep)

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost')
            )
            channel = connection.channel()

            channel.exchange_declare(exchange='payment_logs', exchange_type='direct')

            message = f"Message type [{routing_key}] from publisher N {counter}"

            channel.basic_publish(
                exchange='payment_logs',
                routing_key=routing_key,
                body=message
            )

            print(f"Message type [{routing_key}] is sent into Direct Exchange [N:{counter}]")
            counter += 1

            connection.close()
    return task

def main():
    # Create and start threads for each routing key with different sleep times
    threading.Thread(target=create_task(12000, "success"), daemon=True).start()
    threading.Thread(target=create_task(10000, "failure"), daemon=True).start()
    threading.Thread(target=create_task(8000, "pending"), daemon=True).start()

    # Keep the main thread alive
    print("Press Ctrl+C to exit...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == '__main__':
    main()
