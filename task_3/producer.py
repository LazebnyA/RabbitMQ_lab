import pika
import random
import time

languages = ["English", "Spanish", "French", "Ukrainian"]

def generate_routing_key():
    """
    Generate a routing key randomly.
    """
    return f"language.{random.choice(languages)}"

def main():
    """
    Main function to publish messages to the RabbitMQ topic exchange.
    """
    counter = 0
    connection_params = pika.ConnectionParameters('localhost')
    
    while True:
        time_to_sleep = random.randint(1, 2)
        time.sleep(time_to_sleep)
        
        with pika.BlockingConnection(connection_params) as connection:
            channel = connection.channel()
            
            channel.exchange_declare(exchange="topic_logs", exchange_type="topic")
            
            routing_key = generate_routing_key()
            message = f"Message type [{routing_key}] from publisher N {counter}"
            
            channel.basic_publish(
                exchange="topic_logs",
                routing_key=routing_key,
                body=message.encode('utf-8')
            )
            
            print(f"Message type [{routing_key}] is sent into Topic Exchange [N:{counter}]")
            counter += 1

if __name__ == "__main__":
    main()
