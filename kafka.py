from kafka import KafkaProducer
import time
import logging

# Configure Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def send_to_kafka(self, message):
    # Send progress update message to Kafka
    self.producer.send('progress_updates', message.encode())
    logging.info(f"Sent message to Kafka: {message.strip()}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Specify the log file path and Kafka topic
    log_file_path = 'path/to/your/log_file.log'
    kafka_topic = 'progress_updates'

    # Continuously monitor the log file and send progress updates to Kafka
    try:
        while True:
            send_to_kafka(log_file_path, kafka_topic)
            # Adjust the sleep time as needed
            time.sleep(5)  # Check every 5 seconds for updates
    except KeyboardInterrupt:
        logging.info("Kafka producer stopped.")
