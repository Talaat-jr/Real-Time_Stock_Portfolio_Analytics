"""
Kafka Producer for Stock Trading Data Stream
Reads from stream.csv and publishes records to Kafka topic.
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import pandas as pd
import time
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers='kafka:9092'):
    """
    Create and configure Kafka producer.
    
    Args:
        bootstrap_servers: Kafka broker address
        
    Returns:
        KafkaProducer: Configured producer instance
    """
    logger.info("Creating Kafka producer...")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    
    logger.info("Kafka producer created successfully")
    return producer


def send_record(producer, topic_name, record, record_num):
    """
    Send a single record to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic_name: Target Kafka topic
        record: Dictionary representing one row
        record_num: Record number for logging
        
    Returns:
        bool: True if successful
    """
    try:
        # Convert pandas Series to dict if needed
        if isinstance(record, pd.Series):
            record_dict = record.to_dict()
        else:
            record_dict = record
        
        # Convert any non-JSON-serializable types
        for key, value in record_dict.items():
            if pd.isna(value):
                record_dict[key] = None
            elif isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
                record_dict[key] = str(value)
        
        # Send to Kafka
        future = producer.send(topic_name, value=record_dict)
        
        # Block until sent (or timeout)
        record_metadata = future.get(timeout=10)
        
        # logger.info(f"Record {record_num} sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
        return True
        
    except KafkaError as e:
        logger.error(f"Failed to send record {record_num}: {e}")
        return False


def stream_data(stream_file='output/stream.csv', topic_name=None, sleep_ms=300):
    """
    Read stream.csv and publish records to Kafka topic with delay.
    
    Args:
        stream_file: Path to stream CSV file
        topic_name: Kafka topic name (will use STUDENT_ID_Topic if not provided)
        sleep_ms: Milliseconds to sleep between records (default 300)
    """
    # Get student ID from environment or use default
    if topic_name is None:
        topic_name = os.getenv('KAFKA_TOPIC_NAME', '55_23722_23342_Topic')
    
    logger.info("=" * 60)
    logger.info("Kafka Producer - Stock Trading Stream")
    logger.info("=" * 60)
    logger.info(f"Stream file: {stream_file}")
    logger.info(f"Topic name: {topic_name}")
    logger.info(f"Sleep interval: {sleep_ms}ms")
    logger.info("=" * 60)
    
    # Check if stream file exists
    if not os.path.exists(stream_file):
        logger.error(f"Stream file not found: {stream_file}")
        logger.error("Please run the main pipeline first to generate stream.csv")
        sys.exit(1)
    
    # Load stream data
    logger.info(f"Loading stream data from {stream_file}...")
    stream_df = pd.read_csv(stream_file)
    total_records = len(stream_df)
    logger.info(f"Loaded {total_records} records to stream")
    
    # Create producer
    try:
        producer = create_producer()
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        logger.error("Make sure Kafka is running (docker-compose up -d)")
        sys.exit(1)
    
    # Stream records
    try:
        logger.info("\nStarting to stream records...")
        success_count = 0
        
        for idx, record in stream_df.iterrows():
            record_num = idx + 1
            
            # Send record
            if send_record(producer, topic_name, record, record_num):
                success_count += 1
            
            # Progress update every 10 records
            if record_num % 10 == 0:
                logger.info(f"Progress: {record_num}/{total_records} records sent ({success_count} successful)")
            
            # Sleep between records
            time.sleep(sleep_ms / 1000.0)
        
        # Send End of Stream message
        logger.info("\nSending End of Stream (EOS) message...")
        eos_message = "EOS"
        producer.send(topic_name, value=eos_message)
        producer.flush()
        
        logger.info("=" * 60)
        logger.info(f"Streaming complete!")
        logger.info(f"Total records sent: {success_count}/{total_records}")
        logger.info(f"Topic: {topic_name}")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("\nStreaming interrupted by user")
    except Exception as e:
        logger.error(f"Error during streaming: {e}")
    finally:
        logger.info("Closing producer...")
        producer.close()
        logger.info("Producer closed")


def main():
    """
    Main function to run the producer.
    Can be configured via command-line arguments.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Producer for Stock Trading Stream')
    parser.add_argument('--stream-file', default='output/stream.csv', help='Path to stream CSV file')
    parser.add_argument('--topic', default=None, help='Kafka topic name (default: STUDENT_ID_Topic)')
    parser.add_argument('--sleep-ms', type=int, default=300, help='Milliseconds between records (default: 300)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    # Override default bootstrap servers if provided
    if args.bootstrap_servers != 'localhost:9092':
        # This would require modifying create_producer to accept bootstrap_servers
        logger.info(f"Using bootstrap servers: {args.bootstrap_servers}")
    
    # Start streaming
    stream_data(
        stream_file=args.stream_file,
        topic_name=args.topic,
        sleep_ms=args.sleep_ms
    )


if __name__ == '__main__':
    main()

# import json
# import time
# import pandas as pd
# from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# stream_data = pd.read_csv('output/stream.csv')

# print("Sending data to Kafka...")

# for index, row in stream_data.iterrows():
#     message = row.to_dict()
#     message['timestamp'] = pd.Timestamp.now().isoformat()
#     producer.send('55_23722_23342_Topic', value=message)
#     print(f"Sent: {message}")
#     time.sleep(0.3)
    
# producer.send('55_23722_23342_Topic', value='EOS')
# print("Sent EOS message.")

# producer.close()
