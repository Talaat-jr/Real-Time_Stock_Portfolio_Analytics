from kafka import KafkaConsumer
import json
import pandas as pd
import os
import logging
from encoding_utils import encode_single_record

logger = logging.getLogger(__name__)


def create_consumer(topic_name, bootstrap_servers='kafka:9092', group_id='stock_consumer_group'):
    """
    Create and configure Kafka consumer.
    
    Args:
        topic_name: Kafka topic to subscribe to
        bootstrap_servers: Kafka broker address
        group_id: Consumer group ID
        
    Returns:
        KafkaConsumer: Configured consumer instance
    """
    logger.info(f"Creating Kafka consumer for topic: {topic_name}")
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',  # Start from beginning if no offset
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # consumer_timeout_ms=10000  # Timeout after 10 seconds of inactivity
    )
    
    logger.info("Kafka consumer created successfully")
    return consumer


def process_stream(record, lookup_tables):
    """
    Process a single streamed record: encode and prepare for storage.
    
    Args:
        record: Dictionary representing one row from stream
        lookup_tables: Dictionary of encoding lookup tables
        
    Returns:
        dict: Processed and encoded record
    """
    try:
        # Encode the record using lookup tables
        encoded_record = encode_single_record(record, lookup_tables)
        
        return encoded_record
        
    except Exception as e:
        logger.error(f"Error processing stream record: {e}")
        return None


def consume_and_process(consumer, lookup_tables, output_file='output/FULL_STOCKS.csv'):
    """
    Consume messages from Kafka, process them, and append to output file.
    
    Args:
        consumer: KafkaConsumer instance
        lookup_tables: Dictionary of encoding lookup tables
        output_file: Path to append processed records
        
    Returns:
        int: Number of records processed
    """
    logger.info("Starting to consume and process messages...")
    
    processed_count = 0
    records_to_append = []
    
    try:
        for message in consumer:
            record = message.value
            
            # Check for End of Stream message
            if record == "EOS" or (isinstance(record, dict) and record.get('message') == 'EOS'):
                logger.info("Received End of Stream (EOS) message. Stopping consumer.")
                break
            
            # Process the record
            processed_record = process_stream(record, lookup_tables)
            
            if processed_record:
                records_to_append.append(processed_record)
                processed_count += 1
                
                if processed_count % 10 == 0:
                    logger.info(f"Processed {processed_count} records from stream")
        
        # Append all processed records to the output file
        if records_to_append:
            logger.info(f"Appending {len(records_to_append)} records to {output_file}")
            
            # Load existing data
            if os.path.exists(output_file):
                existing_df = pd.read_csv(output_file)
            else:
                existing_df = pd.DataFrame()
            
            # Create DataFrame from new records
            new_df = pd.DataFrame(records_to_append)
            
            # Append and save
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df.to_csv(output_file, index=False)
            
            logger.info(f"Successfully appended {len(records_to_append)} records")
    
    except Exception as e:
        logger.error(f"Error in consume_and_process: {e}")
    
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total records processed: {processed_count}")
    
    return processed_count


def wait_for_kafka(bootstrap_servers='kafka:9092', max_retries=30, retry_interval=2):
    """
    Wait for Kafka to be ready.
    
    Args:
        bootstrap_servers: Kafka broker address
        max_retries: Maximum connection attempts
        retry_interval: Seconds between retries
        
    Returns:
        bool: True if Kafka is ready
    """
    import time
    from kafka.errors import NoBrokersAvailable
    
    logger.info("Waiting for Kafka to be ready...")
    
    for attempt in range(max_retries):
        try:
            # Try to create a simple consumer to test connection
            test_consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=1000
            )
            test_consumer.close()
            logger.info("Kafka is ready!")
            return True
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    logger.error("Failed to connect to Kafka")
    return False
