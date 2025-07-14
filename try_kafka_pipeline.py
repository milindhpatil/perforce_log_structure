```python
from confluent_kafka import Producer, Consumer, KafkaError
from confluence_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import json
import time
import logging
import clickhouse_connect
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
import ksql
from-thread import Thread

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka and ClickHouse configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = 'default'
CLICKHOUSE_TABLE = 'kafka_data'
INPUT_TOPIC = 'input_topic'
OUTPUT_TOPIC = 'output_topic'

def create_kafka_topics():
    """Create Kafka topics if they don't exist."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    topics = [
        NewTopic(INPUT_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(OUTPUT_TOPIC, num_partitions=1, replication_factor=1)
    ]
    
    fs = admin_client.create_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Topic {topic} created")
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")

def create_clickhouse_table():
    """Create ClickHouse table if it doesn't exist."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE
    )
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        id String,
        text String,
        timestamp DateTime
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    
    client.command(create_table_query)
    logger.info(f"ClickHouse table {CLICKHOUSE_TABLE} created or already exists")
    client.close()

class KafkaProducerThread:
    """Kafka Producer for sending messages."""
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'python-producer'
        })
        
    def delivery_report(self, err, msg):
        """Delivery callback for Kafka messages."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce(self, text):
        """Produce a message to Kafka."""
        message = {
            'id': str(time.time()),
            'text': text,
            'timestamp': int(time.time())
        }
        
        self.producer.produce(
            topic=INPUT_TOPIC,
            value=json.dumps(message),
            callback=self.delivery_report
        )
        self.producer.flush()

def create_kafka_stream():
    """Create Kafka Stream to process messages."""
    ksql_client = ksql.KSQLAPI('http://localhost:8088')
    
    # Create stream
    create_stream_query = f"""
    CREATE STREAM IF NOT EXISTS input_stream (
        id STRING,
        text STRING,
        timestamp BIGINT
    ) WITH (
        KAFKA_TOPIC='{INPUT_TOPIC}',
        VALUE_FORMAT='JSON'
    );
    """
    
    # Create processed stream
    create_processed_stream_query = f"""
    CREATE STREAM IF NOT EXISTS output_stream
    AS SELECT
        id,
        UPPER(text) AS text,
        timestamp
    FROM input_stream
    EMIT CHANGES;
    """
    
    try:
        ksql_client.ksql(create_stream_query)
        ksql_client.ksql(create_processed_stream_query)
        logger.info("Kafka Streams created successfully")
    except Exception as e:
        logger.error(f"Failed to create Kafka Streams: {e}")

def configure_kafka_connect():
    """Configure Kafka Connect ClickHouse Sink."""
    connector_config = {
        "name": "clickhouse-sink",
        "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
        "tasks.max": "1",
        "topics": OUTPUT_TOPIC,
        "clickhouse.url": f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}",
        "clickhouse.database": CLICKHOUSE_DATABASE,
        "clickhouse.table": CLICKHOUSE_TABLE,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
    
    # This would typically be sent to Kafka Connect REST API
    logger.info("Kafka Connect configuration:")
    logger.info(json.dumps(connector_config, indent=2))

def main():
    """Main function to run the Kafka pipeline."""
    create_kafka_topics()
    create_clickhouse_table()
    create_kafka_stream()
    configure_kafka_connect()
    
    producer = KafkaProducerThread()
    
    try:
        for i in range(10):
            message = f"Test message {i}"
            producer.produce(message)
            logger.info(f"Produced: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    except Exception as e:
        logger.error(f"Error in producer: {e}")

if __name__ == "__main__":
    main()
```
