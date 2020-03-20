"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        super().__init__(
            topic_name="org.chicago.cta.turnstile.v1", # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.key_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        # produce entry through turnstile event to kafka
        for entry in range(num_entries):
            key={"timestamp": self.time_millis()}
            value={
                       "station_id": self.station.station_id,
                       "station_name": self.station.name,
                       "line": self.station.color.name,
                   }
            logger.info(json.dumps(key))
            logger.info(json.dumps(value))
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key_schema=self.key_schema, 
                    value_schema=self.value_schema,                   
                    key={"timestamp": self.time_millis()},
                    value={
                       "station_id": self.station.station_id,
                       "station_name": self.station.name,
                       "line": self.station.color.name,
                    }
                )
                logger.info(f"Succeeded: {self.station.station_id}, {self.station.name}, {self.station.color.name}")
            except Exception as e: 
                # logger.info("Turnstile failed to write to topic {} with exception {}.".format(self.topic_name, e))
                logger.info(f"Failed: {self.station.station_id}, {self.station.name}, {self.station.color.name}")



