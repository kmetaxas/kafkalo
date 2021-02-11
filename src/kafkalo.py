#!/usr/bin/env python3
from alladmin import AllAdmin
from inputparser import InputParser

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
    }
    sr_config = {
        "url": "http://localhost:8081",
    }
    mds_config = {
        "url": "http://localhost:8090",
    }

    admin = AllAdmin(kafka_config,
                     sr_config,
                     mds_config,
                     )

    parser = InputParser("data.yaml")
