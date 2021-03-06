""" This script should be used to replay user listens to fix bad data.
"""
import abc
import json
import uuid

from datetime import datetime
from flask import current_app
from listenbrainz.utils import get_measurement_name, quote, convert_to_unix_timestamp
from listenbrainz.webserver import create_app
from listenbrainz.webserver.influx_connection import init_influx_connection
from listenbrainz.listenstore.influx_listenstore import DUMP_CHUNK_SIZE


class UserReplayer(abc.ABC):
    def __init__(self, user_name):
        self.user_name = user_name
        self.max_time = datetime.now()
        self.app = create_app()


    @abc.abstractmethod
    def filter(self, row):
        """ Modify row as needed by the subclass

        Returns:
            row (dict): modified row as needed
                or None if row needs to be removed
        """
        pass


    def convert_to_influx_insert_format(self, row, measurement):
        data = {
            'measurement': measurement,
            'time': convert_to_unix_timestamp(row['time']),
        }

        data['fields'] = row
        data['fields'].pop('time')

        try:
            dedup_tag = data['fields'].pop('dedup_tag')
            data['tags'] = {'dedup_tag': dedup_tag}
        except KeyError:
            pass # no dedup tag, don't need to do anything

        return data


    def copy_measurement(self, src, dest, apply_filter=False):
        done = False
        offset = 0
        while True:
            result = self.ls.get_listens_batch_for_dump(src, self.max_time, offset)
            rows = []
            count = 0
            for row in result.get_points(get_measurement_name(src)):
                count += 1
                if apply_filter:
                    row = self.filter_function(row)
                if row:
                    rows.append(self.convert_to_influx_insert_format(row, quote(dest)))
            self.ls.write_points_to_db(rows)
            offset += DUMP_CHUNK_SIZE
            if count == 0:
                break


    def start(self):
        with self.app.app_context():
            current_app.logger.info("Connecting to Influx...")
            self.ls = init_influx_connection(current_app.logger, {
                'REDIS_HOST': current_app.config['REDIS_HOST'],
                'REDIS_PORT': current_app.config['REDIS_PORT'],
                'REDIS_NAMESPACE': current_app.config['REDIS_NAMESPACE'],
                'INFLUX_HOST': current_app.config['INFLUX_HOST'],
                'INFLUX_PORT': current_app.config['INFLUX_PORT'],
                'INFLUX_DB_NAME': current_app.config['INFLUX_DB_NAME'],
            })
            current_app.logger.info("Done!")

            new_measurement_name = str(uuid.uuid4())
            current_app.logger.info("Temporary destination measurement: %s", new_measurement_name)

            current_app.logger.info("Copying listens from %s to temporary measurement...", self.user_name)
            self.copy_measurement(src=self.user_name, dest=new_measurement_name, apply_filter=True)
            current_app.logger.info("Done!")


            current_app.logger.info("Removing user measurement...")
            self.ls.delete(self.user_name)
            current_app.logger.info("Done!")

            current_app.logger.info("Copying listens back from temporary measurement to %s...", self.user_name)
            self.copy_measurement(src=new_measurement_name, dest=self.user_name, apply_filter=False)
            current_app.logger.info("Done!")

            current_app.logger.info("Removing temporary measurement...")
            self.ls.delete(new_measurement_name)
            current_app.logger.info("Done!")
