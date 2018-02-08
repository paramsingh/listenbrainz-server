#!/usr/bin/env python3


import sys
import os
import pika
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
import ujson
import logging
from listenbrainz.listen import Listen
from time import time, sleep
from listenbrainz.listenstore import InfluxListenStore
from listenbrainz.utils import escape, get_measurement_name, get_escaped_measurement_name, \
                               get_influx_query_timestamp, convert_to_unix_timestamp, \
                               convert_timestamp_to_influx_row_format
import listenbrainz.utils as utils
from requests.exceptions import ConnectionError
from redis import Redis
from collections import defaultdict

from listenbrainz.listen_writer import ListenWriter

class InfluxWriterSubscriber(ListenWriter):
    def __init__(self):
        super().__init__()

        self.ls = None
        self.influx = None
        self.incoming_ch = None
        self.unique_ch = None

    def add_listens_to_queue(self, listens):
        duplicate_count = 0
        unique_count = 0

        # Partition the listens on the basis of user names
        # and then store the time range for each user
        users = {}
        for listen in listens:

            t = int(listen['listened_at'])
            user_name = listen['user_name']

            if user_name not in users:
                users[user_name] = {
                    'min_time': t,
                    'max_time': t,
                    'listens': [listen],
                }
                continue

            if t > users[user_name]['max_time']:
                users[user_name]['max_time'] = t

            if t < users[user_name]['min_time']:
                users[user_name]['min_time'] = t

            users[user_name]['listens'].append(listen)

        # get listens in the time range for each user and
        # remove duplicates on the basis of timestamps
        for user_name in users:

            # get the range of time that we need to get from influx for
            # deduplication of listens
            min_time = users[user_name]['min_time']
            max_time = users[user_name]['max_time']

            query = """SELECT time, recording_msid
                         FROM %s
                        WHERE time >= %s
                          AND time <= %s
                    """ % (get_escaped_measurement_name(user_name), get_influx_query_timestamp(min_time), get_influx_query_timestamp(max_time))

            while True:
                try:
                    results = self.influx.query(query)
                    break
                except Exception as e:
                    self.log.error("Cannot query influx: %s" % str(e))
                    sleep(3)

            # collect all the timestamps for this given time range.

            timestamps = defaultdict(list) # dict of list of listens indexed by timestamp
            for result in results.get_points(measurement=get_measurement_name(user_name)):
                timestamps[convert_to_unix_timestamp(result['time'])].append(result)

            for listen in users[user_name]['listens']:
                # Check if a listen with the same timestamp and recording msid is already present in
                # Influx DB and if it is, mark current listen as duplicate
                t = int(listen['listened_at'])
                recording_msid = listen['recording_msid']
                dup = False

                if t in timestamps:
                    for row in timestamps[t]:
                        if row['recording_msid'] == recording_msid:
                            duplicate_count += 1
                            dup = True
                            break
                    else:
                        # if there are listens with the same timestamp but different
                        # metadata, we add a tag specifically for making sure that
                        # influxdb doesn't drop one of the listens. This value
                        # is monotonically increasing and defaults to 0
                        listen['dedup_tag'] = len(timestamps[t])

                if not dup:
                    unique_count += 1
                    self.data.append(listen)
                    timestamps[t].append({
                        'time': convert_timestamp_to_influx_row_format(t),
                        'recording_msid': recording_msid
                    })

        self.log.info("dups: %d, unique: %d" % (duplicate_count, unique_count))


    def insert_to_listenstore(self, data, retries=5):
        """
        Inserts a batch of listens to the ListenStore. If this fails, then breaks the data into
        two parts and recursively tries to insert them, until we find the culprit listen

        Args:
            data: the data to be inserted into the ListenStore
            retries: the number of retries to make before deciding that we've failed

        Returns: number of listens successfully sent
        """

        if not data:
            return 0

        failure_count = 0
        while True:
            try:
                self.ls.insert(data)
                return len(data)
            except (InfluxDBServerError, InfluxDBClientError, ValueError) as e:
                failure_count += 1
                if failure_count >= retries:
                    break
                sleep(ERROR_RETRY_DELAY)
            except ConnectionError as e:
                self.log.error("Cannot write data to listenstore: %s. Sleep." % str(e))
                sleep(ERROR_RETRY_DELAY)

        # if we get here, we failed on trying to write the data
        if len(data) == 1:
            # try to send the bad listen one more time and if it doesn't work
            # log the error
            try:
                self.ls.insert(data)
                return 1
            except (InfluxDBServerError, InfluxDBClientError, ValueError, ConnectionError) as e:
                self.log.error("Unable to insert bad listen to listenstore: %s" % str(e))
                if DUMP_JSON_WITH_ERRORS:
                    self.log.error("Was writing the following data:")
                    influx_dict = data[0].to_influx(get_measurement_name(data[0].user_name))
                    self.log.error(ujson.dumps(influx_dict))
                return 0
        else:
            slice_index = len(data) // 2
            # send first half
            sent = self.insert_to_listenstore(data[:slice_index], retries)
            # send second half
            sent += self.insert_to_listenstore(data[slice_index:], retries)
            return sent


    def write_data(self):

        t0 = time()
        submit = [Listen.from_json(x) for x in self.data]
        submitted_count = self.insert_to_listenstore(submit)
        self.time += time() - t0

        if not len(self.data):
            return True

        while True:
            try:
                self.unique_ch.basic_publish(
                    exchange=self.config.UNIQUE_EXCHANGE,
                    routing_key='',
                    body=ujson.dumps(self.data),
                    properties=pika.BasicProperties(delivery_mode=2,),
                )
                break
            except pika.exceptions.ConnectionClosed:
                self.connect_to_rabbitmq()

        self.ls.update_listen_counts()

        return True


    def start(self):
        self.log.info("influx-writer init")

        self._verify_hosts_in_config()

        if not hasattr(self.config, "INFLUX_HOST"):
            self.log.error("Influx service not defined. Sleeping {0} seconds and exiting.".format(self.ERROR_RETRY_DELAY))
            sleep(self.ERROR_RETRY_DELAY)
            sys.exit(-1)

        while True:
            try:
                self.ls = InfluxListenStore({ 'REDIS_HOST' : self.config.REDIS_HOST,
                                              'REDIS_PORT' : self.config.REDIS_PORT,
                                              'INFLUX_HOST': self.config.INFLUX_HOST,
                                              'INFLUX_PORT': self.config.INFLUX_PORT,
                                              'INFLUX_DB_NAME': self.config.INFLUX_DB_NAME})
                self.influx = InfluxDBClient(host=self.config.INFLUX_HOST, port=self.config.INFLUX_PORT, database=self.config.INFLUX_DB_NAME)
                break
            except Exception as err:
                self.log.error("Cannot connect to influx: %s. Retrying in 2 seconds and trying again." % str(err))
                sleep(ERROR_RETRY_DELAY)

        while True:
            try:
                self.redis = Redis(host=self.config.REDIS_HOST, port=self.config.REDIS_PORT, decode_responses=True)
                self.redis.ping()
                break
            except Exception as err:
                self.log.error("Cannot connect to redis: %s. Retrying in 2 seconds and trying again." % str(err))
                sleep(ERROR_RETRY_DELAY)

        while True:
            self.connect_to_rabbitmq()
            self.incoming_ch = self.connection.channel()
            self.incoming_ch.exchange_declare(exchange=self.config.INCOMING_EXCHANGE, exchange_type='fanout')
            self.incoming_ch.queue_declare(self.config.INCOMING_QUEUE, durable=True)
            self.incoming_ch.queue_bind(exchange=self.config.INCOMING_EXCHANGE, queue=self.config.INCOMING_QUEUE)
            self.incoming_ch.basic_consume(
                lambda ch, method, properties, body: self.static_callback(ch, method, properties, body, obj=self),
                queue=self.config.INCOMING_QUEUE,
            )

            self.unique_ch = self.connection.channel()
            self.unique_ch.exchange_declare(exchange=self.config.UNIQUE_EXCHANGE, exchange_type='fanout')

            self.log.info("influx-writer started")
            try:
                self.incoming_ch.start_consuming()
            except pika.exceptions.ConnectionClosed:
                self.log.info("Connection to rabbitmq closed. Re-opening.")
                self.connection = None
                continue

            self.connection.close()


    def print_and_log_error(self, msg):
        self.log.error(msg)
        print(msg, file = sys.stderr)


if __name__ == "__main__":
    rc = InfluxWriterSubscriber()
    rc.start()
