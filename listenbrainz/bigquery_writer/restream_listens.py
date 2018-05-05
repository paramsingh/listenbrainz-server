""" This script takes all listens in the ListenBrainz influx database and
submits them to Google BigQuery.
"""

# listenbrainz-server - Server for the ListenBrainz project
#
# Copyright (C) 2018 MetaBrainz Foundation Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import json
import listenbrainz.config as config
import listenbrainz.db as db
import listenbrainz.db.user as db_user
import listenbrainz.utils as utils
import os
import time


from datetime import datetime
from googleapiclient.errors import HttpError
from influxdb import InfluxDBClient
from listenbrainz.bigquery import create_bigquery_object
from listenbrainz.bigquery_writer.bigquery_writer import BigQueryWriter
from listenbrainz.listen import Listen

CHUNK_SIZE = 1000

influx = None
bigquery = None


def init_influx_connection():
    """ Initializes the connection to the Influx database.
    """
    global influx
    influx = InfluxDBClient(
        host=config.INFLUX_HOST,
        port=config.INFLUX_PORT,
        database=config.INFLUX_DB_NAME,
    )


def init_bigquery_connection():
    """ Initiates the connection to Google BigQuery.
    """
    global bigquery
    bigquery = create_bigquery_object()


def convert_to_influx_timestamp(year):
    """ Returns the timestamp for the first second of the specified year in
    the format that influx accepts.
    """
    ts = datetime(year, 1, 1, 0, 0, 0).strftime('%s')
    return utils.get_influx_query_timestamp(ts)


def get_listens_batch(user_name, start_year, end_year, offset=0):
    """ Get a batch of listens for the specified user.

    Args:
        user_name (str): the MusicBrainz ID of the user
        start_year (int): the first year in the time range for which listens are to be returned
        end_year (int): the last year in the time range (exclusive)
        offset (int): the offset used to identify which batch of listens to return

    Returns:
        a list of listens in ListenBrainz API format
    """
    condition = 'time >= %s AND time < %s' % (convert_to_influx_timestamp(start_year), convert_to_influx_timestamp(end_year))
    query = """SELECT *
                 FROM {measurement_name}
                WHERE {condition}
             ORDER BY time
                LIMIT {limit}
               OFFSET {offset}
            """.format(
                measurement_name=utils.get_escaped_measurement_name(user_name),
                condition=condition,
                limit=CHUNK_SIZE,
                offset=offset,
            )

    while True:
        try:
            result = influx.query(query)
            break
        except Exception as e:
            print('Error while getting listens from influx: %s' % str(e))
            time.sleep(3)

    listens = []
    for row in result.get_points(utils.get_measurement_name(user_name)):
        listen = Listen.from_influx(row).to_api()
        listen['user_name'] = user_name
        listens.append(listen)

    return listens


def push_to_bigquery(listens, table_name):
    """ Push a list of listens to the specified table in Google BigQuery

    Args:
        listens: the list of listens to be pushed to Google BigQuery
        table_name: the name of the table in which listens are to be inserted

    Returns:
        int: the number of listens pushed to Google BigQuery
    """
    payload = {
        'rows': BigQueryWriter().convert_to_bigquery_payload(listens)
    }
    while True:
        try:
            ret = bigquery.tabledata().insertAll(
                projectId=config.BIGQUERY_PROJECT_ID,
                datasetId=config.BIGQUERY_DATASET_ID,
                tableId=table_name,
                body=payload).execute(num_retries=5)
            break
        except HttpError as e:
            print('Submit to BigQuery failed: %s, retrying in 3 seconds.' % str(e))
        except Exception as e:
            print('Unknown exception on submit to BigQuery failed: %s. Retrying in 3 seconds.' % str(e))
        time.sleep(3)

    return len(listens)


def push_listens(user_name, table_name, start_year, end_year):
    """ Push all listens in a particular time range to the specified table in Google BigQuery

    Args:
        user_name (str): the MusicBrainz ID of the user
        table_name (str): the name of the table into which listens are to be inserted
        start_year (int): the beginning year of the time range (inclusive)
        end_year (int): the end year of the time range (exclusive)

    Returns:
        int: the number of listens pushed to Google BigQuery
    """
    offset = 0
    count = 0
    job_start_time = time.time()
    t0 = job_start_time
    while True:
        listens = get_listens_batch(
            user_name,
            start_year=start_year,
            end_year=end_year,
            offset=offset,
        )
        if not listens:
            break
        count += push_to_bigquery(listens, table_name)
        offset += CHUNK_SIZE
        if time.time() - t0 > 5:
            print('User %s - table %s: %d listens done ' % (user_name, table_name, count))
            t0 = time.time()

    if count > 0:
        print('Total of %d listens pushed for user %s in table %s in %.2f secs (%.2f listens/sec)' % (
            count, user_name, table_name, time.time() - job_start_time, count / (time.time() - job_start_time)))
    return count


def main():
    db.init_db_connection(config.SQLALCHEMY_DATABASE_URI)
    init_influx_connection()
    init_bigquery_connection()
    users = db_user.get_all_users()
    listen_count = 0
    user_count = 0
    for user in users:
        print('Begin pushing listens for user %s...' % user['musicbrainz_id'])
        listen_count += push_listens(user['musicbrainz_id'], 'before_2002', start_year=1970, end_year=2002)
        for year in range(2002, 2019):
            listen_count += push_listens(user['musicbrainz_id'], table_name=str(year), start_year=year, end_year=year+1)
        user_count += 1
        print('Listens of %d users pushed, %d users remain...' % (user_count, len(users) - user_count))
        print('%d listens pushed to Google BigQuery!' % listen_count)
    print('A total of %d listens restreamed to Google BigQuery' % listen_count)


if __name__ == '__main__':
    main()
