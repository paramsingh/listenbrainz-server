import listenbrainz.utils as utils
import logging
import ujson


from time import sleep
from listenbrainz import default_config as config
try:
    from listenbrainz import custom_config as config
except ImportError:
    pass


SUBMIT_CHUNK_SIZE = 1000 # the number of listens to send to BQ in one batch

# NOTE: this MUST be greater than or equal to the maximum number of listens sent to us in one
# RabbitMQ batch, otherwise BigQueryWriter will submit a partial batch and send an ack for
# the batch.
assert(SUBMIT_CHUNK_SIZE >= 50)

PREFETCH_COUNT = 20    # the number of RabbitMQ batches to prefetch
FLUSH_LISTENS_TIME = 3 # the number of seconds to wait before flushing all listens in queue to BQ

class ListenWriter:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        logging.basicConfig()
        self.log.setLevel(logging.INFO)

        self.redis = None
        self.connection = None
        self.total_inserts = 0
        self.inserts = 0
        self.time = 0

        self.REPORT_FREQUENCY = 5000
        self.DUMP_JSON_WITH_ERRORS = False
        self.ERROR_RETRY_DELAY = 3 # number of seconds to wait until retrying an operation
        self.config = config

        self.data = []
        self.delivery_tags = []
        self.timer_id = None # keeps track of the timer added to flush listens


    def callback(self, ch, method, properties, body):

        # if some timeout exists, remove it as we'll add a new one
        # before this method exits
        if self.timer_id is not None:
            ch.connection.remove_timeout(self.timer_id)
            self.timer_id = None

        listens = ujson.loads(body)
        count = len(listens)

        # if adding this batch pushes us over the line, send this batch before
        # adding new listens to queue
        if len(self.data) + count > SUBMIT_CHUNK_SIZE:
            self.write_data()
            # XXX: think about moving reset to a function
            self.data = []
            self.delivery_tags = []


        self.add_listens_to_queue(listens)

        self.delivery_tags.append(method.delivery_tag)

        # if we won't get any new messages until we ack these, submit data
        if len(self.delivery_tags) == PREFETCH_COUNT:
            self.write_data()
            self.data = []
            self.delivery_tags = []

        # add a timeout that makes sure that the listens in the queue get submitted
        # after some time
        self.timer_id = ch.connection.add_timeout(FLUSH_LISTENS_TIME, self.write_data)
        return True


    def write_data(self):
        raise NotImplementedError


    def add_listens_to_queue(self, listens):
        raise NotImplementedError


    @staticmethod
    def static_callback(ch, method, properties, body, obj):
        return obj.callback(ch, method, properties, body)


    def connect_to_rabbitmq(self):
        connection_config = {
            'username': self.config.RABBITMQ_USERNAME,
            'password': self.config.RABBITMQ_PASSWORD,
            'host': self.config.RABBITMQ_HOST,
            'port': self.config.RABBITMQ_PORT,
            'virtual_host': self.config.RABBITMQ_VHOST
        }
        self.connection = utils.connect_to_rabbitmq(**connection_config,
                                                    error_logger=self.log.error,
                                                    error_retry_delay=self.ERROR_RETRY_DELAY)


    def _collect_and_log_stats(self, count, call_method=lambda: None):
        self.inserts += count
        if self.inserts >= self.REPORT_FREQUENCY:
            self.total_inserts += self.inserts
            if self.time > 0:
                self.log.info("Inserted %d rows in %.1fs (%.2f listens/sec). Total %d rows." % \
                    (self.inserts, self.time, count / self.time, self.total_inserts))
            self.inserts = 0
            self.time = 0

            call_method()


    def _verify_hosts_in_config(self):
        if not hasattr(self.config, "REDIS_HOST"):
            self.log.error("Redis service not defined. Sleeping {0} seconds and exiting.".format(self.ERROR_RETRY_DELAY))
            sleep(self.ERROR_RETRY_DELAY)
            sys.exit(-1)

        if not hasattr(self.config, "RABBITMQ_HOST"):
            self.log.error("RabbitMQ service not defined. Sleeping {0} seconds and exiting.".format(self.ERROR_RETRY_DELAY))
            sleep(self.ERROR_RETRY_DELAY)
            sys.exit(-1)
