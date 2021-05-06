"""

"""
import time
import requests
import json

from threading import Thread
from metaflow.sidecar_messages import MessageTypes, Message

HB_URL_KEY = 'hb_url'


class MetadataHeartBeat(object):

    def __init__(self):
        self.headers = {'content-type': 'application/json'}
        self.req_thread = Thread(target=self.ping)
        self.req_thread.daemon = True
        self.default_frequency_secs = 10
        self.hb_url = None

    def process_message(self, msg):
        # type: (Message) -> None
        if msg.msg_type == MessageTypes.SHUTDOWN:
            # todo shutdown doesnt do anything yet? should it still be called
            self.shutdown()
        if (not self.req_thread.is_alive()) and \
                msg.msg_type == MessageTypes.LOG_EVENT:
            # set post url
            self.hb_url = msg.payload[HB_URL_KEY]
            # start thread
            self.req_thread.start()

    def ping(self):
        retry_counter = 0
        while True:
            try:
                frequency_secs = self.heartbeat()

                if frequency_secs is None or frequency_secs <= 0:
                    frequency_secs = self.default_frequency_secs

                time.sleep(frequency_secs)
                retry_counter = 0
            except Exception:
                retry_counter = retry_counter + 1
                time.sleep(4**retry_counter)

    def heartbeat(self):
        if self.hb_url is not None:
            response = \
                requests.post(url=self.hb_url, data="{}", headers=self.headers)
            return json.loads(response.json()).get('wait_time_in_seconds')

    def shutdown(self):
        # attempts sending on last heartbeat
        self.heartbeat()
