# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import signal
import sys
import config
import os

from pathlib import Path

from types import FrameType

from flask import Flask, request

from google.cloud.pubsublite.cloudpubsub import PublisherClient

from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)

from utils.logging import logger

from threading import Thread

app = Flask(__name__)

#Uses auth.json for authentication
auth_file = Path("auth.json")
if auth_file.is_file():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(auth_file)

@app.route("/", methods=['POST'])
def webhook() -> str:
    data = request.get_json()

    logger.info(request)

    logger.info(data)

    #Process the lead in a thread if the event_type is new_lead
    event_type = data['event_type']
    if 'event_type' in data and event_type in config.EVENT_TYPES:
        Thread(target=processLead, args=(data,)).start()
    else:
        logger.warn(f'Found event type: {event_type} : {str(data)}')

    #Return a blank response if api_partner was not passed
    if not 'api_partner' in data:
        logger.warn(f'No api_partner field found: {str(data)}')
        return ''

    ret = data['api_partner']
    ret.pop('name', None)
    return ret

#Processes the roadster lead create pub/sub message
def processLead(data):
    try:
        location = CloudZone(CloudRegion(config.CLOUD_REGION), config.ZONE_ID)
        topic_path = TopicPath(config.PROJECT_ID, location, config.TOPIC_ID)
        with PublisherClient() as publisher:
            encoded_data = str(data).encode("utf-8")
            future = publisher.publish(topic_path, encoded_data)
    except Exception as e:
        logger.error(e)



def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="localhost", port=8080, debug=True)

else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)
