"""
This module include receive sync testing by generating local mock of receive state to core B
"""
import os
from sync_tester.functions import executors
from sync_tester.configuration import config

layer_source_path = config.SYNC_SOURCE_LAYER_PATH_B
layer_name = config.SYNC_SOURCE_NAME_B
layer_receive_source_path = os.path.join(layer_source_path, layer_name)

res = executors.create_new_receive_source(source=layer_receive_source_path)
layer_name = res["layer_name"]
layer_full_path = res["layer_destination"]

res = executors.create_receive_sync(os.path.dirname(layer_full_path), layer_name, image_format='png', file_receive_api=config.FILE_RECEIVER_API_B)