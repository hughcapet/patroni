import json

from behave import step

from features.steps.patroni_api import do_request
from fault_injector import FAULT_TYPES


@step("I inject fault '{fault_name:w}'")
@step("I inject fault '{fault_name:w}' (type={fault_type:w})")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d}, end_after={end_after:d})")
def set_fault_injector(context, fault_name, fault_type=FAULT_TYPES.EXCEPTION, start_from='1', end_after='0'):
    data = {'fault_name': fault_name,
            'fault_type': fault_type,
            'start_from': start_from,
            'end_after': end_after}
    do_request(context, 'POST', 'http://127.0.0.1:8008/fault_point', json.dumps(data))


@step("I inject fault '{fault_name:w}' (type=sleep, sleep_time={sleep_time:d})")
@step("I inject fault '{fault_name:w}' (type=sleep, start_from={start_from:d}, end_after={end_after:d}, \
      sleep_time={sleep_time:d})")
def set_fault_injector_sleep(context, fault_name, sleep_time, start_from='1', end_after='0'):
    data = {'fault_name': fault_name,
            'fault_type': FAULT_TYPES.SLEEP,
            'start_from': start_from,
            'end_after': end_after,
            'sleep_time': sleep_time}
    do_request(context, 'POST', 'http://127.0.0.1:8008/fault_point', json.dumps(data))
