import json

from behave import step

from features.steps.patroni_api import do_request, do_get
from fault_injector import FAULT_TYPES


def check_fault_injected(context, fault_name):
    assert context.status_code == 200
    do_get(context, 'http://127.0.0.1:8008/inject_fault')
    assert context.status_code == 200, 'Fault injection request failed'
    set_point = next(i for i in context.response if i['fault_name'] == fault_name)
    assert set_point, 'Set fault point is not present in the response'


@step("I inject fault '{fault_name:w}'")
@step("I inject fault '{fault_name:w}' (type={fault_type:w})")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d}, end_after={end_after:d})")
def activate_fault_point(context, fault_name, fault_type=FAULT_TYPES.EXCEPTION, start_from=1, end_after=0):
    data = {'fault_name': fault_name,
            'fault_type': fault_type,
            'start_from': start_from,
            'end_after': end_after}
    do_request(context, 'POST', 'http://127.0.0.1:8008/inject_fault', json.dumps(data))
    check_fault_injected(context, fault_name)


@step("I inject fault '{fault_name:w}' (type=sleep, sleep_time={sleep_time:d})")
@step("I inject fault '{fault_name:w}' (type=sleep, start_from={start_from:d}, end_after={end_after:d}, \
      sleep_time={sleep_time:d})")
def activate_fault_point_sleep(context, fault_name, sleep_time, start_from=1, end_after=0):
    data = {'fault_name': fault_name,
            'fault_type': FAULT_TYPES.SLEEP,
            'start_from': start_from,
            'end_after': end_after,
            'sleep_time': sleep_time}
    do_request(context, 'POST', 'http://127.0.0.1:8008/inject_fault', json.dumps(data))
    check_fault_injected(context, fault_name)


@step("I deactivate fault point '{fault_name:w}'")
def deactivate_fault_point(context, fault_name):
    data = {'fault_name': fault_name}
    do_request(context, 'DELETE', 'http://127.0.0.1:8008/inject_fault', json.dumps(data))


@step("I deactivate all fault points")
def reset_fault_injector(context):
    do_request(context, 'DELETE', 'http://127.0.0.1:8008/inject_fault', None)
