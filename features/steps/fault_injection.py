from behave import step

from features.steps.patroni_api import do_get
from fault_injector import FAULT_TYPES


def check_fault_injected(context, fault_name):
    do_get(context, 'http://127.0.0.1:8010/inject_fault')
    assert context.status_code == 200, 'Fault injection check request failed'
    set_point = next(i for i in context.response if i['fault_name'] == fault_name)
    assert set_point, 'Set fault point is not present in the response'


@step("I inject fault '{fault_name:w}' into {patroni_name:w}")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}) into {patroni_name:w}")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d}, \
      end_after={end_after:d}) into {patroni_name:w}")
def activate_fault_point(context, patroni_name,
                         fault_name, fault_type=FAULT_TYPES.EXCEPTION, start_from=1, end_after=0):
    for name, proc in context.pctl._processes.items():
        if name == patroni_name:
            proc.activate_fault_point(fault_name, fault_type, start_from, end_after)
            break
    else:
        assert False, F'Could not find {patroni_name} process'
    check_fault_injected(context, fault_name)


@step("I inject fault '{fault_name:w}' (type=sleep, sleep_time={sleep_time:d}) into Patroni on {address}")
@step("I inject fault '{fault_name:w}' (type=sleep, start_from={start_from:d}, end_after={end_after:d}, \
      sleep_time={sleep_time:d}) into Patroni on {address:w}")
def activate_fault_point_sleep(context, patroni_name,
                               fault_name, sleep_time, start_from=1, end_after=0):
    for p in context.pctl._processes:
        if p == patroni_name:
            p.activate_fault_point(fault_name, FAULT_TYPES.SLEEP, start_from, end_after, sleep_time)
            break
    else:
        assert False, F'Could not find {patroni_name} process'


@step("I deactivate fault point '{fault_name:w}' in {patroni_name:w}")
def deactivate_fault_point(context, fault_name, patroni_name):
    for p in context.pctl._processes:
        if p == patroni_name:
            p.deactivate_fault_point(fault_name)
            break
    else:
        assert False, F'Could not find {patroni_name} process'


@step("I deactivate all fault points in {patroni_name:w}")
def reset_fault_injector(context, patroni_name):
    for p in context.pctl._processes:
        if p == patroni_name:
            p.reset_fault_injector()
            break
    else:
        assert False, F'Could not find {patroni_name} process'


def reset_all_fault_injectors(context):
    context.pctl.reset_all_fault_injectors()
