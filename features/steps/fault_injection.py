from behave import step, then

from fault_injector import FAULT_TYPES
from features.steps.patroni_api import do_get


@step("I inject fault '{fault_name:w}' into {patroni_name:w}")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}) into {patroni_name:w}")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d},"
      " end_after={end_after:d}) into {patroni_name:w}")
def activate_fault_point(context, patroni_name,
                         fault_name, fault_type=FAULT_TYPES.EXCEPTION, start_from=1, end_after=0):
    proc = context.pctl.get_proc_by_name(patroni_name)
    proc.activate_fault_point(fault_name, fault_type, start_from, end_after)


@step("I inject fault '{fault_name:w}' (type=sleep, sleep_time={sleep_time:d}) into {patroni_name:w}")
@step("I inject fault '{fault_name:w}' (type=sleep, start_from={start_from:d}, end_after={end_after:d},"
      " sleep_time={sleep_time:d}) into {patroni_name:w}")
def activate_fault_point_sleep(context, patroni_name,
                               fault_name, sleep_time, start_from=1, end_after=0):
    proc = context.pctl.get_proc_by_name(patroni_name)
    proc.activate_fault_point(fault_name, FAULT_TYPES.SLEEP, start_from, end_after, sleep_time)


@step("I deactivate fault point '{fault_name:w}' in {patroni_name:w}")
def deactivate_fault_point(context, fault_name, patroni_name):
    proc = context.pctl.get_proc_by_name(patroni_name)
    proc.deactivate_fault_point(fault_name)


@step("I deactivate all fault points in {patroni_name:w}")
def reset_fault_injector(context, patroni_name):
    proc = context.pctl.get_proc_by_name(patroni_name)
    proc.reset_fault_injector()


@step("I deactivate all fault points")
def reset_all_fault_injectors(context):
    context.pctl.reset_all_fault_injectors()


@then("fault point '{fault_name:w}' is activated in {patroni_name:w}")
def check_fault_activated(context, patroni_name, fault_name):
    proc = context.pctl.get_proc_by_name(patroni_name)

    do_get(context, proc._restapi_url + '/inject_fault')
    assert context.status_code == 200, 'Fault injection check request failed with code {context.status_code}'

    set_point = next((i for i in context.response if i['name'] == fault_name), None)
    assert set_point, 'Set fault point is not present in the response'


@then("fault point '{fault_name:w}' is not activated in {patroni_name:w}")
def check_fault_deactivated(context, patroni_name, fault_name):
    proc = context.pctl.get_proc_by_name(patroni_name)

    do_get(context, proc._restapi_url + '/inject_fault')
    assert context.status_code == 200, 'Fault injection check request failed with code {context.status_code}'

    set_point = next((i for i in context.response if i['name'] == fault_name), None)
    assert not set_point, 'Set fault point is present in the response'


@then("there are no activated fault points in {patroni_name:w}")
def check_fault_injector_reset(context, patroni_name):
    proc = context.pctl.get_proc_by_name(patroni_name)

    do_get(context, proc._restapi_url + '/inject_fault')
    assert context.status_code == 200, 'Fault injection check request failed with code {context.status_code}'

    assert len(context.response) == 0, 'Fault injector was not reset'
