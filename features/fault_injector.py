#!/usr/bin/env python
from behave import step


@step("I inject fault '{fault_name:w}' (type={fault_type:w})")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d})")
@step("I inject fault '{fault_name:w}' (type={fault_type:w}, start_from={start_from:d}, end_after={end_after:d})")
def set_fault_injector(context, fault_name, fault_type, start_from='1', end_after='0'):
    env = dict()

    env['PATRONI_FAULT_POINT'] = fault_name
    env['PATRONI_FAULT_START_FROM'] = str(start_from)
    env['PATRONI_FAULT_TYPE'] = fault_type
    if int(end_after) > 0:
        env['PATRONI_FAULT_END_AFTER'] = str(end_after)

    context.pctl.set_environment(env)
