#!/usr/bin/env python

import logging
import os

logger = logging.getLogger(__name__)

PATRONI_FAULT_POINT = 'PATRONI_FAULT_POINT'
PATRONI_FAULT_TYPE = 'PATRONI_FAULT_TYPE'
PATRONI_FAULT_START_FROM = 'PATRONI_FAULT_START_FROM'
PATRONI_FAULT_END_AFTER = 'PATRONI_FAULT_END_AFTER'

PATRONI_FAULT_COUNTER = 0

FAULT_TYPES = (
    'exception',
)


def inject_fault(fault_point):
    global PATRONI_FAULT_COUNTER
    logger.info(PATRONI_FAULT_COUNTER)
    if PATRONI_FAULT_POINT in os.environ and fault_point == os.environ[PATRONI_FAULT_POINT]:
        PATRONI_FAULT_COUNTER += 1

        if PATRONI_FAULT_START_FROM in os.environ and int(os.environ[PATRONI_FAULT_START_FROM]) > PATRONI_FAULT_COUNTER:
            return
        if PATRONI_FAULT_END_AFTER in os.environ and int(os.environ[PATRONI_FAULT_END_AFTER]) < PATRONI_FAULT_COUNTER:
            return

        fault_type = os.environ[PATRONI_FAULT_TYPE] if PATRONI_FAULT_TYPE in os.environ else FAULT_TYPES[0]
        if fault_type == 'exception':
            logger.info("fault injector triggered")
            raise Exception('fault triggered, fault name:{0}, fault type:{1}'.format(fault_point, fault_type))
