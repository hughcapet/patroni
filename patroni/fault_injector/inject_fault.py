#!/usr/bin/env python

import logging
import time

from enum import Enum

logger = logging.getLogger(__name__)


class FAULT_TYPES(Enum):
    EXCEPTION = 1
    SLEEP = 2


class FaultPoint(object):
    # TODO: class docstring
    def __init__(self, name: str,
                 fault_type: FAULT_TYPES,
                 start_from: int = 1,
                 end_after: int = None,
                 sleep_time: float = None) -> None:
        if not isinstance(fault_type, FAULT_TYPES):
            raise TypeError('Invalid fault point type')
        if fault_type == FAULT_TYPES.SLEEP and not sleep_time:
            raise ValueError('No sleep_time provided for fault point of type sleep')

        self.name = name
        self.type = fault_type
        self.start_from = start_from
        self.end_after = end_after
        self.sleep_time = sleep_time  # only for type SLEEP

        self.hits = 0


class FaultInjector(object):
    # TODO: class docstring
    def __init__(self) -> None:
        self._fault_points = []  # activated points

    def set_fault_point(self,
                        fault_name: str,
                        fault_type: FAULT_TYPES,
                        start_from: int = 1,
                        end_after: int = None,
                        sleep_time: float = None) -> None:
        """Activate fault point (called by API).
        Every named fault point can be activated only with a single fault type."""
        for fp in self._fault_points:
            if fp.name == fault_name:
                raise ValueError('Fault point %s is already set', fault_name)

        fault_point = FaultPoint(fault_name, fault_type, start_from, end_after, sleep_time)
        self._fault_points.append(fault_point)
        logger.info('Activated fault point %s of type %s', fault_name, fault_type)

    def _get_fault_point_by_name(self, fault_name: str) -> FaultPoint:
        fault_point = None
        for fp in self._fault_points:
            if fp.name == fault_name:
                fault_point = fp
                break
        return fault_point

    def remove_fault_point(self, fault_name: str) -> None:
        """Deactivate fault point"""
        fault_point = self._get_fault_point_by_name(fault_name)
        if not fault_point:
            return
        self._fault_points.remove(fault_point)
        logger.info('Deactivated fault point %s of type %s', fault_name, fault_point.type)

    def inject_fault_if_set(self, fault_name: str) -> None:
        # inject fault only if the required point has been activated
        fault_point = self._get_fault_point_by_name(fault_name)
        if not fault_point:
            return

        fault_point.hits += 1

        # check if it is already the time
        if fault_point.hits < fault_point.start_from:
            return
        # deactivate if work is done
        if fault_point.end_after and fault_point.hits > fault_point.end_after:
            self.remove_fault_point(fault_point.name)
            return

        logger.info('Fault %s of type %s triggered.', fault_point.name, fault_point.type)

        # do the actual work based on the fault type
        if fault_point.type == FAULT_TYPES.EXCEPTION:
            raise Exception('Exception raised by fault point {0}'.format(fault_point.name))
        elif fault_point.type == FAULT_TYPES.SLEEP:
            time.sleep(fault_point.sleep_time)
