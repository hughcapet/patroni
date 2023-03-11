import logging
import time

from copy import deepcopy
from enum import Enum
from threading import RLock
from typing import Optional, List

logger = logging.getLogger(__name__)


class FAULT_TYPES(str, Enum):
    EXCEPTION = 'exception'
    SLEEP = 'string'

    def __repr__(self):
        return self.value


class FaultPoint(object):
    # TODO: class docstring
    def __init__(self, name: str,
                 fault_type: FAULT_TYPES,
                 start_from: Optional[int] = 1,
                 end_after: Optional[int] = None,
                 sleep_time: Optional[float] = None) -> None:
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
        self.fi_lock = RLock()
        self._fault_points = []  # activated points

    def _get_fault_point_by_name(self, fault_name: str) -> FaultPoint:
        """Caller must hold fi_lock"""
        fault_point = None
        for fp in self._fault_points:
            if fp.name == fault_name:
                fault_point = fp
                break
        return fault_point

    def get_fault_points(self) -> List[dict]:
        points = []

        with self.fi_lock:
            for p in self._fault_points:
                points.append({
                    'fault_name': p.name,
                    'fault_type': p.type,
                    'start_from': p.start_from,
                    'end_after': p.end_after,
                    'sleep_time': p.sleep_time,
                    'hits': p.hits
                })
        return points

    def activate_fault_point(self,
                             fault_name: str,
                             fault_type: FAULT_TYPES,
                             start_from: Optional[int] = 1,
                             end_after: Optional[int] = None,
                             sleep_time: Optional[float] = None) -> None:
        """Every named fault point can be activated only with a single fault type.

        :param fault_name
        :param fault_type
        :param start_from: inject fault starting from the Nth hit
        :param end_after: stop fault injection after the Nth hit
        :param sleep_time: seconds to sleep if fault_type == FAULT_TYPES.SLEEP"""
        with self.fi_lock:
            for fp in self._fault_points:
                if fp.name == fault_name:
                    raise ValueError('Fault point %s is already set', fault_name)

            fault_point = FaultPoint(fault_name, fault_type, start_from, end_after, sleep_time)
            self._fault_points.append(fault_point)
            logger.info('Activated fault point %s of type %r', fault_name, fault_type)

    def deactivate_fault_point(self, fault_name: str) -> bool:
        with self.fi_lock:
            fault_point = self._get_fault_point_by_name(fault_name)

            if not fault_point:
                return False
            self._fault_points.remove(fault_point)
            logger.info('Deactivated fault point %s of type %r', fault_name, fault_point.type)
        return True

    def reset(self):
        """Deactivate all fault points"""
        with self.fi_lock:
            self._fault_points = []

    def inject_fault_if_activated(self, fault_name: str) -> None:
        """Function for defining a fault point in the source code.
        If fault point is activated (is in self._fault_points list), inject fault of the required type"""
        with self.fi_lock:
            fault_point = self._get_fault_point_by_name(fault_name)
            if not fault_point:
                return

            fault_point.hits += 1

            # check if it is already the time
            if fault_point.hits < fault_point.start_from:
                return
            # deactivate if work is done
            if fault_point.end_after and fault_point.hits > fault_point.end_after:
                self.deactivate_fault_point(fault_point.name)
                return

            current_fault_point = deepcopy(fault_point)

        logger.info('Fault %s of type %r triggered.', current_fault_point.name, current_fault_point.type)

        # do the actual work based on the fault type
        if current_fault_point.type == FAULT_TYPES.EXCEPTION:
            raise Exception('Exception raised by fault point {0}'.format(current_fault_point.name))
        elif current_fault_point.type == FAULT_TYPES.SLEEP:
            time.sleep(current_fault_point.sleep_time)
