import unittest

from mock import patch

from fault_injector import FaultInjector, FAULT_TYPES


class TestFaultInjector(unittest.TestCase):

    def setUp(self):
        self.fi = FaultInjector()

    def test_activate_fault_point(self):
        with patch('fault_injector.logger.info') as mock_logger_info:
            self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
            self.assertEqual(('Activated fault point %s of type %r', 'inject_exception', FAULT_TYPES.EXCEPTION),
                             mock_logger_info.call_args[0])

        with self.assertRaises(ValueError) as context:
            self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.assertEqual(context.exception.args[0], 'Fault point %s is already set', 'inject_exception')

        self.fi.deactivate_fault_point('inject_exception')

    def test_deactivate_fault_point(self):
        self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)

        with patch('fault_injector.logger.info') as mock_logger_info:
            self.assertTrue(self.fi.deactivate_fault_point('inject_exception'))
            mock_logger_info.assert_called_once()
            self.assertEqual(('Deactivated fault point %s of type %r', 'inject_exception', FAULT_TYPES.EXCEPTION),
                             mock_logger_info.call_args[0])

            mock_logger_info.reset_mock()

            self.assertFalse(self.fi.deactivate_fault_point('non_existent_fault_point'))
            mock_logger_info.assert_not_called()

    def test_get_fault_points(self):
        self.assertEqual([], self.fi.get_fault_points())

        self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.assertIn({
                'name': 'inject_exception',
                'fault_type': FAULT_TYPES.EXCEPTION,
                'start_from': 1,
                'end_after': None,
                'hits': 0
            }, self.fi.get_fault_points())

        self.fi.activate_fault_point('inject_sleep', FAULT_TYPES.SLEEP, sleep_time=42)
        self.assertIn({
                'name': 'inject_sleep',
                'fault_type': FAULT_TYPES.SLEEP,
                'start_from': 1,
                'end_after': None,
                'sleep_time': 42,
                'hits': 0
            }, self.fi.get_fault_points())

    def test_reset(self):
        self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.fi.activate_fault_point('inject_exception_2', FAULT_TYPES.EXCEPTION)
        self.fi.reset()
        self.assertEqual(self.fi.get_fault_points(), [])

    def test_inject_fault_if_activated(self):
        # Type EXCEPTION

        self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.assertRaises(Exception, self.fi.inject_fault_if_activated, 'inject_exception')

        self.fi.deactivate_fault_point('inject_exception')
        self.assertIsNone(self.fi.inject_fault_if_activated('inject_exception'))

        # Type SLEEP

        with self.assertRaises(ValueError) as context:
            self.fi.activate_fault_point('inject_sleep', FAULT_TYPES.SLEEP)
        self.assertEqual(context.exception.args[0], 'No sleep_time provided for fault point of type sleep')

        with patch('time.sleep') as sleep_mock:
            self.fi.activate_fault_point('inject_sleep', FAULT_TYPES.SLEEP, sleep_time=10)
            self.fi.inject_fault_if_activated('inject_sleep')
            sleep_mock.assert_called_once_with(10)
            self.fi.deactivate_fault_point('inject_sleep')

        # Temp fault points

        start_from = 3
        end_after = 4
        self.fi.activate_fault_point('inject_exception', FAULT_TYPES.EXCEPTION, start_from, end_after)
        for i in range(1, end_after+2):
            if i >= start_from and i <= end_after:
                self.assertRaises(Exception, self.fi.inject_fault_if_activated, 'inject_exception')
            else:
                self.assertIsNone(self.fi.inject_fault_if_activated('inject_exception'))
