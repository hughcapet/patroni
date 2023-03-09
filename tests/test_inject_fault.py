import unittest

from mock import patch

from fault_injector import FaultInjector, FAULT_TYPES


class TestFaultInjector(unittest.TestCase):

    def setUp(self):
        self.fi = FaultInjector()

    def test_set_fault_point(self):
        with self.assertRaises(TypeError) as context:
            self.fi.set_fault_point('inject_wrong_type', 'wrong_type')
        self.assertEqual(context.exception.args[0], 'Invalid fault point type')

        with patch('fault_injector.logger.info') as mock_logger_info:
            self.fi.set_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
            self.assertEqual(('Activated fault point %s of type %s', 'inject_exception', FAULT_TYPES.EXCEPTION),
                             mock_logger_info.call_args[0])

        with self.assertRaises(ValueError) as context:
            self.fi.set_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.assertEqual(context.exception.args[0], 'Fault point %s is already set', 'inject_exception')

        self.fi.remove_fault_point('inject_exception')

    def test_remove_fault_point(self):
        self.fi.set_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)

        with patch('fault_injector.logger.info') as mock_logger_info:
            self.fi.remove_fault_point('inject_exception')
            mock_logger_info.assert_called_once()
            self.assertEqual(('Deactivated fault point %s of type %s', 'inject_exception', FAULT_TYPES.EXCEPTION),
                             mock_logger_info.call_args[0])

            mock_logger_info.reset_mock()

            self.assertIsNone(self.fi.remove_fault_point('non_existent_fault_point'))
            mock_logger_info.assert_not_called()

    def test_inject_fault_if_set(self):
        # Type EXCEPTION

        self.fi.set_fault_point('inject_exception', FAULT_TYPES.EXCEPTION)
        self.assertRaises(Exception, self.fi.inject_fault_if_set, 'inject_exception')

        self.fi.remove_fault_point('inject_exception')
        self.assertIsNone(self.fi.inject_fault_if_set('inject_exception'))

        # Type SLEEP

        with self.assertRaises(ValueError) as context:
            self.fi.set_fault_point('inject_sleep', FAULT_TYPES.SLEEP)
        self.assertEqual(context.exception.args[0], 'No sleep_time provided for fault point of type sleep')

        with patch('time.sleep') as sleep_mock:
            self.fi.set_fault_point('inject_sleep', FAULT_TYPES.SLEEP, sleep_time=10)
            self.fi.inject_fault_if_set('inject_sleep')
            sleep_mock.assert_called_once_with(10)
            self.fi.remove_fault_point('inject_sleep')

        # Temp fault points

        start_from = 3
        end_after = 4
        self.fi.set_fault_point('inject_exception', FAULT_TYPES.EXCEPTION, start_from, end_after)
        for i in range(1, end_after+2):
            if i >= start_from and i <= end_after:
                self.assertRaises(Exception, self.fi.inject_fault_if_set, 'inject_exception')
            else:
                self.assertIsNone(self.fi.inject_fault_if_set('inject_exception'))
