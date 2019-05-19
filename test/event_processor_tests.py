import json
import os
import shutil
import unittest

from parameterized import parameterized

from pynopticon.aggregator import AverageAggregator, MedianAggregator, MinAggregator, MaxAggregator
from pynopticon.entry_point import EventProcessor


class EventProcessorAverageTestCase(unittest.TestCase):
    RESULT_DIR = os.path.join(os.getcwd(), ".test_results")
    EXPECTED_CHALLENGE_EXAMPLE = """{"date": "2018-12-26 18:11:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "average_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "average_delivery_time": 20.0}
{"date": "2018-12-26 18:14:00", "average_delivery_time": 20.0}
{"date": "2018-12-26 18:15:00", "average_delivery_time": 20.0}
{"date": "2018-12-26 18:16:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:17:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:18:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:19:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:20:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:21:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:22:00", "average_delivery_time": 31.0}
{"date": "2018-12-26 18:23:00", "average_delivery_time": 31.0}
{"date": "2018-12-26 18:24:00", "average_delivery_time": 42.5}"""
    EXPECTED_EMPTY_INPUT = ""
    EXPECTED_SPARSE_EVENTS = """{"date": "2018-12-26 18:11:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "average_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:14:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:15:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:16:00", "average_delivery_time": 31.0}
{"date": "2018-12-26 18:17:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:18:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:19:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:20:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:21:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:22:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:23:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:24:00", "average_delivery_time": 54.0}"""
    EXPECTED_DENSE_EVENTS = """{"date": "2018-12-26 18:11:00", "average_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "average_delivery_time": 51.0}"""

    def setUp(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

        os.mkdir(self.RESULT_DIR)

    def tearDown(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

    @parameterized.expand([
        ('provided_example', 'input1.json', 'output1.json', 10, EXPECTED_CHALLENGE_EXAMPLE),
        ('empty_input', 'empty.json', 'empty_output.json', 11, EXPECTED_EMPTY_INPUT),
        ('sparse_events', 'input1.json', 'sparse.json', 1, EXPECTED_SPARSE_EVENTS),
        ('dense_events', 'dense_events.json', 'dense.json', 3, EXPECTED_DENSE_EVENTS)
    ])
    def test_parameterized(self, name, input_file, output_file, window_size, expected_output):
        input_file_path = os.path.join(os.getcwd(), "test", "test_inputs", input_file)
        output_file_path = os.path.join(os.getcwd(), self.RESULT_DIR, output_file)
        aggregators = [AverageAggregator(window_size)]
        with EventProcessor(input_file_path, aggregators, output_file_path) as e:
            e.execute()

        parsed_expected = [json.loads(line) for line in expected_output.split("\n") if expected_output != ""]
        with open(output_file_path, 'r') as result_file:
            for index, line in enumerate(result_file):
                parsed_actual = json.loads(line)
                self.assertDictEqual(parsed_expected[index], parsed_actual)


class EventProcessorMedianTestCase(unittest.TestCase):
    RESULT_DIR = os.path.join(os.getcwd(), ".test_results")
    EXPECTED_CHALLENGE_EXAMPLE = """{"date": "2018-12-26 18:11:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "median_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "median_delivery_time": 20.0}
{"date": "2018-12-26 18:14:00", "median_delivery_time": 20.0}
{"date": "2018-12-26 18:15:00", "median_delivery_time": 20.0}
{"date": "2018-12-26 18:16:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:17:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:18:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:19:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:20:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:21:00", "median_delivery_time": 25.5}
{"date": "2018-12-26 18:22:00", "median_delivery_time": 31.0}
{"date": "2018-12-26 18:23:00", "median_delivery_time": 31.0}
{"date": "2018-12-26 18:24:00", "median_delivery_time": 42.5}"""
    EXPECTED_EMPTY_INPUT = ""
    EXPECTED_SPARSE_EVENTS = """{"date": "2018-12-26 18:11:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "median_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:14:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:15:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:16:00", "median_delivery_time": 31.0}
{"date": "2018-12-26 18:17:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:18:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:19:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:20:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:21:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:22:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:23:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:24:00", "median_delivery_time": 54.0}"""
    EXPECTED_DENSE_EVENTS = """{"date": "2018-12-26 18:11:00", "median_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "median_delivery_time": 56.0}"""

    def setUp(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

        os.mkdir(self.RESULT_DIR)

    def tearDown(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

    @parameterized.expand([
        ('provided_example', 'input1.json', 'output1.json', 10, EXPECTED_CHALLENGE_EXAMPLE),
        ('empty_input', 'empty.json', 'empty_output.json', 11, EXPECTED_EMPTY_INPUT),
        ('sparse_events', 'input1.json', 'sparse.json', 1, EXPECTED_SPARSE_EVENTS),
        ('dense_events', 'dense_events.json', 'dense.json', 3, EXPECTED_DENSE_EVENTS)
    ])
    def test_parameterized(self, name, input_file, output_file, window_size, expected_output):
        input_file_path = os.path.join(os.getcwd(), "test", "test_inputs", input_file)
        output_file_path = os.path.join(os.getcwd(), self.RESULT_DIR, output_file)
        aggregators = [MedianAggregator(window_size)]
        with EventProcessor(input_file_path, aggregators, output_file_path) as e:
            e.execute()

        parsed_expected = [json.loads(line) for line in expected_output.split("\n") if expected_output != ""]
        with open(output_file_path, 'r') as result_file:
            for index, line in enumerate(result_file):
                parsed_actual = json.loads(line)
                self.assertDictEqual(parsed_expected[index], parsed_actual)


class EventProcessorMinTestCase(unittest.TestCase):
    RESULT_DIR = os.path.join(os.getcwd(), ".test_results")
    EXPECTED_CHALLENGE_EXAMPLE = """{"date": "2018-12-26 18:11:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:14:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:15:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:16:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:17:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:18:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:19:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:20:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:21:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:22:00", "minimum_delivery_time": 31.0}
{"date": "2018-12-26 18:23:00", "minimum_delivery_time": 31.0}
{"date": "2018-12-26 18:24:00", "minimum_delivery_time": 31.0}"""
    EXPECTED_EMPTY_INPUT = ""
    EXPECTED_SPARSE_EVENTS = """{"date": "2018-12-26 18:11:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "minimum_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:14:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:15:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:16:00", "minimum_delivery_time": 31.0}
{"date": "2018-12-26 18:17:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:18:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:19:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:20:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:21:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:22:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:23:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:24:00", "minimum_delivery_time": 54.0}"""
    EXPECTED_DENSE_EVENTS = """{"date": "2018-12-26 18:11:00", "minimum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "minimum_delivery_time": 15.0}"""

    def setUp(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

        os.mkdir(self.RESULT_DIR)

    def tearDown(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

    @parameterized.expand([
        ('provided_example', 'input1.json', 'output1.json', 10, EXPECTED_CHALLENGE_EXAMPLE),
        ('empty_input', 'empty.json', 'empty_output.json', 11, EXPECTED_EMPTY_INPUT),
        ('sparse_events', 'input1.json', 'sparse.json', 1, EXPECTED_SPARSE_EVENTS),
        ('dense_events', 'dense_events.json', 'dense.json', 3, EXPECTED_DENSE_EVENTS)
    ])
    def test_parameterized(self, name, input_file, output_file, window_size, expected_output):
        input_file_path = os.path.join(os.getcwd(), "test", "test_inputs", input_file)
        output_file_path = os.path.join(os.getcwd(), self.RESULT_DIR, output_file)
        aggregators = [MinAggregator(window_size)]
        with EventProcessor(input_file_path, aggregators, output_file_path) as e:
            e.execute()

        parsed_expected = [json.loads(line) for line in expected_output.split("\n") if expected_output != ""]
        with open(output_file_path, 'r') as result_file:
            for index, line in enumerate(result_file):
                parsed_actual = json.loads(line)
                self.assertDictEqual(parsed_expected[index], parsed_actual)


class EventProcessorMaxTestCase(unittest.TestCase):
    RESULT_DIR = os.path.join(os.getcwd(), ".test_results")
    EXPECTED_CHALLENGE_EXAMPLE = """{"date": "2018-12-26 18:11:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "maximum_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "maximum_delivery_time": 20.0}
{"date": "2018-12-26 18:14:00", "maximum_delivery_time": 20.0}
{"date": "2018-12-26 18:15:00", "maximum_delivery_time": 20.0}
{"date": "2018-12-26 18:16:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:17:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:18:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:19:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:20:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:21:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:22:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:23:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:24:00", "maximum_delivery_time": 54.0}"""
    EXPECTED_EMPTY_INPUT = ""
    EXPECTED_SPARSE_EVENTS = """{"date": "2018-12-26 18:11:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "maximum_delivery_time": 20.0}
{"date": "2018-12-26 18:13:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:14:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:15:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:16:00", "maximum_delivery_time": 31.0}
{"date": "2018-12-26 18:17:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:18:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:19:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:20:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:21:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:22:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:23:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:24:00", "maximum_delivery_time": 54.0}"""
    EXPECTED_DENSE_EVENTS = """{"date": "2018-12-26 18:11:00", "maximum_delivery_time": 0.0}
{"date": "2018-12-26 18:12:00", "maximum_delivery_time": 82.0}"""

    def setUp(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

        os.mkdir(self.RESULT_DIR)

    def tearDown(self):
        if os.path.isdir(self.RESULT_DIR):
            shutil.rmtree(self.RESULT_DIR)

    @parameterized.expand([
        ('provided_example', 'input1.json', 'output1.json', 10, EXPECTED_CHALLENGE_EXAMPLE),
        ('empty_input', 'empty.json', 'empty_output.json', 11, EXPECTED_EMPTY_INPUT),
        ('sparse_events', 'input1.json', 'sparse.json', 1, EXPECTED_SPARSE_EVENTS),
        ('dense_events', 'dense_events.json', 'dense.json', 3, EXPECTED_DENSE_EVENTS)
    ])
    def test_parameterized(self, name, input_file, output_file, window_size, expected_output):
        input_file_path = os.path.join(os.getcwd(), "test", "test_inputs", input_file)
        output_file_path = os.path.join(os.getcwd(), self.RESULT_DIR, output_file)
        aggregators = [MaxAggregator(window_size)]
        with EventProcessor(input_file_path, aggregators, output_file_path) as e:
            e.execute()

        parsed_expected = [json.loads(line) for line in expected_output.split("\n") if expected_output != ""]
        with open(output_file_path, 'r') as result_file:
            for index, line in enumerate(result_file):
                parsed_actual = json.loads(line)
                self.assertDictEqual(parsed_expected[index], parsed_actual)
