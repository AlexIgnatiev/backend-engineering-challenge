import sys
from contextlib import ContextDecorator

import json
import datetime
import math

from pynopticon.util import timestamp_floor


class Event:
    """
    Python representation of events parsed from the input
    """
    def __init__(self, timestamp, duration, word_count):
        self.timestamp = timestamp
        self.duration = duration
        self.word_count = word_count

    @classmethod
    def parse_from_json(cls, json_string, time_format):
        """
        Parses an input line into `Event` object. Assumes all input lines have correct structure and there are no empty
        lines between two input lines. Throws `EOFError` if an empty line is found
        :param json_string: string of the event to parse
        :param time_format: python time format string
        :return: returns parsed `Event` object
        """
        if json_string == "":
            raise EOFError()
        e = json.loads(json_string)
        timestamp = e.get('timestamp')
        if timestamp is not None:
            timestamp = datetime.datetime.strptime(timestamp, time_format)
        e['timestamp'] = timestamp

        return cls(timestamp, e['duration'], e['nr_words'])


class EventProcessorError(Exception):
    pass


class EventProcessor(ContextDecorator):
    """
    Context manager used to process the input into the expected output. This class is responsible for correctly keeping
    and closing the input and output files, it is, therefore not recommended to try using it outside of a context
    creation, i.e. python's `with` statement.
     """
    INPUT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    OUTPUT_TIME_FORMAT = "%Y-%m-%d %H:%M:00"

    def __init__(self, ifile, aggregators, ofile=None):
        """
        Event Processor constructor
        :param ifile: input file path
        :param aggregators: list of aggregators to use in the output
        :param ofile: output file path. If set to `None`, stdout will be used as default.
        """
        self._input_filename = ifile
        self._output_file = ofile

        self.aggregators = aggregators

        self._initialized = False

    def __enter__(self):
        self.input_file_handler = open(self._input_filename, 'r')

        self.output_file_handler = open(self._output_file, 'w') if self._output_file else sys.stdout
        self._initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.input_file_handler.close()
        self.output_file_handler.close()
        return exc_type is EOFError

    def execute(self):
        """
        executes the event processing
        Raises `EventProcessorError` if the instance wasn't initialized as a context manager
        :return: None
        """
        if not self._initialized:
            raise EventProcessorError(
                "{0} must be used as context manager within a with statement".format(self.__class__.__name__))

        first_event = Event.parse_from_json(self.input_file_handler.readline(), self.INPUT_TIME_FORMAT)
        self._init_output_file(first_event)
        next_event = None
        for line in self.input_file_handler:
            next_event = Event.parse_from_json(line, self.INPUT_TIME_FORMAT)
            self._process_next(next_event)
        if next_event is not None:
            self._process_next(next_event, is_final=True)

    def _process_next(self, next_event, is_final=False):
        timespan_minutes = math.floor((next_event.timestamp - self.aggregators[0].last_event.timestamp).seconds / 60)
        end_range = 2 if is_final else 1
        for i in range(1, timespan_minutes + end_range):
            for agg in self.aggregators:
                agg.shift_window()

            aggregated_dict = {x.name: x.aggregate() for x in self.aggregators}
            output_dict = {
                "date": self.aggregators[0].window_lower_bound.strftime(self.OUTPUT_TIME_FORMAT),
            }
            output_dict.update(aggregated_dict)
            self.output_file_handler.write("{0}\n".format(json.dumps(output_dict)))
        if not is_final:
            for agg in self.aggregators:
                agg.add_event(next_event)

    def _init_output_file(self, first_event):
        ts = timestamp_floor(first_event.timestamp)
        for agg in self.aggregators:
            agg.window_lower_bound = ts
        aggregated_dict = {x.name: x.aggregate() for x in self.aggregators}
        output_dict = {
            "date": self.aggregators[0].window_lower_bound.strftime(self.OUTPUT_TIME_FORMAT),
        }
        output_dict.update(aggregated_dict)
        for agg in self.aggregators:
            agg.add_event(first_event)

        self.output_file_handler.write("{0}\n".format(json.dumps(output_dict)))
