import datetime
from collections import deque
from statistics import median


class BaseAggregator:
    """
    Base class for all aggregators
    """
    def __init__(self, use_word_count=False):
        """
        Base constructor
        :param use_word_count: if set to `True` aggregator will be using `word_count` attribute of the events to
        calculate the value that will be used in the output
        """
        self.current_events = deque([])
        self.use_word_count = use_word_count

    def aggregate(self):
        """
        :return: current aggregation value
        """
        return 0.0

    def add_event(self, event):
        """
        Adds event to the list of events that should be considered in aggregation
        :param event: `Event` object
        :return: None
        """
        self.current_events.append(event)

    def get_event_value(self, event):
        """
        Calculates and returns the value that represents the contribution of the `event` in aggregation calculation
        if `use_word_count` is set to False this simply returns the `event.duration`, otherwise it returns an average
        that represents how much time it took to translate one word of that event.
        :param event: `Event` object
        :return: `event`'s contribution to the aggregate value
        """
        if self.use_word_count:
            return event.duration if event.word_count == 0 else event.duration / event.word_count
        else:
            return event.duration

    @property
    def last_event(self):
        """
        :return: `Event` object with the most recent timestamp
        """
        return self.current_events[-1]


class MovingAggregator(BaseAggregator):
    """
    Base class representing an aggregation function that has a moving time window
    """

    def __init__(self, window_size, **kwargs):
        """
        Base constructor
        :param window_size: size of the window, in minutes, that should be considered for aggregation calculation
        :param kwargs: arguments passed to the super constructor
        """
        super().__init__(**kwargs)
        self.window_size = window_size
        self.window_lower_bound = None

    def shift_window(self):
        """
        shifts the time window by 1 unit.
        :return: None
        """
        self.window_lower_bound += datetime.timedelta(seconds=60)
        while self._queue_needs_pop():
            e = self.current_events[0]
            self.current_events.popleft()
            self._pop_update_method(e)

    def _queue_needs_pop(self):
        if len(self.current_events) == 0:
            return False
        last_event_ts_delta = self.current_events[0].timestamp + datetime.timedelta(seconds=60 * self.window_size)
        return last_event_ts_delta < self.window_lower_bound

    def _pop_update_method(self, event):
        pass


class AverageAggregator(MovingAggregator):
    """
    Moving average aggregation function
    """
    name = "average_delivery_time"

    def __init__(self, window_size, **kwargs):
        super().__init__(window_size, **kwargs)
        self.current_sum = 0.0

    def aggregate(self):
        return self.current_sum / len(self.current_events) if len(self.current_events) > 0 else 0.0

    def add_event(self, event):
        super().add_event(event)
        self.current_sum += self.get_event_value(event)

    def _pop_update_method(self, event):
        self.current_sum -= self.get_event_value(event)


class MedianAggregator(MovingAggregator):
    """
    Moving median aggregation function
    """

    name = "median_delivery_time"

    def aggregate(self):
        if len(self.current_events) == 0:
            return 0.0
        else:
            return float(median([self.get_event_value(x) for x in self.current_events]))


class MaxAggregator(MovingAggregator):
    """
    Moving maximum aggregation function
    """

    name = "maximum_delivery_time"

    def aggregate(self):
        if len(self.current_events) == 0:
            return 0.0
        else:
            return float(max([self.get_event_value(x) for x in self.current_events]))


class MinAggregator(MovingAggregator):
    """
    Moving minimum aggregation function
    """

    name = "minimum_delivery_time"

    def aggregate(self):
        if len(self.current_events) == 0:
            return 0.0
        else:
            return float(min([self.get_event_value(x) for x in self.current_events]))
