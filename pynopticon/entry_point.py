from pynopticon.aggregator import MedianAggregator, AverageAggregator, MaxAggregator, MinAggregator
from pynopticon.event_processor import EventProcessor


def run(parsed_args):
    parsed_args.aggregator = set(parsed_args.aggregator)
    aggregators = []
    for agg in parsed_args.aggregator:
        if agg == 'median':
            aggregators.append(MedianAggregator(parsed_args.window_size, use_word_count=parsed_args.use_word_count))
        elif agg == 'max':
            aggregators.append(MaxAggregator(parsed_args.window_size, use_word_count=parsed_args.use_word_count))
        elif agg == 'min':
            aggregators.append(MinAggregator(parsed_args.window_size, use_word_count=parsed_args.use_word_count))
        else:
            aggregators.append(AverageAggregator(parsed_args.window_size, use_word_count=parsed_args.use_word_count))

    with EventProcessor(parsed_args.input_file, aggregators, ofile=parsed_args.output_file) as processor:
        processor.execute()
