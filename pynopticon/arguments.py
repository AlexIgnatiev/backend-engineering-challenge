import argparse


def parse_args():
    """
    Parses command line arguments and returns the result
    :return: `Namespace` object with a list of aggregators, input file name, window size and a boolean indicating if
    word cou t should be considered for aggregations
    """
    parser = argparse.ArgumentParser(description="Process translation events",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--input_file',
                        help="Path for the json input file",
                        required=True)

    parser.add_argument('--window_size',
                        default=10,
                        type=int,
                        help="window size in minutes for which the output will be produced")

    parser.add_argument('--aggregator',
                        choices=['average', 'median', 'min', 'max'],
                        default=['average'],
                        nargs='+',
                        help="specify how values should be aggregated")

    parser.add_argument('--use_word_count',
                        action='store_true',
                        help="consider nr_words attribute in events to calculate aggregates")

    parser.add_argument('--output_file',
                        help="Path for the output file. If not set stdout will be used")

    return parser.parse_args()
