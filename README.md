#Unbabel challenge

##### Running the solution
Solution is written entirely in Python. It was tested with Python 3.7, but any Python 3.x version should work.

To see usage help message, `cd` into root directory and type `python main.py --help`. The flags follow the same
signature described in the challenge proposal, thus to run the example input enter the following command:
`python main.py --input_file test/test_inputs/input1.json --window_size 10`. By default output will be printed to
`stdout`. This can be changed with stdout redirection or using the `--output_file` flag.
##### Tests
A few unit tests were developed to ensure correctness of the solution. To run them you need to install `parameterized`
package. You can do this by running `pip install -r requirements.txt`. Note that if you are not running this solution in
a disposable environment like a container or virtual machine, it's recommended to install dependencies in a virtual
environment like virtualenv or pipenv. 

To run the tests type `python -m unittest test` from the solution's root directory.

 
 ##### Bonus
 - While average is a good metric, it may lead to incorrect conclusions because it is greatly influenced by minimum and
 maximum values. Thus, often times it is also useful to include other aggregation information alongside.
 This solution allows you to output other types of moving aggregate functions, namely minimum, maximum and median in
 addition to the requested average function. Example of usage to output average and median:
 `python main.py --input_file test/test_inputs/input1.json --window_size 10 --aggregator average median`.
 By default, only the average aggregator is used. 
 
 
 - Intuitively, it is fair to consider that a request to translate bigger text, would take more time to complete.
 Therefore, time to complete alone is not enough information to judge whether the system is being slow. A more fair
 comparision would be an average of how much time it took to translate one word of that event. This is what
 `--use_word_count` does in the implemented solution, i.e. for each event, instead of considering `duration` field,
 the program computes `duration / nr_words` and uses that value in aggregate functions. This is compatible with any
 combination of aggregation functions described in previous point. Example of usage:
 `python main.py --input_file test/test_inputs/input1.json --window_size 10 --aggregator average median min max --use_word_count`