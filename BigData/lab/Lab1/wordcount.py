# Lab 1. Basic wordcount
from mrjob.job import MRJob
import re

# this is a regular expression that finds all the words inside a String
WORD_REGEX = re.compile(r"\b\w+\b")

# this line declares the class Lab1, that extends the MRJob format.
class Lab1(MRJob):
# this class will define two additional methods: the mapper method goes after this line
# and the reducer method goes after this line
# this part of the python script tells to actually run the defined MapReduce job.
# Note that Lab1 is the name of the class

    # Tamaño de las palabras
    def mapper(self, _, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield (len(word), 1)

    def combiner(self, word, values):
        yield (word, sum(values))

    def reducer(self, word, values):
        total = sum(values)
        yield (word, total) 

if __name__ == "__main__":
    Lab1.run()