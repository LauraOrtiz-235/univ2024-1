from mrjob.job import MRJob
import re
import os

WORD_REGEX = re.compile(r"\b\w+\b")

class InvertedIndex(MRJob):
    def mapper(self, _, line):
        # Obtener la ruta del archivo actual
        file = os.environ['map_input_file']
        file_name = os.path.basename(file)
        words = WORD_REGEX.findall(line)

        for word in words:
            if len(word) > 5:
                yield (word.lower(), file_name)

    def reducer(self, word, file_names):
        yield (word, list(set(file_names)))

if __name__ == "__main__":
    InvertedIndex.run()
