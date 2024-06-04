from mrjob.job import MRJob
import time

class Twwet(MRJob):
    def mapper(self, _, line):
        fields = line.split(";")

        time_epoch = int(fields[0])/1000
        day = time.strftime("%H", time.gmtime(time_epoch))
        yield(day, 1)

    def combiner(self, day, values):
        yield(day, sum(values))

    def reducer(self, day, values):
        total = sum(values)
        yield(day, total)

if __name__ == "__main__":  
    Twwet.run()