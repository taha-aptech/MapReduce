from mrjob.job import MRJob

class WeatherCount(MRJob):
    def mapper(self, _, line):
        try:
            columns = line.split(",")
            weather = columns[-1].strip().lower()  # Assuming last column is 'weather'
            yield weather, 1
        except:
            pass

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    WeatherCount.run()
