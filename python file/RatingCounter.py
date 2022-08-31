#ini contoh untuk mapper reducer (basic)

from mrjob.job import MRJob #import MRjob dari package mrjob.job

class MRRatingCounter (MRJob): #class implemant dari mrjob namanya MRRatingCounter
    def mapper (self, key, line): 
        (userID, movieID, rating, timestamp) = line.split('\t') 
        yield rating, 1 #yeald ini spt return tapi untuk generator, dan output ini masuk ke input reducer

    def reducer(self, rating, occurences): #parameter ini dari mapper diatas
        yield rating, sum(occurences)

if __name__ == '__main__': 
    MRRatingCounter.run()