# mencari kata pada buku pake regex, hasilnya bagus tanpa simbol2

from mrjob.job import MRJob
import re #import dulu regex

WORD_REGEXP = re.compile(r"[\w']+") #re.compile ini artinya kita mau cari regex nya patternya gimana , \w brarti word
#kalo \d brarti word

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
