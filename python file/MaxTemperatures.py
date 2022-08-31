#ini contoh untuk cari nilai nilai maximum suhu

from mrjob.job import MRJob

class MRMaxTemperature (MRJob):

def MakeFahrenheit (self, tenthsOfCelsius):
    celsius= float(tenthsOfCelsius) / 10.0
    fahrenheit = celsius * 1.8+ 32.0
    return fahrenheit

def mapper(self, _, line):
    (location, date, type, data, x, y, z, w)= line.split(',') #data ini temperature, x,y,z,w ini data dari 1800.csv, karena tidak ada datanya maka tidak didefinisikan
        if (type == 'TMAX'):
        temperature = self.MakeFahrenheit (data)
        yield location, temperature

def reducer(self, location, temps): 
    yield location, max(temps) #outputnya ini

if __name__ == '__main__':
    MRMaxTemperature.run()