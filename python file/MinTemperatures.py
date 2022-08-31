#ini contoh untuk cari nilai nilai minimum suhu

from mrjob.job import MRJob

class MRMinTemperature (MRJob):

    def MakeFahrenheit (self, tenthsOfCelsius):
    celsius= float(tenthsOfCelsius) / 10.0
    fahrenheit = celsius * 1.8+ 32.0
    return fahrenheit

def mapper(self, _, line):
    (location, date, type, data, x, y, z, w)= line.split(',') #data ini temperature, x,y,z,w ini data dari 1800.csv, karena tidak ada datanya maka tidak didefinisikan
    if (type == 'TMIN'):
        temperature = self.MakeFahrenheit (data)
        yield location, temperature

def reducer(self, location, temps): 
    yield location, min(temps) #outputnya ini

if __name__ == '__main__':
    MRMinTemperature.run()