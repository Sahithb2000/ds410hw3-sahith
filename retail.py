from mrjob.job import MRJob
import csv


def preProcess(old_l):
    l = [x for x in old_l]
    for i in range(len(l)):
        l[i] = l[i].replace('"', '')
        l[i] = l[i].strip()
        if l[i] == "":
            l[i] = "NA"
    return l

class Retail(MRJob):

    def mapper(self, key, line):
        (InvoiceNo, StockCode, Description, 
        Quantity, InvoiceDate, UnitPrice, 
        CustomerID, Country) =  preProcess( list ( csv.reader([line]) )[0])
        
        if InvoiceNo != "InvoiceNo":
            yield(Description, float(Quantity) * float(UnitPrice))
            
    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Retail.run()
    