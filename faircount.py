from mrjob.job import MRJob

class WordCount(MRJob):
    
    def mapper(self, key, line):
        words = line.split()
        
        yield("_lines_", 1)
        
        for w in words:
            yield (w, 1)
            for c in w:
                if ('a' <= c and c <= 'z') or ('A' <= c and c <= 'Z'): 
                    yield(c + "_", 1)
    
    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()