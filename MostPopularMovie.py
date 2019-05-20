from mrjob.job import MRJob
from mrjob.job import MRStep

class MRMostPopularMovie(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ratings,reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]
    def mapper_get_ratings(self,key,line):
        (userID,movieID,rating,timestamp)=line.split("\t")
        yield movieID,1
        
    def reducer_count_ratings(self,key,values):
        yield None, (sum(values),key)
        
    def reducer_find_max(self,key,values):
        yield max(values)
        
if __name__=='__main__':
    MRMostPopularMovie.run()