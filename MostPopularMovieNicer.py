from mrjob.job import MRJob
from mrjob.job import MRStep

class MRMostPopularMovie(MRJob):
    
    def configure_options(self):
        super(MRMostPopularMovie,self).configure_options()
        self.add_file_option('--items',help='Path to u.item')
    
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ratings,
            reducer_init=self.reducer_init,
            reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]
    def mapper_get_ratings(self,key,line):
        (userID,movieID,rating,timestamp)=line.split("\t")
        yield movieID,1
    
    def reducer_init(self):
        self.movieNames={}
        with open("u.ITEM") as f:
            for line in f:
                fields=line.split('|')
                self.movieNames[fields[0]]=fields[1]
        
    def reducer_count_ratings(self,key,values):
        yield None, (sum(values),self.movieNames[key])
        
    def reducer_find_max(self,key,values):
        yield max(values)
        
if __name__=='__main__':
    MRMostPopularMovie.run()