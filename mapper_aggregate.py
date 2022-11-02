from mrjob.job import MRJob, MRStep
import psycopg2

class aggregatproduct(MRJob):

    def mapper_init(self):
        self.conn = psycopg2.connect(database="postgres", user="degaraja", password="123456789", host="host.docker.internal", port="5432")
    
    def mapper(self, _, line):
        item = line.strip().split(',')
        year = int(item[1][-4:])
        yield year, int(item[4])

    def reducer(self, key, values):
        yield key, sum(values)

    def inputtodb(self, key , values):
        self.cur = self.conn.cursor()
        self.cur.execute("insert into productyear(productyearrr,countyear)values(%s,%s)", (key, values))

    def mapper_final(self):
        self.conn.commit()
        self.conn.close()

    def step(self):
        return[
            MRStep(
                mapper_init = self.mapper_init,
                mapper = self.mapper,
                reducer = self.reducer,
                inputtodb = self.inputtodb,
                mapper_final = self.mapper_final
            )
        ]
   
if __name__ == '__main__':
    aggregatproduct.run()