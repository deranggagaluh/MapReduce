from mrjob.job import MRJob, MRStep
import psycopg2

#filter product with price > 100 save to new table newproduct only nameproduct and price
class filterproduct(MRJob):

    def mapper_init(self):
        self.conn = psycopg2.connect(database="postgres", user="degaraja", password="123456789", host="host.docker.internal", port="5432")
    
    def mapper(self, _, line):
        self.cur = self.conn.cursor()
        item = line.strip().split(',')
        if int(item[3]) > 100:
            self.cur.execute("insert into newproduct(productName,price)values(%s,%s)", (item[1],item[3]))
            yield item[1], item[3]
      
    def mapper_final(self):
        self.conn.commit()
        self.conn.close()

    def steps(self):
       return [
        MRStep(
            mapper_init=self.mapper_init,
            mapper=self.mapper,
            mapper_final=self.mapper_final
            )
       ]

if __name__ == '__main__':
    filterproduct.run()