from mrjob.job import MRJob
checking = 'ProductID'

class filterproduct(MRJob):

    def mapper(self, _, line):
       item = line.strip().split(',')
       if not item[3] in checking:
            if int(item[3]) > 93:
                yield item[1], item[3]


if __name__ == '__main__':
    filterproduct.run()