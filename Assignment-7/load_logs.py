import sys
import datetime
from cassandra.cluster import Cluster
import gzip
import os
import re
from cassandra.query import BatchStatement

def match_line(line):
    pattern = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    result = re.match(pattern,line)
    if result:
        result = result.groups()
        result = list(result)
        host  = result[0]
        date_time = datetime.datetime.strptime(result[1], "%d/%b/%Y:%H:%M:%S")
        path = result[2]
        trans_bytes= int(result[3])
        resulted_tuple = (host,date_time,path,trans_bytes)
        return resulted_tuple


def main(input_dir, keyspace, table):
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)
    #read input file
    for f in os.listdir(input_dir):
        statement = session.prepare('INSERT into nasalogs (host,datetime,path,bytes,uid) values (?,?,?,?,UUID())')
        batch = BatchStatement()
        count = 0
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
             for line in logfile:
                 #get filtered data
                 row_data = match_line(line)
                 if row_data:
                    batch.add(statement, (row_data[0], row_data[1], row_data[2], row_data[3])) 
                    count += 1
                    #executing the statements in batches of 100
                    if (count == 400):
                        session.execute(batch)
                        count = 0
                        batch = BatchStatement()
             session.execute(batch)


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table    = sys.argv[3]
    main(input_dir,keyspace,table)
