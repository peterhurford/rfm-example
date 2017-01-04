from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

filename = 'data/trans10m.csv'
print 'Starting...'
num_lines = sum(1 for line in open(filename))
print '...File length is {}'.format(num_lines)
start = datetime.now()
print '...Processing file starting at ' + str(start)
with open(filename, 'r') as filehandle:
    i = 0
    curr_done = 0
    filehandle.readline() # Throw out header
    customers = {}
    while True:
        item = filehandle.readline()
        if not item:
            break
        item = item.split(',')
        idx = int(item[0])
        data = {
            'date': datetime.strptime(item[6], '%Y-%m-%d'),
            'quantity': float(item[9]),
            'amount': float(item[10])
        }
        if idx not in customers.keys():
            customers[idx] = [data]
        else:
            customers[idx].append(data)
        i += 1
        if i % 1000000 == 0:
            now = datetime.now()
            speed = (now - start).total_seconds() * 1000 / float(i)
            lines_remaining = num_lines - i
            remaining_time = time.strftime('%H:%M:%S', time.gmtime(speed * lines_remaining / 1000.0))
            message = '{i} lines done in {time} ({done}%), {speed}ms per line, {remain} remaining'
            print message.format(i=i,
                                 time=str(now - start),
                                 done=str(int(i / float(num_lines) * 100)),
                                 speed=str(speed),
                                 remain=remaining_time)

print 'Processing...'
rfm_customer = {}
ending_date = datetime(2013, 7, 27, 0, 0)
one_year_ago = ending_date - relativedelta(years=1)
for idx, customer in customers.iteritems():
    rfm_customer[idx] = {
        'recency': ending_date - reduce(lambda x, y: x if x > y else y, map(lambda x: x['date'], customer)),
        'frequency': len(filter(lambda x: x['date'] > one_year_ago, customer)),
        'monetization': reduce(lambda x, y: x if x > y else y, map(lambda x: x['amount'], customer))
    }
print 'Awaiting further commands...'
import pdb
pdb.set_trace()
