from collections import Counter
from datetime import datetime
from dateutil.relativedelta import relativedelta  # pip install python-dateutil
from multiprocessing import Pool
from vowpal_platypus import split_file
import time

def run_core(filename):
    print '{} Starting...'.format(filename)
    num_lines = sum(1 for line in open(filename))
    print '{}: ...File length is {}'.format(filename, num_lines)
    start = datetime.now()
    print '{}: ...Processing file starting at '.format(filename) + str(start)
    with open(filename, 'r') as filehandle:
        i = 0
        curr_done = 0
        customers = {}
        filehandle.readline() # Throw out header (TODO: Remove)
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
                message = '{filename}: {i} lines done in {time} ({done}%), {speed}ms per line, {remain} remaining'
                print message.format(filename=filename,
                                     i=i,
                                     time=str(now - start),
                                     done=str(int(i / float(num_lines) * 100)),
                                     speed=str(speed),
                                     remain=remaining_time)

    print '{} Processing raw RFM data...'.format(filename)
    rfm_customer = {}
    ending_date = datetime(2013, 7, 27, 0, 0)
    one_year_ago = ending_date - relativedelta(years=1)
    for idx, customer in customers.iteritems():
        rfm_customer[idx] = {
            'recency': ending_date - reduce(lambda x, y: x if x > y else y, map(lambda x: x['date'], customer)),
            'frequency': len(filter(lambda x: x['date'] > one_year_ago, customer)),
            'monetization': sum(map(lambda x: x['amount'], customer)) / float(len(customer))
        }

    print '{} Converting to RFM score...'.format(filename)
    def ordinal_score(data, key):
        score = sorted(zip(map(lambda x: x[key], data.values()), data.keys()))
        return Counter(dict(map(lambda x: (x[1][1], x[0]), zip(range(len(score)), score))))

    frequency_scores = ordinal_score(rfm_customer, 'frequency')
    recency_scores = ordinal_score(rfm_customer, 'recency')
    monetization_scores = ordinal_score(rfm_customer, 'monetization')
    rfm_scores = frequency_scores + recency_scores + monetization_scores
    print '{}: Shuffling back...'.format(filename)
    return rfm_scores

filenames = split_file('data/transactions.csv', num_cores=40, header=True)
pool = Pool(40)
rfm_scores_list = pool.map(run_core, filenames)
rfm_scores = {}
[rfm_scores.update(scores) for scores in rfm_scores_list]
customers_sorted_by_rfm = map(lambda x: x[0], sorted(rfm_scores.items(), key=lambda x: x[1]))
cust_to_decile = zip(customers_sorted_by_rfm, map(lambda x: x / max(int(len(customers_sorted_by_rfm) / 10), 9) + 1, range(len(customers_sorted_by_rfm))))
print 'Awaiting further commands...'
import pdb
pdb.set_trace()
