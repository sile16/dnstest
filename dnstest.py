import os
import shutil
import threading
import Queue
import dns.resolver
from pprint import pprint
import time
import traceback


dns_server = "192.168.142.150"
log_file = "log_data"
dns_name = "isi150.lappy.lab"


class DNSQueryWorker(threading.Thread):
    """Threaded Folder Discovery and Copy"""
    def __init__(self,queue,stats,stats_burst):
        threading.Thread.__init__(self)
        self.queue = queue
        self.stats = stats
        self.stats_burst = stats_burst
        self.stats['error_empty'] = 0
        self.stats['error_exception'] = 0
        self.stats_burst['error_empty'] = 0
        self.stats_burst['error_exception'] = 0
        self.stats['error_nxdomain'] = 0
        self.stats['error_timeout'] = 0
        self.stats['error_otherdns'] = 0
        self.stats_burst['error_nxdomain'] = 0
        self.stats_burst['error_timeout'] = 0
        self.stats_burst['error_otherdns'] = 0



    def run(self):
        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [dns_server]

        while(True):
            workitem = self.queue.get()
            if workitem == 'b':
                stat_current = self.stats_burst
            else:
                stat_current = self.stats

            try:
                answer = my_resolver.query(dns_name)

                count=0
                for rdata in answer:
                    count+=1
                    if rdata.address not in stat_current:
                        stat_current[rdata.address]=1
                    else:
                        stat_current[rdata.address]+=1
                if count == 0:
                    stat_current['error_empty'] +=1

            except dns.resolver.NXDOMAIN:
                stat_current['error_nxdomain']+=1
            except dns.resolver.Timeout:
                stat_current['error_exception']+=1
            except dns.exception.DNSException:
                stat_current['error_otherdns']+=1
                traceback.print_exc()
            except Exception, e:
                stat_current['error_exception']+=1
                traceback.print_exc()

            self.queue.task_done()






def main():
    work_queue = Queue.Queue()
    stats = []
    stats_burst = []

    for i in range(300):
        stats.append({})
        stats_burst.append({})
        t = DNSQueryWorker(work_queue,stats[i],stats_burst[i])
        t.setDaemon(True)
        t.start()

    for i in range(20000):
        work_queue.put("")

        time.sleep(0.004)
        if i % 5000 == 0:
            print "."
            for b in range(1000):
                work_queue.put('b')


    work_queue.join()


    total = {}
    for thread_stats in stats:
        for item in thread_stats:
            if item in total:
                total[item]+=thread_stats[item]
            else:
                total[item]=thread_stats[item]

    print('total:')
    pprint(total)

    total = {}
    for thread_stats in stats_burst:
        for item in thread_stats:
            if item in total:
                total[item]+=thread_stats[item]
            else:
                total[item]=thread_stats[item]

    print('total:')
    pprint(total)




if __name__ == "__main__":
    main()