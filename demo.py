#! /usr/bin/python

from Queue import Queue
import threading
import time
import rfc822
import json

import twitterstream

class Info(object):
    sent_counter = Queue()
    lost_counter = Queue()
    
    @staticmethod
    def init():
        Info.sent_counter.put(0)
        Info.lost_counter.put(0)

    @staticmethod
    def sent(sent=0):
        count = Info.sent_counter.get()
        count += sent
        Info.sent_counter.put(count)
        return count

    @staticmethod
    def lost(lost=0):
        count = Info.lost_counter.get()
        if lost > count:
            Info.lost_counter.put(lost)
        else:
            Info.lost_counter.put(count)
        return max(lost, count)

    @staticmethod
    def output(size):
        sent = Info.sent()
        lost = Info.lost()
        total = sent + lost
        threads = threading.activeCount() - 1
        if total % 100 == 0:
            print '[received: %d, lost: %d (%.2f%%), workers %d/64, queue: %d]' % (sent, lost, float(sent * 100) / (total), threads, size)
            
def queue_wrapper(queue, timeout):
    while True:
        try:
            yield queue.get(timeout=timeout)
        except:
            break

def read(input):
    for item in input:
        item = json.loads(item)
        if 'in_reply_to_status_id' in item:
            yield item
        elif 'limit' in item:
            Info.lost(int(item['limit']['track']))
        elif 'delete' in item:
            #delete = item['delete']['status']
            #delete['id'], delete['user_id']
            pass

def extract(item):
    return (long(time.mktime(rfc822.parsedate(item['created_at']))),
            item['user']['id_str'],
            item['id_str'],
            item['user']['followers_count'],
            [url['url'] for url in item['entities']['urls']],
            [hashtag['text'].lower() for hashtag in item['entities']['hashtags']])

def consumer(input):
    items = read(queue_wrapper(input, 1))
    for item in items:
        item = extract(item)
        print str(item)
        Info.sent(1)
        Info.output(input.qsize())

def spawn(consumer, queue):
    thread = threading.Thread(target=consumer, args=(queue,))
    thread.daemon = True
    thread.start()
    
def check(queue):
    size = queue.qsize()
    threads = threading.activeCount() - 1
    if size > 100 + (threads * 10) and threads < 64:
        spawn(consumer, queue)
    
def producer(track, username, password):
    queue = Queue()
    chunks = twitterstream.filter(username, password, track)
    for chunk in chunks:
        queue.put(chunk)
        check(queue)

username = 'your_username'
password = 'your_password'

if __name__ == '__main__':
    track = [
            'redis',
            'mongodb',
            'python',
            'nginx',
            'nodejs'
        ]
		
    Info.init()
    producer(','.join(track), username, password)
