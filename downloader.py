#!/usr/bin/env python2
# encoding: utf-8

"""
Common Crawl 2012 Downloader
"""

import boto
import sys
import os
from itertools import islice
import multiprocessing as mp


CC_PREFIX = "common-crawl/parse-output"


def get_cc_bucket():
    conn = boto.connect_s3(anon=True)
    bucket = conn.get_bucket('aws-publicdatasets')
    return bucket


def get_segments(bucket):
    request = CC_PREFIX + '/valid_segments.txt'
    segments = bucket.get_key(request).get_contents_as_string()
    return segments.split()


def get_list(bucket, segment):
    return bucket.list(prefix="{}/segment/{}/".format(CC_PREFIX, segment),
                       delimiter="/")


def get_filenames(segment):
    stats = {'arc': [], 'metadata': [], 'textdata': []}

    bucket = get_cc_bucket()

    for key in islice(get_list(bucket, segment), None):
        name = key.name.encode('utf-8').split('/')[-1]
        if name.endswith('arc.gz'):
            stats['arc'].append(key.name.encode('utf-8'))
        elif name.startswith('metadata'):
            stats['metadata'].append(key.name.encode('utf-8'))
        elif name.startswith('textData'):
            stats['textdata'].append(key.name.encode('utf-8'))
    return stats


def parse_segment(segment):
    print >> sys.stderr, "Start processing segment: {}".format(segment)
    print "{}/segment/{}".format(CC_PREFIX, segment)
    stats = get_filenames(segment)
    for k, v in stats.items():
        print k, len(v)

    if not os.path.exists(segment):
        os.makedirs(segment)

    input_queue = mp.Queue()

    # for i in stats['metadata']:
        # input_queue.put(i)

    for i in stats['textdata']:
        input_queue.put(i)

    for i in range(6):
        input_queue.put('DONE')

    processes = [mp.Process(target=download_files, args=(segment, input_queue,)) for i in
                 range(6)]
    for p in processes:
        p.start()

    for p in processes:
        p.join()


def download_files(segment, queue):
    bucket = get_cc_bucket()
    while True:
        msg = queue.get()
        if msg == 'DONE':
            break
        filename = './{}/{}'.format(segment, msg.split('/')[-1])
        bucket.get_key(msg).get_contents_to_filename(filename)


def process_segments(buckets, segments):
    for segment in segments:
        parse_segment(segment)
    print >> sys.stderr, "Finished."


def main():
    """ main """
    bucket = get_cc_bucket()
    segments = get_segments(bucket)
    process_segments(bucket, segments[:1])
    print >> sys.stderr, 'Number of segments:', len(segments)


if __name__ == '__main__':
    main()
