#!/usr/bin/env python2
# encoding: utf-8

"""
Common Crawl 2012 Downloader
"""

import boto
import argparse
import sys
import os
from itertools import islice
from boto.s3.connection import Location


CC_PREFIX = "common-crawl/parse-output"

def get_cc_bucket():
    conn = boto.connect_s3(anon=True)
    bucket = conn.get_bucket('aws-publicdatasets')
    return bucket


def get_segments(bucket):
    segments = bucket.get_key(CC_PREFIX + '/valid_segments.txt').get_contents_as_string()
    return segments.split()


def parse_segment(bucket, segment):
    print >> sys.stderr, "Start processing segment: {}".format(segment)
    print "{}/segment/{}".format(CC_PREFIX, segment)
    stats = {'arc' : [], 'metadata': [], 'textdata': []}
    for key in islice(bucket.list(prefix="{}/segment/{}/".format(CC_PREFIX, segment), delimiter="/"),None):
        name = key.name.encode('utf-8').split('/')[-1]
        if name.endswith('arc.gz'):
            stats['arc'].append(key.name.encode('utf-8'))
        elif name.startswith('metadata'):
            stats['metadata'].append(key.name.encode('utf-8'))
        elif name.startswith('textData'):
            stats['textdata'].append(key.name.encode('utf-8'))
    for k, v in stats.items():
        print k, len(v)

    if not os.path.exists(segment):
        os.makedirs(segment)
    for _file in stats['arc']:
        print _file
        bucket.get_key(_file).get_contents_to_filename('./{}/{}'.format(segment,
                                                                        _file.split('/')[-1]))



def main():
    """ main """
    bucket = get_cc_bucket()
    segments = get_segments(bucket)
    print >> sys.stderr, 'Number of segments:', len(segments)
    parse_segment(bucket, segments[0])
    print >> sys.stderr, "Finished."


if __name__ == '__main__':
    main()

