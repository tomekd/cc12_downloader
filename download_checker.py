#!/usr/bin/python2

import glob
import boto
import sys
from itertools import islice

CC_PREFIX = "common-crawl/parse-output"


def get_cc_bucket():
    conn = boto.connect_s3(anon=True)
    bucket = conn.get_bucket('aws-publicdatasets')
    return bucket


def get_list(bucket, segment):
    return bucket.list(prefix="{}/segment/{}/".format(CC_PREFIX, segment),
                       delimiter="/")


def get_cc_filenames(segment):
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


def get_downloaded_filenames(segment):
    return glob.glob("./{}/textData*".format(segment))


def check_segment(segment):
    print "SEGMENT:", segment
    cc = get_cc_filenames(segment)['textdata']
    fs = get_downloaded_filenames(segment)
    print "CC:", len(cc)
    print "DOWNLOADED:", len(fs)
    if len(cc) == len(fs):
        print "DOWNLOADED CORRECTLY."
        with open('{}/{}.DOWNLOADED'.format(segment, segment), 'w') as _file:
            _file.write(str(len(cc)) + '\n')


def main():
    segments = sys.argv[1:]
    for segment in segments:
        check_segment(segment)


if __name__ == '__main__':
    main()
