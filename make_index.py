#!/usr/bin/env python
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import glob

from hadoop.io import SequenceFile

def main():
    segment = sys.argv[1]
    files = glob.glob("./{}/textData*".format(segment))
    for text_file in files:
        print >> sys.stderr, text_file
        try:
            reader = SequenceFile.Reader(text_file)

            key_class = reader.getKeyClass()
            value_class = reader.getValueClass()

            key = key_class()
            value = value_class()

            #reader.sync(4042)
            position = reader.getPosition()
            while reader.next(key, value):
                print '*' if reader.syncSeen() else ' ',
                print '%s %s %6s' % (segment, text_file, key.toString())
                position = reader.getPosition()

            reader.close()
        except Exception:
            print >> sys.stderr,'Parsing failed: ', text_file

if __name__ == '__main__':
    main()

