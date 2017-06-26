# -*- coding:utf-8 -*-

import argparse
import codecs
import os
import time

from pathos.pools import ProcessPool
from pathos.helpers import cpu_count

class RFParser(object):
    def __init__(self):
        pass

    def extract(self, txt_dir, rf_dir, parsing_log):
        self.txt_dir = txt_dir
        if not os.path.exists(txt_dir):
            os.makedirs(txt_dir)

        self.rf_dir = rf_dir
        if not os.path.exists(rf_dir):
            os.makedirs(rf_dir)

        def text_gen(txt_dir):
            # Yields markup & name
            for path, subdirs, filenames in os.walk(txt_dir):
                for filename in filenames:
                    if not filename.endswith('.txt'):
                        continue
                    path_split = path.split("/")
                    cik = path_split[-1]
                    yield cik, filename

        def parsing_job(obj):
            cik, filename = obj
            print("Parsing: {}".format(filename))

            # Read text
            filepath = os.path.join(self.txt_dir, cik, filename)
            with codecs.open(filepath,'rb',encoding='utf-8') as fin:
                text = fin.read()

            name, ext = os.path.splitext(filename)
            # Parse Risk Factor part

            msg = ""
            rf, end = self.parse_rf(text)
            # Parse second time if first parse results in index
            if rf and len(rf.encode('utf-8')) < 1000:
                rf, _ = self.parse_rf(text, start=end)

            if rf: # Has value
                msg = "SUCCESS"
                file_dir = os.path.join(self.rf_dir, cik)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                
                rf_path = os.path.join(file_dir, name + '.rf')
                with codecs.open(rf_path,'w', encoding='utf-8') as fout:
                    fout.write(rf)
            else:
                msg = msg if rf else "RF NOT FOUND"
            #print("{},{}".format(name,msg))
            return name + '.txt', msg #


        ncpus = cpu_count() if cpu_count() <= 8 else 8
        pool = ProcessPool( ncpus )

        _start = time.time()
        parsing_failed = pool.map( parsing_job, \
                                   text_gen(self.txt_dir) )
        _end = time.time()

        print("RF parsing time taken: {} seconds.".format(_end-_start))

        # Write failed parsing list
        count = 0
        with open(parsing_log,'w') as fout:
            print("Writing parsing results to {}".format(parsing_log))
            for name, msg in parsing_failed:
                fout.write('{},{}\n'.format(name,msg))
                if msg != "SUCCESS":
                    count = count + 1

        print("Number of failed text:{}".format(count))

    def parse_rf(self, text, start=0):
        debug = False
        """
            Return Values
        """

        rf = ""
        end = 0

        """
            Parsing Rules
        """
        
        # Define start & end signal for parsing
        item1_begins = [ u'\nITEM 1A.', u'\nITEM 1A –', u'\nITEM 1A:', u'\nITEM 1A ', u'\nITEM 1A\n', u'\nRISK FACTORS\n' ]
        item1_ends   = [ u'\nITEM 1B.', u'\nITEM 1B –', u'\nITEM 1B:', u'\nITEM 1A ', u'\nITEM 1B\n' ]
        if start != 0:
            item1_ends.append('\nITEM 1A') # Case: ITEM 1B does not exist
        item2_begins = [ u'\nITEM 2.', u'\nITEM 2 –', u'\nITEM 2:', u'\nITEM 2 ', u'\nITEM 2\n' ]

        """
            Parsing code section
        """
        text = text[start:]

        # Get begin
        for item1 in item1_begins:
            begin = text.rfind(item1)
            if debug:
                print(item1,begin)
            if begin != -1:
                break

        if begin != -1: # Begin found
            for item1B in item1_ends:
                end = text.rfind(item1B, begin+1)
                if debug:
                    print(item1B,end)
                if end != -1:
                    break

            if end == -1: # ITEM 1B does not exist
                for item2 in item2_begins:
                    end = text.rfind(item2, begin+1)
                    if debug:
                        print(item2,end)
                    if end != -1:
                        break

            # Get RF
            if end > begin:
                rf = text[begin:end].strip()
            else:
                end = 0

        return rf, end

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Parse RF section of Edgar Form 10k")
    parser.add_argument('--txt_dir',type=str,default='./data/txt')
    parser.add_argument('--rf_dir',type=str,default='./data/rf')
    parser.add_argument('--log_file',type=str,default='./parsing.log')
    args = parser.parse_args()

    # Extract Risk Factor from processed text
    # Note that the parser parses every text in the text_dir, not according to the index file
    parser = RFParser()
    parser.extract(txt_dir=args.txt_dir, rf_dir=args.rf_dir, parsing_log=args.log_file)
