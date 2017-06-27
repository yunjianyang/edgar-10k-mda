# -*- coding:utf-8 -*-

import argparse
import codecs
import csv
from glob import glob
import os
import re
import sys
import unicodedata
import re
import bleach


from html2text import html2text
from sql_connect import df_from_sql
from bs4 import BeautifulSoup
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
import requests

SEC_GOV_URL = 'https://www.sec.gov/Archives'

class Form10k(object):
    def __init__(self):
        pass

    def _process_text(self, text):
        """
            Preprocess Text
        """
        text = unicodedata.normalize("NFKD", text) # Normalize
        text = '\n'.join(text.splitlines()) # Let python take care of unicode break lines

        # Convert to upper
        text = text.upper() # Convert to upper

        # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
        text = re.sub(r'[ ]+\n', '\n', text)
        text = re.sub(r'\n[ ]+', '\n', text)
        text = re.sub(r'\n+', '\n', text)

        # To find MDA section, reformat item headers
        text = text.replace('\n.\n','.\n') # Move Period to beginning

        text = text.replace('\nI\nTEM','\nITEM')
        text = text.replace('\nITEM\n','\nITEM ')
        text = text.replace('\nITEM  ','\nITEM ')

        text = text.replace(':\n','.\n')

        # Math symbols for clearer looks
        text = text.replace('$\n','$')
        text = text.replace('\n%','%')

        # Reformat
        text = text.replace('\n','\n\n') # Reformat by additional breakline

        return text

    def download(self, df, txt_dir):
        # Save to txt dir
        self.txt_dir = txt_dir
        if not os.path.exists(self.txt_dir):
            os.makedirs(self.txt_dir)

        # Count Total Urls to Process
        num_urls = len(df)

        def iter_path_generator(df):
            for index, row in df.iterrows():
                url_idx = index
                year = row['YearNum']
                company_name = row['CompanyName']
                cik = row['CIK']
                filename = row['Filename']
                url = os.path.join(SEC_GOV_URL, filename).replace("\\", "/")
                yield (url_idx, url, cik)

        def download_job(obj):
            url_idx, url, cik = obj

            fname = '_'.join(url.split('/')[-2:])

            fname, ext = os.path.splitext(fname)
            htmlname = fname + '.html'

            company_dir = os.path.join(self.txt_dir, cik)
            if not os.path.exists(company_dir):
                os.makedirs(company_dir)

            text_path = os.path.join(company_dir, fname + '.txt')

            if os.path.exists(text_path):
                print("Already exists, skipping {}...".format(url))
                sys.stdout.write("\033[K")
            else:
                print("Total: {}, Downloading & Parsing: {}...".format(num_urls, url_idx))
                sys.stdout.write("\033[K")

                r = requests.get(url)
                try:
                    # Parse html with Beautiful Soup ’ “  ”
                    #p = re.compile('<TABLE(.*?)<\/TABLE>', re.DOTALL | re.IGNORECASE)
                    #drop_table = p.sub('', r.content)
                    #soup = BeautifulSoup(r.content, "html.parser" )
                    #text = soup.findAll(text=True)
                    #text = html2text(r.content)
                    p = re.compile('<FILENAME>(.*?)\n')
                    destination = p.search(r.content).group(1)
                    filename, ext = os.path.splitext(url)
                    final_url = os.path.join(filename.replace('-',''), destination)
                    r = requests.get(final_url)
                    text = bleach.clean(r.content, tags=[], strip=True)

                    # Process Text
                    text = self._process_text(text)
                    text_path = os.path.join(company_dir, fname + '.txt')

                    # Write to file
                    with codecs.open(text_path,'w',encoding='utf-8') as fout:
                        fout.write(text)
                except BaseException as e:
                    print("{} parsing failed: {}".format(url,e))

        ncpus = cpu_count() if cpu_count() <= 8 else cpu_count() - 1;
        pool = ProcessPool( ncpus )
        pool.map( download_job,
                    iter_path_generator(df) )

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Download Edgar Form 10k according to index")
    parser.add_argument('--txt_dir',type=str,default='./data/txt')
    args = parser.parse_args()

    # Download 10k forms, parse html and preprocess text

    sql_query = """
    SELECT *
    FROM SFMMarketData.dbo.v_sec_idx vsi 
    WHERE FormType = '10-K' and DaTaType = 'master' and YearNum > 2005
    ORDER BY vsi.CIK, vsi.YearNum
    """

    df = df_from_sql(sql_query)
    form10k = Form10k()
    form10k.download(df=df, txt_dir=args.txt_dir)
