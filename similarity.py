import argparse
import os
import codecs
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer

class MDAsimilarity():
    def __init__(self):
        pass

    def _get_MDA(self, subdir):
        year = []
        content = []
        for filename in os.listdir(subdir):
            filepath = os.path.join(subdir, filename)
            year.append(filename.split("-")[1])
            with codecs.open(filepath,'rb',encoding='utf-8') as fin:
                text = fin.read()
            content.append(text)
        return year, content

    def _calculate_similarity(self, year, content):
        tokenize = lambda doc: doc.lower().split(" ")
        sklearn_tfidf = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, smooth_idf=False, sublinear_tf=True, tokenizer=tokenize)
        tfidf = sklearn_tfidf.fit_transform(content)
        df = pd.DataFrame((tfidf * tfidf.T).A)
        df.columns, df.index = year, year
        return df

    def calculate(self, mda_dir, category):
        for subdir in os.listdir(mda_dir):
            print "processing cik: {}".format(subdir)
            subdir_path = os.path.join(mda_dir, subdir)
            year, content = self._get_MDA(subdir_path)
            df = self._calculate_similarity(year, content)
            csv_path = os.path.join(subdir_path, "{}_difference_matrix.csv".format(category))
            df.to_csv(csv_path, float_format='%.4f')



if __name__ == "__main__":
    parser = argparse.ArgumentParser("Download Edgar Form 10k according to index")
    parser.add_argument('--dir',type=str,default='./data')
    parser.add_argument('--category',type=str,default='mda')
    args = parser.parse_args()
    similarity = MDAsimilarity()
    directory = os.path.join(args.dir, args.category)
    similarity.calculate(directory, args.category)
