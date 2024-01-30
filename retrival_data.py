import random
import os
from os.path import join
import json
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm
from sql_data import WikiSql

from typing import List
'''
the loading process even with multithreading takes an HOUR on my machine which is just too slow
I suspect having the files all over the place like this dosent help

putting it as an sql database means we get to play with order a bit more
and we are getting better compression on hebrew text
and we are getting data locallity and transactions
'''

class RetrivalDataGen():
    def __init__(self, data_path, seed=23, reshuffle_at_epoch_end=False):
        self.wiki = WikiSql(data_path)
        self.seed = seed
        self.reshuffle_at_epoch_end = reshuffle_at_epoch_end
        self.folders = self.wiki.get_all_folders()
        self.rng = random.Random(seed)

        self.shuffle_folders()

    def shuffle_folders(self):
        self.rng.shuffle(self.folders)

    def get_data(self,langs:List[str]):
    	for f in self.folders:
    		yield {l:self.wiki.load_data(f,l) for l in langs}
    	
    	if(self.reshuffle_at_epoch_end):
    		self.shuffle_folders()



if __name__=="__main__":
	dataset=RetrivalDataGen(join('data','wikisql.db'))