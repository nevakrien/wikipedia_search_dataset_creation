import random
import os
from os.path import join
import json
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

'''
the loading process even with multithreading takes an HOUR on my machine which is just too slow
I suspect having the files all over the place like this dosent help

adding compression to make this better when loading to the cloud
'''

class RetrivalDataGen():
	def __init__(self,data_dir,seed=23,reshufle_at_epoch_end=False):
		self.data_dir=data_dir
		self.seed=seed 
		self.reshufle_at_epoch_end=reshufle_at_epoch_end
		self.rng=random.Random(seed)

		self.data=[]
		with ThreadPoolExecutor() as excutor:
			m=excutor.map(self.load_data,os.listdir(data_dir))
			self.num_invalid=sum(tqdm(m,total=len(os.listdir(data_dir))))



	def load_data(self,folder):
		path=join(self.data_dir,folder)
		ans={}
		for name in os.listdir(path):
			if name[-5:]!='.json':
				continue
			with open(join(path,name)) as f:
				d=json.load(f)

			ans[name[:-5]]=d 

		if len(ans)==0:
			return 1

		self.data.append(ans)
		return 0

if __name__=="__main__":
	dataset=RetrivalDataGen(join('data','pairs_dataset'))