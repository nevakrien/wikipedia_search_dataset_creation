from main import sensitize_text

import os
from os.path import join
import json 

if __name__=="__main__":
	save_dir=join('data','pairs_dataset')
	os.makedirs(save_dir,exist_ok=True)

	titles=list(os.listdir(save_dir))


	with open(join(save_dir,titles[0],'he.json')) as f:
		d=json.load(f)
	print(d)