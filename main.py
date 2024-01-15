from wiki import get_article_pairs_from_hebrew, parse_to_dict

import json
import os 
from os.path import join

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def process_title(title,save_dir):
	e,h=get_article_pairs_from_hebrew(title)
	if not e or not h:
		return 

	e=parse_to_dict(e)
	h=parse_to_dict(h)
	save_dir=join(save_dir,title)
	os.makedirs(save_dir)

	with open(join(save_dir,'en.json'),'w') as f:
		json.dump(e,f)

	with open(join(save_dir,'he.json'),'w') as f:
		json.dump(h,f)

if __name__=="__main__":
	save_dir=join('data','pairs_dataset')
	os.makedirs(save_dir,exist_ok=True)

	with open(join('data','hewiki-20240101-all-titles-in-ns0')) as f:
		titles=f.read().split('\n')

	done=set(os.listdir(save_dir))
	titles=[x for x in titles if x not in done]

	with ThreadPoolExecutor() as ex:
		m=ex.map(lambda x: process_title(x,save_dir),titles)
		list(tqdm(m,total=len(titles)))

	

