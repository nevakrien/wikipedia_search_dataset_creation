from wiki import get_article_pairs_from_hebrew, parse_to_dict
from validate import remove_empty

import json
import os 
from os.path import join

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from requests.exceptions import ConnectionError, ChunkedEncodingError,JSONDecodeError
# import time

# def delay_iterator(l,window):
# 	#t=time.time()
# 	for x in l:
# 		yield x
# 		time.sleep(window)

def sensitize_text(text):
    
    text = text.replace(" ", "_")
    text = text.replace("\n", "_n_")
    
    for i,char in enumerate(["/", "\\", ":",'.',"\"","\'"]):
        text = text.replace(char, f"@{i}")
        
    return text

def process_title(title,save_dir,bar=None):
	e,h=get_article_pairs_from_hebrew(title)

	save_dir=join(save_dir,title)
	os.mkdir(save_dir)

	if not e or not h:
		with open(join(save_dir,'error.txt'),'w') as f:
			f.write('no pair')
		if bar:
			bar.update(1)
		return 

	e=parse_to_dict(e)
	h=parse_to_dict(h)
	

	with open(join(save_dir,'en.json'),'w') as f:
		json.dump(e,f)

	with open(join(save_dir,'he.json'),'w') as f:
		json.dump(h,f)

	if bar:
		bar.update(1)

def make_data(save_dir,titles,bar):
	with ThreadPoolExecutor() as ex:
		m=ex.map(lambda x: process_title(x,save_dir,bar),titles)
		list(m)

if __name__=="__main__":
	save_dir=join('data','pairs_dataset')
	os.makedirs(save_dir,exist_ok=True)

	with open(join('data','hewiki-20240101-all-titles-in-ns0')) as f:
		titles=f.read().split('\n')

	titles=list(map(sensitize_text,titles))
	bar=tqdm(range(len(titles)))


	done=set(os.listdir(save_dir))
	#bar.n=len(done)
	bar.update(len(done))
	titles=[x for x in titles if x not in done]
	
	print(f'found existing {len(done)} elements')
	while(titles):
		try: 
			print('starting data collection')
			make_data(save_dir,titles,bar)
			#Exception
		except (ConnectionError, ChunkedEncodingError,JSONDecodeError) as e:
			print(f'errored likely because of API limits:\n {e}')
			print('preforming cleanup')
			remove_empty(save_dir)

			done=set(os.listdir(save_dir))
			#bar.n=len(done)
			titles=[x for x in titles if x not in done]

	bar.close()

	

