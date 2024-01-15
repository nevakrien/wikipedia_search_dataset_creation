import json
import os 
from os.path import join
from tqdm import tqdm

from concurrent.futures import ThreadPoolExecutor

def remove_empty(save_dir):
	empty = [join(save_dir,t) for t in os.listdir(save_dir)]
	empty = [t for t in empty if not os.listdir(t)]

	print(f'found {len(empty)} empty files deleting them')

	with ThreadPoolExecutor() as ex:
		m=ex.map(os.rmdir,empty)
		list(tqdm(m,total=len(empty)))

def check_data(save_dir):
	subs={t:os.listdir(join(save_dir,t)) for t in os.listdir(save_dir)}
	
	empty=[k for k,v in subs.items() if not v]
	errors=[k for k,v in subs.items() if 'error.txt' in v]
	
	print(f'found {len(empty)} empty dirs')
	print(f'found {len(errors)} errors dirs')

if __name__=="__main__":
	save_dir=join('data','pairs_dataset')
	remove_empty(save_dir)
	check_data(save_dir)
	

	# print(f'removing emptys')
	# 
	# 	m=ex.map(lambda x: os.rmdir(join()))
