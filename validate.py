import json
import os 
from os.path import join
from tqdm import tqdm

from concurrent.futures import ThreadPoolExecutor

def truncate(text,max_bytes=200):
	# Encode to bytes and truncate if necessary
    text_bytes = text.encode('utf-8')
    if len(text_bytes) > max_bytes:
        # Truncate the byte string and decode back to a string
        text = text_bytes[:max_bytes].decode('utf-8', errors='ignore')
    return text

def sensitize_text(text):
    
    text = text.replace(" ", "_")
    text = text.replace("\n", "_n_")
    
    for i,char in enumerate(["/", "\\", ":",'.',"\"","\'"]):
        text = text.replace(char, f"@{i}")
    
    text=truncate(text,200)

    return text

def truncate_names(save_dir):
	def rename_file(name):
		new_name=truncate(name)
		if(new_name!=name):
			os.rename(join(save_dir,name),join(save_dir,new_name))

	with ThreadPoolExecutor() as ex:
		m=ex.map(rename_file,os.listdir(save_dir))
		list(m)

def remove_empty(save_dir):
	empty = [join(save_dir,t) for t in os.listdir(save_dir)]
	empty = [t for t in empty if not os.listdir(t)]

	print(f'found {len(empty)} empty files in {save_dir} deleting them')

	with ThreadPoolExecutor() as ex:
		m=ex.map(os.rmdir,empty)
		list(m)
		#list(tqdm(m,total=len(empty)))

def check_data(save_dir):
	subs={t:os.listdir(join(save_dir,t)) for t in os.listdir(save_dir)}
	
	empty=[k for k,v in subs.items() if not v]
	errors=[k for k,v in subs.items() if 'error.txt' in v]
	valid=[k for k,v in subs.items() if 'en.json' in v and 'he.json' in v]
	
	print(f'found {len(valid)} valid dirs')
	print(f'found {len(empty)} empty dirs')
	print(f'found {len(errors)} errors dirs')

if __name__=="__main__":
	save_dir=join('data','pairs_dataset')
	#remove_empty(save_dir)
	#truncate_names(save_dir)
	check_data(save_dir)