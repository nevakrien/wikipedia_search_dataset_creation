import h5py #may get rid of it
import json
import os
from os.path import join
from concurrent.futures import ThreadPoolExecutor

from dataclasses import dataclass
from typing import List

import struct

@dataclass
class PageData:
    title: str
    summary: str
    sections: List[str]
    text: str

    def to_binary(self):
        # Encode the three initial strings without storing their lengths
        ans = self.title.encode('utf-8') + b'\x00'
        ans += self.summary.encode('utf-8') + b'\x00'
        ans += self.text.encode('utf-8') + b'\x00'

        # Encode the sections with their lengths
        for section in self.sections:
            ans += section.encode('utf-8') + b'\x00'

        return ans

    @staticmethod
    def from_binary(data):
        offset = 0

        # Helper function to read a null-terminated string
        def read_string():
            nonlocal offset
            string_data = b''
            while data[offset] != 0:
                string_data += data[offset:offset + 1]
                offset += 1
            offset += 1  # Skip the null terminator
            return string_data.decode('utf-8')

        title = read_string()
        summary = read_string()
        text = read_string()

        sections = []
        while offset < len(data):
            section = read_string()
            sections.append(section)

        return PageData(title, summary, sections, text)



if __name__=="__main__":
	with open(join('data','pairs_dataset','תל_אבו_הוריירה','en.json')) as f:
		d=json.load(f)

	#test class integrity
	p=PageData(**d)
	assert p.__dict__==d

	#test binary encoding
	b=p.to_binary()
	assert p.__dict__==PageData.from_binary(b).__dict__

	maklen=len(os.listdir(join('data','pairs_dataset')))
	# # Create a file and dataset
	# with h5py.File('variable_length_strings.h5', 'w') as f:
	#     # Create a special dtype for variable-length strings
	#     dt = h5py.special_dtype(vlen=str)
	    
	#     # Initialize the dataset with the special dtype
	#     # The shape is (max_entries, 1) because each entry is a list of strings
	#     dset = f.create_dataset('string_lists',dtype=dt, maxshape=(maklen, 4))
