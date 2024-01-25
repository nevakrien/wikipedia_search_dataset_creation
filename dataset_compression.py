import h5py #may get rid of it
import json
import os
from os.path import join
from concurrent.futures import ThreadPoolExecutor

from dataclasses import dataclass
from typing import List

import struct
import numpy as np
import tempfile

import sqlite3

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

def write_to_sqlite(conn, page_data):
    binary_data = page_data.to_binary()
    conn.execute('INSERT INTO page_data (data) VALUES (?)', (binary_data,))

def read_from_sqlite(conn, row_id):
    cursor = conn.execute('SELECT data FROM page_data WHERE id = ?', (row_id,))
    data = cursor.fetchone()[0]
    return PageData.from_binary(data)

def test_sqlite_read_write():
    # Create a temporary file
    with tempfile.NamedTemporaryFile() as temp_file:
        file_path = temp_file.name

        # Setup SQLite database
        conn = sqlite3.connect(file_path)
        conn.execute('CREATE TABLE page_data (id INTEGER PRIMARY KEY, data BLOB)')

        # Sample PageData object
        page_data_sample = PageData("Title", "Summary", ["Section1", "Section2"], "Text")

        # Write data to SQLite
        write_to_sqlite(conn, page_data_sample)
        conn.commit()

        # Read data from SQLite
        retrieved_data = read_from_sqlite(conn, 1)

        # Test the integrity of the data
        assert page_data_sample == retrieved_data
        print("Test passed! Data integrity maintained.")

        # Close the database connection
        conn.close()

if __name__=="__main__":
	with open(join('data','pairs_dataset','תל_אבו_הוריירה','en.json')) as f:
		d=json.load(f)

	#test class integrity
	p=PageData(**d)
	assert p.__dict__==d
	print('class matches save data')

	#test binary encoding
	b=p.to_binary()
	assert p.__dict__==PageData.from_binary(b).__dict__
	print('binary encding works')

	test_sqlite_read_write()
	