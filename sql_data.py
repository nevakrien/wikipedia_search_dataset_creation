import sqlite3 #build into python aperently
import json
import os
from os.path import join
from dataclasses import dataclass
from typing import List

import tempfile
from concurrent.futures import ThreadPoolExecutor
import threading

@dataclass
class PageData:
    title: str
    summary: str
    sections: List[str]
    text: str

class WikiSql():
    def __init__(self,db_path):
        #self.data_dir = data_dir
        self.db_path = db_path
        self.connections={}
        self.make_db()

    def get_connection(self):
        thread_id = threading.get_ident()
        if thread_id not in self.connections:
            self.connections[thread_id] = sqlite3.connect(self.db_path)
        return self.connections[thread_id]

    def make_db(self):
        conn = sqlite3.connect(self.db_path)
        print('initilizing database')

        cur=conn.cursor()#with  as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS main_data 
                        (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                         folder TEXT, lang TEXT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS titles 
                        (id INTEGER, title TEXT, 
                         FOREIGN KEY(id) REFERENCES main_data(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS summaries 
                        (id INTEGER, summary TEXT, 
                         FOREIGN KEY(id) REFERENCES main_data(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sections 
                        (id INTEGER, section_num INTEGER, section TEXT, 
                         FOREIGN KEY(id) REFERENCES main_data(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS texts 
                        (id INTEGER, text TEXT, 
                         FOREIGN KEY(id) REFERENCES main_data(id))''')
        conn.commit()
        conn.close()

        

    def add_to_db(self,data_dir):
        with ThreadPoolExecutor() as ex:
            #print([type(x) for x in dir(ex._threads)])
            #print([x for x in ex._work_queue])
            list(ex.map(self._process_folder,[join(data_dir,p) for p in os.listdir(data_dir)]))
        
        # for c in self.connections.values():
        #     c.commit()
        #     c.close()
        self.connections={}
        
    def _process_folder(self,path):
        #cur=get_connection().cursor()
        for name in os.listdir(path):
                if name.endswith('.json'):
                    self._process_file(os.path.basename(path), name[:-5], os.path.join(path, name))

    def _process_file(self, folder, lang, file_path):
        conn=self.get_connection()
        cur=conn.cursor()

        with open(file_path, 'r') as f:
            data = json.load(f)
        page_data = PageData(**data)

        cur.execute('INSERT INTO main_data (folder, lang) VALUES (?, ?)', (folder, lang))
        main_id = cur.execute('SELECT last_insert_rowid()').fetchone()[0]

        cur.execute('INSERT INTO titles (id, title) VALUES (?, ?)', (main_id, page_data.title))
        cur.execute('INSERT INTO summaries (id, summary) VALUES (?, ?)', (main_id, page_data.summary))
        for i, section in enumerate(page_data.sections):
            cur.execute('INSERT INTO sections (id, section_num, section) VALUES (?, ?, ?)', 
                         (main_id, i, section))
        cur.execute('INSERT INTO texts (id, text) VALUES (?, ?)', (main_id, page_data.text))
        conn.commit()

    def load_data(self, folder, lang):
        cur = self.get_connection().cursor()
        main_id = cur.execute('SELECT id FROM main_data WHERE folder = ? AND lang = ?', 
                               (folder, lang)).fetchone()
        if main_id:
            main_id = main_id[0]
            title = cur.execute('SELECT title FROM titles WHERE id = ?', (main_id,)).fetchone()[0]
            summary = cur.execute('SELECT summary FROM summaries WHERE id = ?', (main_id,)).fetchone()[0]
            sections = [row[0] for row in cur.execute('SELECT section FROM sections WHERE id = ? ORDER BY section_num', (main_id,))]
            text = cur.execute('SELECT text FROM texts WHERE id = ?', (main_id,)).fetchone()[0]
            #conn.close()
            return PageData(title, summary, sections, text)
        #conn.close()
        return None

def test_retrieval_data_gen():
    # Create a temporary SQLite database file
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db_file,tempfile.TemporaryDirectory() as tmpdirname:
        db_path = temp_db_file.name

        # Sample PageData for testing
        sample_page_data = PageData(
            title="Test Title",
            summary="Test Summary",
            sections=["Section 1", "Section 2"],
            text="Test Text"
        )

        # Initialize WikiSql with the temporary database
        #conn = sqlite3.connect(temp_db_file.name)
        dataset_gen = WikiSql(temp_db_file.name)
        #dataset_gen.populate_db

        # Mock folder and language for the test
        test_folder = 'test_folder'
        test_lang = 'en'
        os.mkdir(join(tmpdirname,test_folder))
        os.listdir(tmpdirname)

        with open(join(tmpdirname,test_folder,test_lang+'.json'),'w') as f:
            json.dump(sample_page_data.__dict__,f)

        print('Adding data')
        dataset_gen.add_to_db(tmpdirname)

        print('Retrieving data')
        retrieved_data = dataset_gen.load_data(os.path.basename(test_folder), test_lang)
        
        
        #print(temp_db_file.read())        

        # Validate retrieved data
        #print(retrieved_data)
        #print(sample_page_data)
        assert retrieved_data == sample_page_data, "Retrieved data does not match the sample data"
        print("Test passed! Retrieved data matches the sample data.")

# Run the test
#test_retrieval_data_gen()

if __name__ == "__main__":
    test_retrieval_data_gen()
    # dataset_gen = WikiSql('path/to/data', 'page_data.db')
    # data_entry = dataset_gen.load_data('folder_name', 'lang')
    # print(data_entry)