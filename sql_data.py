import sqlite3 #build into python aperently
import json
import os
from os.path import join
from dataclasses import dataclass
from typing import List

import tempfile
from concurrent.futures import ThreadPoolExecutor
import threading

from tqdm import tqdm
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

    def get_all_folders(self):
        cur = self.get_connection().cursor()
        cur.execute('SELECT DISTINCT folder FROM main_data')
        folders = [row[0] for row in cur.fetchall()]
        return folders


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
                         folder TEXT, lang TEXT,
                         UNIQUE(folder, lang))''')
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

        

    def add_to_db(self, data_dir):
        full_data= [join(data_dir, p) for p in os.listdir(data_dir)]
        step=100
        # limit=step*100
        
        with ThreadPoolExecutor() as executor:
            for i in tqdm(range(0,len(full_data),step)):      
                # Process each folder in parallel and gather data
                results = list(executor.map(self._gather_data_for_insert,full_data[i:i+step]))
            
                # Flatten the list of results
                sub_data = [item for sublist in results for item in sublist]

                # Perform bulk insert
                self._bulk_insert(sub_data)
                # if(i==limit):
                #     break

        self.get_connection().close()

    def _gather_data_for_insert(self, path):
        data_for_insert = []
        for name in os.listdir(path):
            if name.endswith('.json'):
                folder = os.path.basename(path)
                lang = name[:-5]
                file_path = join(path, name)

                with open(file_path, 'r') as f:
                    data = json.load(f)
                page_data = PageData(**data)

                # Prepare data for bulk insert
                data_for_insert.append((folder, lang, page_data))

        return data_for_insert

    def _bulk_insert(self, all_data):
        conn = self.get_connection()
        cur = conn.cursor()

        try:
            # Create a temporary table for bulk insert
            cur.execute('CREATE TEMPORARY TABLE temp_main_data (folder TEXT, lang TEXT)')

            # Insert all folder and lang combinations into temporary table
            temp_data = [(folder, lang) for folder, lang, _ in all_data]
            cur.executemany('INSERT INTO temp_main_data (folder, lang) VALUES (?, ?)', temp_data)

            # Remove entries from temp_main_data that already exist in main_data
            cur.execute('''
                DELETE FROM temp_main_data
                WHERE EXISTS (
                    SELECT 1 FROM main_data
                    WHERE main_data.folder = temp_main_data.folder AND main_data.lang = temp_main_data.lang
                )
            ''')

            # Check if there are any records left in the temp_main_data
            cur.execute('SELECT COUNT(*) FROM temp_main_data')
            if cur.fetchone()[0] == 0:
                #print("No new data to insert.")
                return

            # Insert remaining new records from temp_main_data to main_data
            cur.execute('''
                INSERT INTO main_data (folder, lang)
                SELECT folder, lang FROM temp_main_data
            ''')

            # Create id_mapping dictionary from the temp_main_data
            cur.execute('''
                SELECT temp_main_data.folder, temp_main_data.lang, main_data.id 
                FROM temp_main_data
                JOIN main_data ON main_data.folder = temp_main_data.folder AND main_data.lang = temp_main_data.lang
            ''')
            id_mapping = {(folder, lang): main_id for folder, lang, main_id in cur.fetchall()}

            # Prepare and insert other data
            titles_data = []
            summaries_data = []
            sections_data = []
            texts_data = []

            for folder, lang, page_data in all_data:
                if (folder, lang) in id_mapping:
                    main_id = id_mapping[(folder, lang)]
                    titles_data.append((main_id, page_data.title))
                    summaries_data.append((main_id, page_data.summary))
                    sections_data.extend((main_id, i, section) for i, section in enumerate(page_data.sections))
                    texts_data.append((main_id, page_data.text))

            # Bulk insert using executemany for each table
            cur.executemany('INSERT INTO titles (id, title) VALUES (?, ?)', titles_data)
            cur.executemany('INSERT INTO summaries (id, summary) VALUES (?, ?)', summaries_data)
            cur.executemany('INSERT INTO sections (id, section_num, section) VALUES (?, ?, ?)', sections_data)
            cur.executemany('INSERT INTO texts (id, text) VALUES (?, ?)', texts_data)

            # Commit all changes at once
            conn.commit()

        except Exception as e:
            # Rollback in case of error
            conn.rollback()
            raise e

        finally:
            # Drop the temporary table and ensure the connection is closed properly
            cur.execute('DROP TABLE temp_main_data')
            cur.close()
        

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
    #test_retrieval_data_gen()
    
    database_path=join('data','wikisql.db')
    data_path=join('data','pairs_dataset')

    dataset_gen = WikiSql(database_path)
    print('adding')
    dataset_gen.add_to_db(data_path)


