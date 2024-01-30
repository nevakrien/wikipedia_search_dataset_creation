# wikipedia_search_dataset_creation
this project works on making a paired dataset of english and hebrew wikipedia articles.
we will then generate query answer pairs for both dataset with the goal of training ml models on them.

our goal is to not only have a hebrew search bot but compare it to english search bot in the same domain
however this dataset is not really ment for validation for validation look at https://github.com/nevakrien/search_assesment.git

# technical notes
for the wikipedia api to work fast (ie 5000 instead of 500) u need an api key with an acount that has both languges
note that the listed limits are lower then the practical limits this took on my machine around 10x faster then the maximum limits suggest

for data u can and probably should go to the hebrew wikipedia data dump and get it 
https://dumps.wikimedia.org/hewiki/20240101
I couldnt find a way to parse that dump thats identical to what the api would give me so I decided to just run the query to the api twice we still need the dump for the page titles make sure to take namespace 0 since these are the actual articles	"hewiki-20240101-all-titles-in-ns0.gz"


we gave 2 dbs because sqlite is kind of anoying to work with while using multithreading but it does better data encoding and is supported by the python runtime for python 3 so its the better choice for the runing on compute nodes.
if u are runing locally u may be able to avoid that step	

# usage
just run main.py 
note that the program will crash and recover a lot because of api limits this is in no way perfect but it does seem to work so thats what we are going with

validation.py checks that the retrived data is good.

