# wikipedia_search_dataset_creation
for the wikipedia api to work fast (ie 5000 instead of 500) u need an api key with an acount that has both languges

for data u can and probably should go to the hebrew wikipedia data dump and get it 
https://dumps.wikimedia.org/hewiki/20240101
I couldnt find a way to parse that dump thats identical to what the api would give me so I decided to just run the query to the api twice we still need the dump for the page titles make sure to take namespace 0 since these are the actual articles	"hewiki-20240101-all-titles-in-ns0.gz"
