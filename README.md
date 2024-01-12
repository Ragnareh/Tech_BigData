# Tech_BigData

Comments:

- The first iteration and simple way to complete the task - is to read and write the result (LogicTest ->  test: Playground: local parses, commented out)

- The second iteration - is to read the files using standartized extractor and case classes to make the code more flexible and extendable. 
	This will allow to extend the input files locations, extension and add some casts if the data in not cleanced well in advance. 
	Example: files are in different formats - csv,parquet,fixed; files has different encodings

- After	some SQLs I decided to create some additional output datasets: domains + countries and the desired dataset: Category, Address(country, region...), Phone, Company names

- Solution for similar data:
	- manual check shows the better candidate for the most full adresses from google. I'm thinking that the addresses are already cleanced there and some mapping systems are used already (like Trillium)
	- we can use frequence for every adress
	- we can use some similarity analysis (picked that one)
	- we can use some ML methods here

- On data conflicts
	- we can rely on the importance/fullness of the proposed datasets (1 - google,2 - facebook, 3 - website. Why? Manual check )	
	- we can rely on some unified weight based on similarity analysis

- Data amount and processing time
	- not relevant for the proposed datasets
	- if the data is static and reloaded by schedule (+ full datasets arrive every time) - we can simpy reprocess everything 
	- if we should operate with the streaming data - we sould use different approach in data retrival and operate with delta and windowed pieces (can be asyncronous) - and that is the different task and story
	
- Some DQC are also included in the code (some diff remained unexplained)
- The code may be enriched by the configs (if this task is repetable), but this will be dependent on the architecture used
- It's not easy to complete the task - the desisions depends on the time, budget and boundaries
- It's also possible to go standard DWH way
- put the datasets in /src/test/resources as it was provided and the test will be runnable and will generate the output in /target/testoutput

Initial Planning and Estimations

1. Read the datasets, fix the issues (formats, encoding, casts, etc)
2. Make output (joins) - single dataset:  Category, Address(country, region...), Phone, Company names
3. Potential join columns (mix, subsets and functions): 
	domain,categories,city,country_code,country_name,phone,region_code,zip_code
	domain,category,city,country_code,country_name,phone,region_code,zip_code
	root_domain;domain_suffix;s_category;main_city;main_country;main_region;phone
4. Propose Mapping/Etalon table for the merging Addresses/Companies
	-> solution for similar data
	-> THINK: mapping systems? ml? opensource datasets
5. On data conflicts - which to keep
	-> THINK: density, remapping, keep all,manual clarification
6. Data amount and processing time
	-> THINK: check performance, split to chunks, preprocess the data
	
Estimations:
	- rought check the datasets and write working plan: 1h (done)
	- POC: 2-8h
	- additional tools setup (ex: configuring Spark, setting some exploratory tools - currently don't know which to choose): 2-8h
	- iterative coding: 2-16h
	
Real time spent:
- Monday 4h (no configuring Spark)
- Tuesday 4h (illness of relatives stolen and attension)
- Wednesday 4h (coding, reading, experimenting)
- Thursday 8h (coding, stabilizying)
- Friday 5h (coding, stabilizying)