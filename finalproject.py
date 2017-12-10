import os
import json
import sqlite3
import gspread
import api_info
from oauth2client.service_account import ServiceAccountCredentials
from yelpapi import YelpAPI
import facebook

### AUTHENTICATION: Authorizes the two google sheets, the yelp api, and
### the Facebook api
###

# Google sheets API
scope = ['https://spreadsheets.google.com/feeds']
creds1 = ServiceAccountCredentials.from_json_keyfile_name('DNC_voters.json', scope)
creds2 = ServiceAccountCredentials.from_json_keyfile_name('DNC_lookup.json', scope)
client1 = gspread.authorize(creds1)
client2 = gspread.authorize(creds2)
# Yelp api
yelp_api = YelpAPI(api_info.yelp_client_id, api_info.yelp_client_secret)
# Facebook  API 
access_token = api_info.graph_access_token
graph = facebook.GraphAPI(access_token)

### FUNCTIONS: these are all caches for google sheets, yelp, and facebook
### 
###

# Used to access and cache the DNC_voters sheet into dnc.json
def get_new_users_info():
	CACHE_NEW = "dnc.json"

	try:
	    cache_file = open(CACHE_NEW, 'r') # Try to read the data from the file
	    cache_contents = cache_file.read()  # If it's there, get it into a string
	    CACHE_DICTION = json.loads(cache_contents) # And then load it into a dictionary
	    cache_file.close() # Close the file, we're good, we got the data in a dictionary.
	except:
		CACHE_DICTION = {}

	if os.path.isfile('dnc.json'):
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			name_lst = client1.open("File to de-duplicate").sheet1 # Opens the sheet
			name_check = name_lst.get_all_records() # Pulls out all records in list of dictionary
			CACHE_DICTION = name_check # Caching...
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			name_lst.close()
			return CACHE_DICTION
		except:
			print("Wasn't in cache and wasn't valid search either")
			return None
# This function is used to cache the DNC_lookup sheet
def get_all_users_info():
	CACHE_NEW = "all_users.json"

	try:
	    cache_file = open(CACHE_NEW, 'r') # Try to read the data from the file
	    cache_contents = cache_file.read()  # If it's there, get it into a string
	    CACHE_DICTION = json.loads(cache_contents) # And then load it into a dictionary
	    cache_file.close() # Close the file, we're good, we got the data in a dictionary.
	except:
		CACHE_DICTION = {}

	# Define your function get_user_tweets here:
	if os.path.isfile('all_users.json'):
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			all_lst = client2.open("Existing Data").sheet1 # Opens the sheet
			all_names = all_lst.col_values(9) # Creates list of emails
			CACHE_DICTION = all_names # Caching...
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			return CACHE_DICTION
		except:
			print("Wasn't in cache and wasn't valid search either")
			return None
# Caches the top 3 restaurants in the city that the user lives in
def get_yelp_results(loc):
	CACHE_NEW = "yelp.json"
	#Obtain these from Yelp's manage access page
	try:
		cache_file = open(CACHE_NEW, 'r') # Try to read the data from the file
		cache_contents = cache_file.read()  # If it's there, get it into a string
		CACHE_DICTION = json.loads(cache_contents) # And then load it into a dictionary
		cache_file.close() # Close the file, we're good, we got the data in a dictionary.
	except:
		CACHE_DICTION = {}

	if loc in CACHE_DICTION:
		return CACHE_DICTION[loc]
	else:
		try:
			request = yelp_api.search_query(term='restaurant', location=loc, sort_by='rating', limit=3, radius=40000) # Searches for top three restaurants in the area
			CACHE_DICTION[loc] = request # Caching...
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			return CACHE_DICTION[loc]
		except:
			return None
# Caches the instagram info of the users
def get_facebook_results():
	CACHE_NEW = "facebook.json"

	try:
	    cache_file = open(CACHE_NEW, 'r') 
	    cache_contents = cache_file.read()  
	    CACHE_DICTION = json.loads(cache_contents)
	    cache_file.close() 
	except:
		CACHE_DICTION = {}

	# Define your function get_user_tweets here:
	if os.path.isfile('facebook.json'):
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			posts = graph.get_connections(id = 'me', connection_name = 'posts') # Pulls out top 20 recent posts
			CACHE_DICTION = posts # Caching...
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			return CACHE_DICTION
		except:
			print("Wasn't in cache and wasn't valid search either")
			return None


### SETUP: This area is to create the sqlite databases Users, which stores 
### all the new users, and Yelp, which stores the City and ratings of the
### top 3 restaurants in the city

conn = sqlite3.connect('data.sqlite')
conn = sqlite3.connect('yelp.sqlite')
connn = sqlite3.connect('facebook.sqlite')
cur = conn.cursor()
# Drops the Users table if it exists, and creates the table 
cur.execute('DROP TABLE IF EXISTS Users')
cur.execute('''
CREATE TABLE Users (first_name TEXT, last_name TEXT, address TEXT, city TEXT, state TEXT, zip_code INTEGER, email  TEXT)''')
# Drops Yelp table if exists, creates table and attributes
cur.execute('DROP TABLE IF EXISTS Yelp')
cur.execute('''
CREATE TABLE Yelp (city TEXT, name TEXT PRIMARY KEY, loc TEXT, rating INTEGER, price TEXT)''')
# Drops Facebook table if exists, creates table and attributes
cur.execute('DROP TABLE IF EXISTS Facebook')
cur.execute('''
CREATE TABLE Facebook (id TEXT PRIMARY KEY, story TEXT, message TEXT, created_at TIMESTAMP)''')


### EXECUTION: Calls the caches, compares the two sheets to find the users
### not in the all_users sheet, finds the top 3 restaurants in each area the
### person lives, and stores them in the databases

new_users = get_new_users_info()
all_users = get_all_users_info()
# Creates lists for yelp
yelp = []
# Lowers the characters in the list to compare properly
for i in range(len(all_users)):
	all_users[i] = all_users[i].lower()
# For finding the new users and using yelp function
for person in new_users:
	# If new person then add all ino to the sqlite database
	if person["Primary Email Address"].lower() not in all_users:
		tup = person['First Name'], person["Last Name"], person['Primary Address 1'], person['Primary City'], person['Primary State'], person["Primary Zip"], person["Primary Email Address"]
		cur.execute('INSERT INTO Users (first_name, last_name, address, city, state, zip_code, email) VALUES (?, ?, ?, ?, ?, ?, ?)', (tup))
		
		# If city cell is empty, doesn't add yelp results to the list
		if person['Primary City'] != '':
			yelp.append(get_yelp_results(person['Primary City']))
# Adds city, name, location, rating, and price to the Yelp sqlite
for city in yelp:
	for restaurant in city['businesses']:
		tup = restaurant['location']['city'], restaurant["name"], restaurant['location']['address1'], restaurant['rating'], restaurant['price']
		# Checks if restaurant is in database already. If not, inserts in
		dup = restaurant['name'],
		cur.execute('SELECT name FROM Yelp WHERE name = (?)', (dup))
		dupled = cur.fetchone()
		if dupled != dup:
			# Actual insertion
			cur.execute('INSERT INTO Yelp (city, name, loc, rating, price) VALUES (?, ?, ?, ?, ?)', (tup))

# SEPARATE: return my posts with facebook function
facebook = get_facebook_results()
# Puts results in facebook.sqlite
for info in facebook['data']:
	if 'message' and 'story' not in info:
		tup = info['id'], info['created_time']
		cur.execute('INSERT INTO Facebook (id, created_at) VALUES (?, ?)', (tup))
	elif 'message' not in info:
		tup = info['id'], info['story'], info['created_time']
		cur.execute('INSERT INTO Facebook (id, story, created_at) VALUES (?, ?, ?)', (tup))
	elif 'story' not in info:
		tup = info['id'], info['message'], info['created_time']
		cur.execute('INSERT INTO Facebook (id, message, created_at) VALUES (?, ?, ?)', (tup))
	else:
		tup = info['id'], info['story'], info['message'], info['created_time']
		cur.execute('INSERT INTO Facebook (id, story, message, created_at) VALUES (?, ?, ?, ?)', (tup))



### OUTPUT: These select from each sqlite based on a condition and prints
### them
###

# Google sheets
cur.execute('SELECT first_name, last_name FROM Users WHERE state = "MI"')
mi_residence = cur.fetchall()
print('\n --- These people live in Michigan ---' + '\n')
# Prints it prettily
for tup in mi_residence:
	print(' '.join(tup))
print('\n')

# Yelp
cur.execute('SELECT name FROM Yelp WHERE price = "$"')
cheap = cur.fetchall()
print("--- These are the cheap eats in the selected cities ---" + '\n')
# Prints it prettily
for tup in cheap:
	for name in tup:
		print(name)
print('\n')

# Facebook
cur.execute('SELECT created_at FROM Facebook')
created = cur.fetchall()
print("--- I've been active on Facebook on these days --- \n")
#Splice the date out of the timestamp
for tup in created:
	timestamp = ''.join(tup)
	print(timestamp[:10])
print('\n')


conn.commit()
cur.close()