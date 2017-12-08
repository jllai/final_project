import os
import unittest
import itertools
import collections
import json
import sqlite3
import gspread
import yelp_info
from oauth2client.service_account import ServiceAccountCredentials
from yelpapi import YelpAPI

### AUTHENTICATION: authorizes the two google sheet, the yelp api, and insta
### 
###

# Google sheets
scope = ['https://spreadsheets.google.com/feeds']
creds1 = ServiceAccountCredentials.from_json_keyfile_name('DNC_voters.json', scope)
creds2 = ServiceAccountCredentials.from_json_keyfile_name('DNC_lookup.json', scope)
client1 = gspread.authorize(creds1)
client2 = gspread.authorize(creds2)

# Yelp api
yelp_api = YelpAPI(yelp_info.client_id, yelp_info.client_secret)


### FUNCTIONS: these are all caches for the yelp and google sheets
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

	# Define your function get_user_tweets here:
	if os.path.isfile('dnc.json'):
		print("using cache")
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			#opens the sheet
			name_lst = client1.open("File to de-duplicate").sheet1
			#pulls everything from the sheet and puts it into a list of dictionaries
			name_check = name_lst.get_all_records()
			CACHE_DICTION = name_check
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
		print("using cache")
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			#opens the sheet
			all_lst = client2.open("Existing Data").sheet1
			all_names = all_lst.col_values(9)
			CACHE_DICTION = all_names
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			return CACHE_DICTION
		except:
			print("Wasn't in cache and wasn't valid search either")
			return None
# Caches the top 3 restaurants in the area that the user lives in
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
			request = yelp_api.search_query(term='restaurant', location=loc, sort_by='rating', limit=3, radius=40000)
			CACHE_DICTION[loc] = request
			dumped_json_cache = json.dumps(CACHE_DICTION)
			fw = open(CACHE_NEW,"w")
			fw.write(dumped_json_cache)
			fw.close() # Close the open file
			return CACHE_DICTION[loc]
		except:
			return None


### SETUP: This area is to create the sqlite databases Users, which stores 
### all the new users, and Yelp, which stores the City and ratings of the
### top 3 restaurants in the city
###

conn = sqlite3.connect('data.sqlite')
conn = sqlite3.connect('yelp.sqlite')
cur = conn.cursor()

# Drops the Users table if it exists, and creates the table 
cur.execute('DROP TABLE IF EXISTS Users')
cur.execute('''
CREATE TABLE Users (first_name TEXT, last_name TEXT, address TEXT, city TEXT, state TEXT, zip_code INTEGER, email  TEXT)''')


cur.execute('DROP TABLE IF EXISTS Yelp')
cur.execute('''
CREATE TABLE Yelp (city TEXT, name TEXT PRIMARY KEY, loc TEXT, rating INTEGER, price TEXT)''')


### EXECUTION: calls the caches, compares the two sheets to find the users
### not in the all_users sheet, finds the top 3 restaurants in each area the
### person lives, and stores them in the databases

new_users = get_new_users_info()
all_users = get_all_users_info()

yelp = []

# Lowers the characters in the list to compare properly
for i in range(len(all_users)):
	all_users[i] = all_users[i].lower()

for person in new_users:
	# if new person then add to the sqlite database
	if person["Primary Email Address"].lower() not in all_users:
		tup = person['First Name'], person["Last Name"], person['Primary Address 1'], person['Primary City'], person['Primary State'], person["Primary Zip"], person["Primary Email Address"]
		cur.execute('INSERT INTO Users (first_name, last_name, address, city, state, zip_code, email) VALUES (?, ?, ?, ?, ?, ?, ?)', (tup))
		
		if person['Primary City'] != '':
			yelp.append(get_yelp_results(person['Primary City']))

for city in yelp:
	for restaurant in city['businesses']:
		tup = restaurant['location']['city'], restaurant["name"], restaurant['location']['address1'], restaurant['rating'], restaurant['price']
		dup = restaurant['name'],
		cur.execute('SELECT name FROM Yelp WHERE name = (?)', (dup))
		dupled = cur.fetchone()
		if dupled != dup:
			cur.execute('INSERT INTO Yelp (city, name, loc, rating, price) VALUES (?, ?, ?, ?, ?)', (tup))

conn.commit()
cur.close()








