import os
import unittest
import itertools
import collections
import json
import sqlite3
import gspread
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict

#authenticates the two sheets
scope = ['https://spreadsheets.google.com/feeds']
creds1 = ServiceAccountCredentials.from_json_keyfile_name('DNC_voters.json', scope)
creds2 = ServiceAccountCredentials.from_json_keyfile_name('DNC_lookup.json', scope)
client1 = gspread.authorize(creds1)
client2 = gspread.authorize(creds2)

# This function is used to access and cache the DNC_voters sheet into dnc.json
def get_new_user_info():
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
			name_check = name_lst.col_values(8)
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

new_user = get_new_user_info()

def get_all_user_info():
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

all_users = get_all_user_info()

for i in range(len(all_users)):
	all_users[i] = all_users[i].lower()

name_lst = client1.open("File to de-duplicate").sheet1

for ppl in new_user[1:]:
	ppl = ppl.lower()
	if ppl in all_users:
		name_lst.delete_row(new_user.index(ppl) + 1)

name_lst.close()





