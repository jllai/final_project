import os
import unittest
import itertools
import collections
import json
import sqlite3
import gspread
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
		# global CACHE_DICTION
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
		# global CACHE_DICTION
		return CACHE_DICTION
	else:
		print("fetching")
		try:
			#opens the sheet
			all_lst = client2.open("Existing Data").sheet1
			all_names = all_lst.get_all_records()
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

i = 0
for ppl_info in all_users:
	if ppl_info['Primary Email Address'] in new_user[i]:
		i += 1
		continue














