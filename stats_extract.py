import json
import urllib.request
import sqlite

sample_url = "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/skatersummary?cayenneExp=seasonId=20152016%20and%20gameTypeId=2"

req = urllib.request.Request(sample_url)

data = json.loads(urllib.request.urlopen(req).read())
# data['data'][0]['playerId']

for index in range(0,data['total']):
    print("Player ID: ", data['data'][index]['playerId']," Player Name: ",data['data'][index]['playerName'])

print("Now I'm done")
