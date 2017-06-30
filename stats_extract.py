import json
import urllib.request
import sqlite3

# some variable names
db_name = "NHLseasonML.db"


def scrape_data(database_name, table_list):
    # loop through the list, pulling the data into our DB
    for table_key in table_list:

        # make the web request to pull the json data
        req = urllib.request.Request(table_list[table_key])
        # now pretty it up
        data = json.loads(urllib.request.urlopen(req).read())

        # connect to our database that will hold everything, or create it if it doesn't exist
        conn = sqlite3.connect(database_name)

        # get the cursor so we can do stuff
        cur = conn.cursor()

        # get all columns from the json string
        column_list = ""
        for key in data['data'][0].keys():
            if isinstance(data['data'][0][key],float):
                datatype = "real"
            elif isinstance(data['data'][0][key],int):
                datatype = "integer"
            else:
                datatype = "text"
            column_list += key + " " + datatype + ','

        # Finish building the SQL query
        create_table = "CREATE table IF NOT EXISTS " + table_key + " (" + column_list + "unique(seasonId, playerId));"
        # create our table if it doesn't exist
        cur.execute(create_table)
        conn.commit()

        print("Processing " + table_key)

        # for each row in the data, do an insert/update for all the columns that are in the data
        for index in range(0, data['total']):
            insert_query = "insert or ignore into " + table_key + "("
            for key in data['data'][index].keys():
                insert_query += key + ", "
            # remove the trailing comma
            insert_query = insert_query[:-2]
            insert_query += ") values("
            for key in data['data'][index].keys():
                value = data['data'][index][key]
                # print(value)
                if isinstance(value, float):
                    insert_query += str(value) + ","
                elif isinstance(value, int):
                    insert_query += str(value) + ","
                elif value is None:
                    value = ""
                    insert_query += "'" + value + "',"
                else:
                    if value.find("'") != -1:
                        value = value[:value.find("'")] + "'" + value[value.find("'"):]
                    insert_query += "'" + value + "',"

            # remove the comma
            insert_query = insert_query[:-1]
            insert_query += ")"
            cur.execute(insert_query)
        # commit the changes
        conn.commit()

        # after it's all in the database, the tables can be joined and it can be easily exported to CSV
        conn.close()

    return


def create_annual_tables(start_season):
    hockey_season = str(start_season) + str(start_season + 1)
    # dictionary of data. Consists of a table name, and the corresponding JSON URL
    tables = {
        "s_skater_summary": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/skatersummary?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_sat_pct_5_on_5": "http://www.nhl.com/stats/rest/grouped/skaters/shooting/season/skaterpercentages?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_sat_5_on_5": "http://www.nhl.com/stats/rest/grouped/skaters/shooting/season/skatersummaryshooting?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_points_penalties_per_60": "http://www.nhl.com/stats/rest/grouped/skaters/core/season/skaterscoring?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_faceoffs_by_zone": "http://www.nhl.com/stats/rest/grouped/skaters/core/season/faceoffsbyzone?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_shots_by_type": "http://www.nhl.com/stats/rest/grouped/skaters/core/season/shottype?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_bio_info": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/bios?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_shootout": "http://www.nhl.com/stats/rest/grouped/skaters/shootout/season/skatershootout?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_time_on_ice": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/timeonice?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_penalties": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/penalties?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_realtime_events": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/realtime?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_penalty_kill": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/penaltykill?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_power_play": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/powerplay?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_faceoffs": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/faceoffs?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_points_by_strength": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/points?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "s_goals_by_strength": "http://www.nhl.com/stats/rest/grouped/skaters/basic/season/goals?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "g_goalie_summary": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/season/goaliesummary?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "g_saves_by_strength": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/season/goaliebystrength?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "g_goalie_shootout": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_shootout/season/goalieshootout?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2",
        "g_bio_info": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/season/goaliebios?cayenneExp=seasonId=" + hockey_season + "%20and%20gameTypeId=2"
    }

    return tables

# put the range of seasons we want to pull data for
for x in range(2009, 2016):
    #do the needful
    print("Scraping data from " + str(x) + "-" + str(x+1))
    scrape_data(db_name, create_annual_tables(x))

# export the data to csv files


print("Now I'm done")
