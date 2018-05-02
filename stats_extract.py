import json
import urllib.request
import sqlite3
import datetime
import time

# some variable names
db_name = "NHLseasonML_seasonstats.db"

# seasons lookup. format for date range is YYYY-MM-DD,YYYY-MM-DD
seasons_dict = {
    "20172018": "2017-10-04,2018-04-08",
    "20162017": "2016-10-12,2017-04-09",
    "20152016": "2015-10-07,2016-04-10",
    "20142015": "2014-10-08,2015-04-11",
    "20132014": "2013-10-01,2014-04-13",
    "20122013": "2013-01-19,2013-04-28",
    "20112012": "2011-10-06,2012-04-07",
    "20102011": "2010-10-07,2011-04-10",
    "20092010": "2009-10-01,2010-04-11",
    "20082009": "2008-10-04,2009-04-12",
    "20072008": "2007-09-29,2008-04-06",
    "20062007": "2006-10-04,2007-04-08",
    "20052006": "2005-10-05,2006-04-18"
}

test_seasons_dict = {
    "20172018": "2017-10-04,2017-11-07"
}


def scrape_data(database_name, table_list, table_type):
    # loop through the list, pulling the data into our DB
    for table_key in table_list:
        # make the web request to pull the json data. Bare minimum error checking
        req = urllib.request.Request(table_list[table_key])
        try:
            res = urllib.request.urlopen(req).read()
        except Exception as e:
            print("Error requesting: " + table_list[table_key])
            print(e)
            continue

        # now pretty it up.
        data = json.loads(res.decode('utf-8'))

        # make sure we have actual results, otherwise skip this table for this time period
        if data['total'] >= 1:

            # connect to our database that will hold everything, or create it if it doesn't exist
            conn = sqlite3.connect(database_name)

            # get the cursor so we can do stuff
            cur = conn.cursor()

            # get all columns from the json string
            column_list = ""
            for key in data['data'][0].keys():
                if isinstance(data['data'][0][key],int):
                    datatype = "integer"
                elif isinstance(data['data'][0][key],float):
                    datatype = "real"
                else:
                    datatype = "text"
                column_list += key + " " + datatype + ','

            # Finish building the SQL query
            if table_type == "season":
                create_table = "CREATE table IF NOT EXISTS " + table_key + " (" + column_list + "unique(seasonId, playerId));"
            else:
                create_table = "CREATE table IF NOT EXISTS " + table_key + " (" + column_list + "unique(gameId, playerId));"

            print("Processing " + table_key)

            # create our table if it doesn't exist
            cur.execute(create_table)
            conn.commit()


            # use a list to store each row that will be written
            biglist = []
            insert_query2 = "insert or ignore into " + table_key + " values("

            for x in range(0, data['data'][0].__len__()):
                insert_query2 += "?,"
            insert_query2 = insert_query2[:-1]
            insert_query2 += ")"

            # for each row in the data, do an insert/update for all the columns that are in the data
            for index in range(0, data['total']):
                # store the values to be inserted in each row as a list
                player_data_list = []

                # make sure each column in the row is valid and modify it if required
                for key in data['data'][index].keys():
                    value = data['data'][index][key]
                    # print(value)
                    if isinstance(value, float):
                        player_data_list.append(value)
                    elif isinstance(value, int):
                        player_data_list.append(value)
                    # deal with null or blank values
                    elif value is None:
                        value = ""
                        player_data_list.append(value)
                    # the apostrophe in names causes havoc. eg Ryan O'Reilly.
                    else:
                        if value.find("'") != -1:
                            value = value[:value.find("'")] + "'" + value[value.find("'"):]
                        player_data_list.append(value)

                # add this row of data to the list we'll insert
                biglist.append(player_data_list)

            # commit the changes
            # Sometimes the database stays locked for a fraction of a second after the previous commit was done
            #  so a loop was added here to make it wait, then re-try the query. The amount of time to wait is
            #  proportional to how many records we're trying to insert
            while True:
                try:
                    #insert / ignore all the rows to the table and commit the changes
                    cur.executemany(insert_query2, biglist)
                    conn.commit()
                    print(str(data['total']) + " rows committed")
                    break
                except sqlite3.OperationalError:
                    # catch the error thrown by sqlite, wait a bit, then re-connect so the loop can start over
                    print("Error, DB still locked. Trying again...")
                    time.sleep(data['total'] / 400)
                    cur.close()
                    conn.close()
                    conn = sqlite3.connect(database_name)
                    cur = conn.cursor()

            # after it's all in the database, we close the connections
            cur.close()
            conn.close()

        # no data available, this would only happen if there were no games on this day.
        else:
            print("No data for " + table_key)

    return


def create_annual_tables(hockey_season):
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


def create_annual_tables_nhldotcom2018(hockey_season):
    # dictionary of data. Consists of a table name, and the corresponding JSON URL
    # skater franchise totals is not useful at all so its being left out
    tables = {
        "s_skater_summary": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skatersummary&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_skater_points": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skaterpoints&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_skater_goals": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skatergoals&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_faceoffs": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=faceoffs&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_power_play": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skaterpowerplay&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_penalty_kill": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skaterpenaltykill&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_realtime_events": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=realtime&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_penalties": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=penalties&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_time_on_ice": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=timeonice&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_shootout": "http://www.nhl.com/stats/rest/skaters?reportType=shootout&reportName=skatershootout&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_plusminus": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=plusminus&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_bio_info": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=bios&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_sat_5_on_5": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skatersummaryshooting&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_sat_pct_5_on_5": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skaterpercentages&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_points_penalties_per_60": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=skaterscoring&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_faceoffs_by_zone": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=faceoffsbyzone&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "s_shots_by_type": "http://www.nhl.com/stats/rest/skaters?&reportType=basic&reportName=shottype&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "g_goalie_summary": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&reportName=goaliesummary&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "g_saves_by_strength": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&reportName=goaliebystrength&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "g_goalie_shootout": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_shootout&reportName=goalieshootout&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season,
        "g_bio_info": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&reportName=goaliebios&cayenneExp=gameTypeId=2%20and%20seasonId=" + hockey_season
    }

    return tables


def create_daily_tables(day):
    this_day_start = day.strftime("%Y-%m-%d")
    this_day_end = (day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # same as annual tables. JSON URL, and the key corresponds to the SQL table name that will be created/updated
    tables = {
        "s_daily_skater_summary":"http://www.nhl.com/stats/rest/grouped/skaters/basic/game/skatersummary?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_goals": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/goals?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_points": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/points?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_faceoffs": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/faceoffs?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_powerplay": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/powerplay?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_penaltykill": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/penaltykill?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_realtime": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/realtime?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_penalties": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/penalties?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_timeonice": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/timeonice?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_shootout": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/skatershootout?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_SATpctg": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/skaterpercentages?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_SAT5v5": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/skatersummaryshooting?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_points_penalties_per60": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/skaterscoring?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_faceoffs_by_zone": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/faceoffsbyzone?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "s_daily_skater_shots_by_type": "http://www.nhl.com/stats/rest/grouped/skaters/basic/game/shottype?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2",
        "g_daily_goalie_summary": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/game/goaliesummary?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2%20",
        "g_daily_goalie_by_strength": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/game/goaliebystrength?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2%20",
        "g_daily_goalie_shootout": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/game/goalieshootout?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2%20",
        "g_daily_goalie_daysrest": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/game/goaliedaysrest?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2%20",
        "g_daily_goalie_penaltyshots": "http://www.nhl.com/stats/rest/grouped/goalies/goalie_basic/game/goaliepenaltyshots?cayenneExp=gameDate%3E=%27" + this_day_start + "T06:00:00.000Z%27%20and%20gameDate%3C=%27" + this_day_end + "T05:59:59.999Z%27%20and%20gameTypeId=2%20"
    }
    return tables

def create_daily_tables_nhldotcom2018(day):
    this_day_start = day.strftime("%Y-%m-%d")
    this_day_end = (day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # same as annual tables. JSON URL, and the key corresponds to the SQL table name that will be created/updated
    cayenne = "&cayenneExp=gameDate%3E=%22" + this_day_start + "%22%20and%20gameDate%3C=%22" + this_day_end + "%22%20and%20gameTypeId=2"
    tables = {
        "s_daily_skater_summary":"http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=skatersummary" + cayenne,
        "s_daily_skater_goals": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=skatergoals" + cayenne,
        "s_daily_skater_points": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=skaterpoints" + cayenne,
        "s_daily_skater_faceoffs": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=faceoffs" + cayenne,
        "s_daily_skater_powerplay": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=skaterpowerplay" + cayenne,
        "s_daily_skater_penaltykill": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=skaterpenaltykill" + cayenne,
        "s_daily_skater_realtime": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=realtime" + cayenne,
        "s_daily_skater_penalties": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=penalties" + cayenne,
        "s_daily_skater_timeonice": "http://www.nhl.com/stats/rest/skaters?reportType=basic&isGame=true&reportName=timeonice" + cayenne,
        "s_daily_skater_shootout": "http://www.nhl.com/stats/rest/skaters?reportType=shootout&isGame=true&reportName=skatershootout" + cayenne,
        "s_daily_skater_SATpctg": "http://www.nhl.com/stats/rest/skaters?reportType=shooting&isGame=true&reportName=skaterpercentages" + cayenne,
        "s_daily_skater_SAT5v5": "http://www.nhl.com/stats/rest/skaters?reportType=shooting&isGame=true&reportName=skatersummaryshooting" + cayenne,
        "s_daily_skater_points_penalties_per60": "http://www.nhl.com/stats/rest/skaters?reportType=core&isGame=true&reportName=skaterscoring" + cayenne,
        "s_daily_skater_faceoffs_by_zone": "http://www.nhl.com/stats/rest/skaters?reportType=core&isGame=true&reportName=faceoffsbyzone" + cayenne,
        "s_daily_skater_shots_by_type": "http://www.nhl.com/stats/rest/skaters?reportType=core&isGame=true&reportName=shottype" + cayenne,
        "g_daily_goalie_summary": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&isGame=true&reportName=goaliesummary" + cayenne,
        "g_daily_goalie_by_strength": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&isGame=true&reportName=goaliebystrength" + cayenne,
        "g_daily_goalie_shootout": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_shootout&isGame=true&reportName=goalieshootout" + cayenne,
        "g_daily_goalie_daysrest": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&isGame=true&reportName=goaliedaysrest" + cayenne,
        "g_daily_goalie_penaltyshots": "http://www.nhl.com/stats/rest/goalies?reportType=goalie_basic&isGame=true&reportName=goaliepenaltyshots" + cayenne
    }
    return tables

def scrape_by_season(season_info):
    #loop through all seasons in the dict
    for this_season in season_info:
        print("Scraping data from " + this_season)
        #scrape the data here, this also builds the appropriate tables for the queries
        scrape_data(db_name, create_annual_tables_nhldotcom2018(this_season), "season")
    return


def scrape_by_game(season_info):
    delta = datetime.timedelta(days=1)
    # go through each season, looping from the first day to the last day
    for season_key in season_info:
        first_day = datetime.datetime.strptime(season_info[season_key].split(',')[0], "%Y-%m-%d").date()
        last_day = datetime.datetime.strptime(season_info[season_key].split(',')[1], "%Y-%m-%d").date()
        today = first_day
        while today <= last_day:
            print("Scraping data for games on " + today.strftime("%Y-%m-%d"))
            # scrape the data here, this also builds the appropriate tables for the queries
            scrape_data(db_name, create_daily_tables_nhldotcom2018(today), "daily")
            today += delta
    return


start = datetime.datetime.now()
print(str(start) + " Start")

# run this function to get seasonal stats tables created/updated
#scrape_by_season(test_seasons_dict)

# run this function to get daily stats tables created/updated
scrape_by_game(test_seasons_dict)

print(str(start) + " Start")
print(str(datetime.datetime.now()) + " Finish")
