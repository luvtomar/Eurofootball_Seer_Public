import sys
import time
import json
import pymysql
import boto3
import os
import shutil
#from mysql.connector import MySQLConnection
#import mysql.connector
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def lambda_handler(event, context):
    #RDS connection
    HOST = "soccer-schedules.my-rds-ID.rds.amazonaws.com"
    USERNAME = "my-username"
    PASSWORD = "my-password"
    DATABASE = "soccer_schedules"


    #db = MySQLConnection(host=HOST, user=USERNAME, password=PASSWORD, database=DATABASE)
    #db_cursor = db.cursor()
    conn = pymysql.connect(HOST, user=USERNAME, port=3306, passwd=PASSWORD, database=DATABASE)
    db_cursor = conn.cursor()

    s3 = boto3.resource('s3',aws_access_key_id='my-aws-access-key-id',\
                        aws_secret_access_key='my-aws-secret-access-key',\
                        region_name='ca-central-1')

    #Open the player data dictionary
    team_data = {}
    #with open('players_data.json','r') as r_file: team_data = json.load(r_file)
    obj = s3.Object('eurofootballseer', 'players_data.json')
    team_data = json.loads(obj.get()['Body'].read().decode('utf-8'))

    #Open the team data dictionary
    team_data2 = {}
    #with open('teams.json','r') as r_file: team_data2 = json.load(r_file)
    obj2 = s3.Object('eurofootballseer', 'teams.json')
    team_data2 = json.loads(obj2.get()['Body'].read().decode('utf-8'))
        
    db_cursor.execute("show tables")
    tables = db_cursor.fetchall()
    for table in tables:
        #Scan the input competition schedule for WhoScored match stats links only
        if event['competition'] not in table[0]:
            continue

        #Extract the required data from the schedule for the input season
        db_cursor.execute("select whoscored_link, date_time, home_team, away_team from " + table[0] + " where date_time >= '" + event['season year 1']+ "-08-01 00:00:00' and date_time < '"+event['season year 2']+"-08-01 00:00:00' order by date_time")
        links = db_cursor.fetchall()

        #In case of an interrupt during the scraping, start from the match last match link that was being scraped.
        link_found = False
        for link in links:
            current_link = link[0].replace("Live","LiveStatistics")
            #In case of an interrupt during the scraping, start from the match last match link that was being scraped.
            if current_link != 'https://www.whoscored.com/Matches/1189197/LiveStatistics/Netherlands-Eredivisie-2017-2018-FC-Utrecht-AZ-Alkmaar' and link_found == False: continue
            link_found = True
            
            home_team = link[2]
            away_team = link[3]
            current_date_time = link[1].strftime("%Y-%m-%d %H:%M:%S")

            #Check if the stats for this match have already been scraped.
            all_home_dates_per_player = [list(team_data[player]['Match Stats'].keys()) for player in list(team_data2[home_team].keys())]
            all_away_dates_per_player = [list(team_data[player]['Match Stats'].keys()) for player in list(team_data2[away_team].keys())]
            home_player_has_date = [True if current_date_time in dates else False for dates in all_home_dates_per_player]
            away_player_has_date = [True if current_date_time in dates else False for dates in all_away_dates_per_player]
            if (False not in home_player_has_date) and (False not in away_player_has_date):
                continue

            print(current_link)

            #Start Chromedriver
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--incognito")
            #chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1280x1696')
            chrome_options.add_argument('--user-data-dir=/tmp/user-data')
            #chrome_options.add_argument('--hide-scrollbars')
            #chrome_options.add_argument('--enable-logging')
            #chrome_options.add_argument('--log-level=0')
            #chrome_options.add_argument('--v=99')
            #chrome_options.add_argument('--single-process')
            #chrome_options.add_argument('--data-path=/tmp/data-path')
            chrome_options.add_argument('--ignore-certificate-errors')
            #chrome_options.add_argument('--homedir=/tmp')
            #chrome_options.add_argument('--disk-cache-dir=/tmp/cache-dir')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
            browser = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=chrome_options)
            try:
                browser.get(current_link)
            except:
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break

            #Click through the initial message about cookies
            try:
                container = browser.find_element_by_xpath('//button[@class="'+ "qc-cmp-button" +'"]')
                browser.execute_script("arguments[0].click();",container)
            except:
                print('Cookies message not there')

            #In case of a connection interruption
            try:
                wait = WebDriverWait(browser, 10)
            except TimeoutException as e:
                print('summary webdriver wait break 1')
                #obj.put(Body=json.dumps(team_data))
                with open('players_data.json','w') as w_file:
                    json.dump(team_data,w_file)
                break

            #In case of a connection interruption
            try:
                element = wait.until(EC.element_to_be_clickable((By.ID, 'statistics-table-home-summary')))
            except TimeoutException as e:
                print('summary webdriver wait break 2')
                #obj.put(Body=json.dumps(team_data))
                with open('players_data.json','w') as w_file:
                    json.dump(team_data,w_file)
                break
            
            #Summary Tables#######################################################################################################################################################
            try:
                home_table_row_tags = browser.find_element_by_id("statistics-table-home-summary").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                print('home table timeout error')
                #obj.put(Body=json.dumps(team_data))
                with open('players_data.json','w') as w_file:
                    json.dump(team_data,w_file)
                break
            player_count = 0;

            #for each player in the home team
            for tag in home_table_row_tags:
                #extract player name
                player_count = player_count + 1
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break

                #get the player's name
                player_name = ""
                for name in player_name_split:
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                #Create a dictionary for match stats for this player if it does not exist
                if player_name not in list(team_data.keys()):
                    team_data[player_name] = {'Match Stats':{}}

                #Create a dictionary under Match Stats for this date if it doesn't exist already for this player
                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}

                #If at least one of the summary features of this player for this match
                if "Shots" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue

                #For every match link, the top 15 players at most (including up to 3 substitutions) are involved at least for a minute in the match
                if player_count < 15:
                    shots_T = int(tag.find_element_by_class_name("ShotsTotal").text.replace(" ",""))
                    shotsOT_T = int(tag.find_element_by_class_name("ShotOnTarget").text.replace(" ",""))
                    keyPasses_T = int(tag.find_element_by_class_name("KeyPassTotal").text.replace(" ",""))
                    PA_T = float(tag.find_element_by_class_name("PassSuccessInMatch").text.replace(" ",""))
                    aerialsWon_T = int(tag.find_element_by_class_name("DuelAerialWon").text.replace(" ",""))
                    touches_T = int(tag.find_element_by_class_name("Touches").text.replace(" ",""))

                    #Extract the start times for players substituted on and the end time for players substituted off
                    try:
                        rating_T = float(tag.find_element_by_class_name("rating").text.replace(" ",""))
                        
                        if player_count > 11:
                            startT = int(tag.find_element_by_class_name("pn").find_element_by_class_name("player-link").\
                                             find_element_by_class_name("incident-wrapper").\
                                             find_element_by_class_name("player-meta-data").\
                                             text.replace("(","").replace("′)",""))
                            endT = 90
                        else:
                            startT = 0
                            try:
                                endT = int(tag.find_element_by_class_name("pn").find_element_by_class_name("player-link").\
                                               find_element_by_class_name("incident-wrapper").\
                                               find_element_by_class_name("player-meta-data").\
                                               text.replace("(","").replace("′)",""))
                            except:
                                endT = 90
                    #In case not all three substitutes are used
                    except:
                        rating_T = 0
                        startT = "-"
                        endT = "-"                    
                    
                else:
                    shots_T = 0
                    shotsOT_T = 0
                    keyPasses_T = 0
                    PA_T = 0
                    aerialsWon_T = 0
                    touches_T = 0
                    rating_T = 0
                    startT = "-"
                    endT = "-"
                    
                team_data[player_name]["Match Stats"][current_date_time]["My Team"] = home_team
                team_data[player_name]["Match Stats"][current_date_time]["Opponent Team"] = away_team
                team_data[player_name]["Match Stats"][current_date_time]["Start Time"] = startT
                team_data[player_name]["Match Stats"][current_date_time]["End Time"] = endT
                team_data[player_name]["Match Stats"][current_date_time]["Shots"] = shots_T
                team_data[player_name]["Match Stats"][current_date_time]["Shots on Target"] = shotsOT_T
                team_data[player_name]["Match Stats"][current_date_time]["Key Passes"] = keyPasses_T
                team_data[player_name]["Match Stats"][current_date_time]["Pass Accuracy"] = PA_T
                team_data[player_name]["Match Stats"][current_date_time]["Aerials Won"] = aerialsWon_T
                team_data[player_name]["Match Stats"][current_date_time]["Touches"] = touches_T
                team_data[player_name]["Match Stats"][current_date_time]["Rating"] = rating_T

            #Repeat the same steps for the Summary stats for the away team.
            try:
                away_table_row_tags = browser.find_element_by_id("statistics-table-away-summary").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            player_count = 0;
            for tag in away_table_row_tags:
                player_count = player_count + 1
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]
                
                if player_name not in list(team_data.keys()):
                    team_data[player_name] = {'Match Stats':{}}
                
                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}

                if "Shots" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    shots_T = int(tag.find_element_by_class_name("ShotsTotal").text.replace(" ",""))
                    shotsOT_T = int(tag.find_element_by_class_name("ShotOnTarget").text.replace(" ",""))
                    keyPasses_T = int(tag.find_element_by_class_name("KeyPassTotal").text.replace(" ",""))
                    PA_T = float(tag.find_element_by_class_name("PassSuccessInMatch").text.replace(" ",""))
                    aerialsWon_T = int(tag.find_element_by_class_name("DuelAerialWon").text.replace(" ",""))
                    touches_T = int(tag.find_element_by_class_name("Touches").text.replace(" ",""))
                    try:
                        rating_T = float(tag.find_element_by_class_name("rating").text.replace(" ",""))
                        
                        if player_count > 11:
                            startT = int(tag.find_element_by_class_name("pn").find_element_by_class_name("player-link").\
                                             find_element_by_class_name("incident-wrapper").\
                                             find_element_by_class_name("player-meta-data").\
                                             text.replace("(","").replace("′)",""))
                            endT = 90
                        else:
                            startT = 0
                            try:
                                endT = int(tag.find_element_by_class_name("pn").find_element_by_class_name("player-link").\
                                               find_element_by_class_name("incident-wrapper").\
                                               find_element_by_class_name("player-meta-data").\
                                               text.replace("(","").replace("′)",""))
                            except:
                                endT = 90
                    except:
                        rating_T = 0
                        startT = "-"
                        endT = "-"                    
                    
                else:
                    shots_T = 0
                    shotsOT_T = 0
                    keyPasses_T = 0
                    PA_T = 0
                    aerialsWon_T = 0
                    touches_T = 0
                    rating_T = 0
                    startT = "-"
                    endT = "-"
                    
                team_data[player_name]["Match Stats"][current_date_time]["My Team"] = away_team
                team_data[player_name]["Match Stats"][current_date_time]["Opponent Team"] = home_team
                team_data[player_name]["Match Stats"][current_date_time]["Start Time"] = startT
                team_data[player_name]["Match Stats"][current_date_time]["End Time"] = endT
                team_data[player_name]["Match Stats"][current_date_time]["Shots"] = shots_T
                team_data[player_name]["Match Stats"][current_date_time]["Shots on Target"] = shotsOT_T
                team_data[player_name]["Match Stats"][current_date_time]["Key Passes"] = keyPasses_T
                team_data[player_name]["Match Stats"][current_date_time]["Pass Accuracy"] = PA_T
                team_data[player_name]["Match Stats"][current_date_time]["Aerials Won"] = aerialsWon_T
                team_data[player_name]["Match Stats"][current_date_time]["Touches"] = touches_T
                team_data[player_name]["Match Stats"][current_date_time]["Rating"] = rating_T





            #Offensive tables#########################################################################################################################################

            #Repeat the same steps for the Offensive stats for the home team.
            container = browser.find_element_by_xpath('//a[@href="#live-player-home-offensive"]')
            browser.execute_script("arguments[0].click();",container)

            container = browser.find_element_by_xpath('//a[@href="#live-player-away-offensive"]')
            browser.execute_script("arguments[0].click();",container)
            
            try:
                wait = WebDriverWait(browser, 10)
            except TimeoutException as e:
                print('offensive webdriver wait break')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            try:
                element = wait.until(EC.element_to_be_clickable((By.ID, 'statistics-table-home-offensive')))
            except TimeoutException as e:
                print('summary webdriver wait break 3')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break

            try:
                home_table_row_tags = browser.find_element_by_id("statistics-table-home-offensive").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                print('offensive table timeout error')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            player_count = 0;
            for tag in home_table_row_tags:
                player_count = player_count + 1
                
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    #print(name)
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                    
                if "Dribbles Won" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    dribbles_T = int(tag.find_element_by_class_name("DribbleWon").text.replace(" ",""))
                    fouled_T = int(tag.find_element_by_class_name("FoulGiven").text.replace(" ",""))
                    offsides_T = int(tag.find_element_by_class_name("OffsideGiven").text.replace(" ",""))
                    dispossessed_T = int(tag.find_element_by_class_name("Dispossessed").text.replace(" ",""))
                    unused_T = int(tag.find_element_by_class_name("Turnover").text.replace(" ",""))
                else:
                    dribbles_T = 0
                    fouled_T = 0
                    offsides_T = 0
                    dispossessed_T = 0
                    unused_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Dribbles Won"] = dribbles_T
                team_data[player_name]["Match Stats"][current_date_time]["Fouls Won"] = fouled_T
                team_data[player_name]["Match Stats"][current_date_time]["Offsides"] = offsides_T
                team_data[player_name]["Match Stats"][current_date_time]["Dispossessed"] = dispossessed_T
                team_data[player_name]["Match Stats"][current_date_time]["Unused Touches"] = unused_T

            #Repeat the same steps for the Offensive stats for the away team.
            try:
                away_table_row_tags = browser.find_element_by_id("statistics-table-away-offensive").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            player_count = 0;
            for tag in away_table_row_tags:

                player_count = player_count + 1
                
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    #print(name)
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                    
                if "Dribbles Won" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    dribbles_T = int(tag.find_element_by_class_name("DribbleWon").text.replace(" ",""))
                    fouled_T = int(tag.find_element_by_class_name("FoulGiven").text.replace(" ",""))
                    offsides_T = int(tag.find_element_by_class_name("OffsideGiven").text.replace(" ",""))
                    dispossessed_T = int(tag.find_element_by_class_name("Dispossessed").text.replace(" ",""))
                    unused_T = int(tag.find_element_by_class_name("Turnover").text.replace(" ",""))
                else:
                    dribbles_T = 0
                    fouled_T = 0
                    offsides_T = 0
                    dispossessed_T = 0
                    unused_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Dribbles Won"] = dribbles_T
                team_data[player_name]["Match Stats"][current_date_time]["Fouls Won"] = fouled_T
                team_data[player_name]["Match Stats"][current_date_time]["Offsides"] = offsides_T
                team_data[player_name]["Match Stats"][current_date_time]["Dispossessed"] = dispossessed_T
                team_data[player_name]["Match Stats"][current_date_time]["Unused Touches"] = unused_T




            #Defensive Tables#####################################################################################################################

            container = browser.find_element_by_xpath('//a[@href="#live-player-home-defensive"]')
            browser.execute_script("arguments[0].click();",container)

            container = browser.find_element_by_xpath('//a[@href="#live-player-away-defensive"]')
            browser.execute_script("arguments[0].click();",container)
            
            #Repeat the same steps for the Defensive stats for the home team.
            try:
                wait = WebDriverWait(browser, 10)
            except TimeoutException as e:
                print('defensive webdriver wait break')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            try:
                element = wait.until(EC.element_to_be_clickable((By.ID, 'statistics-table-home-defensive')))
            except TimeoutException as e:
                print('summary webdriver wait break 4')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            try:
                home_table_row_tags = browser.find_element_by_id("statistics-table-home-defensive").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                print('defensive table timeout error')
                break
            
            player_count = 0;
            for tag in home_table_row_tags:
                player_count = player_count + 1
                
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                    
                if "Tackles Won" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    tackles_T = int(tag.find_element_by_class_name("TackleWonTotal").text.replace(" ",""))
                    interceptions_T = int(tag.find_element_by_class_name("InterceptionAll").text.replace(" ",""))
                    clearances_T = int(tag.find_element_by_class_name("ClearanceTotal").text.replace(" ",""))
                    blocked_T = int(tag.find_element_by_class_name("ShotBlocked").text.replace(" ",""))
                    fouls_T = int(tag.find_element_by_class_name("FoulCommitted").text.replace(" ",""))
                else:
                    tackles_T = 0
                    interceptions_T = 0
                    clearances_T = 0
                    blocked_T = 0
                    fouls_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Tackles Won"] = tackles_T
                team_data[player_name]["Match Stats"][current_date_time]["Interceptions"] = interceptions_T
                team_data[player_name]["Match Stats"][current_date_time]["Clearances"] = clearances_T
                team_data[player_name]["Match Stats"][current_date_time]["Blocked Shots"] = blocked_T
                team_data[player_name]["Match Stats"][current_date_time]["Fouls Committed"] = fouls_T

            #Repeat the same steps for the Defensive stats for the away team.
            try:
                away_table_row_tags = browser.find_element_by_id("statistics-table-away-defensive").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            player_count = 0;
            for tag in away_table_row_tags:
                
                player_count = player_count + 1
                
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                    
                if "Tackles Won" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    tackles_T = int(tag.find_element_by_class_name("TackleWonTotal").text.replace(" ",""))
                    interceptions_T = int(tag.find_element_by_class_name("InterceptionAll").text.replace(" ",""))
                    clearances_T = int(tag.find_element_by_class_name("ClearanceTotal").text.replace(" ",""))
                    blocked_T = int(tag.find_element_by_class_name("ShotBlocked").text.replace(" ",""))
                    fouls_T = int(tag.find_element_by_class_name("FoulCommitted").text.replace(" ",""))
                else:
                    tackles_T = 0
                    interceptions_T = 0
                    clearances_T = 0
                    blocked_T = 0
                    fouls_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Tackles Won"] = tackles_T
                team_data[player_name]["Match Stats"][current_date_time]["Interceptions"] = interceptions_T
                team_data[player_name]["Match Stats"][current_date_time]["Clearances"] = clearances_T
                team_data[player_name]["Match Stats"][current_date_time]["Blocked Shots"] = blocked_T
                team_data[player_name]["Match Stats"][current_date_time]["Fouls Committed"] = fouls_T




            #Passing Tables##############################################################################################################
            #Repeat the same steps for the Passing stats for the home team.
                
            container = browser.find_element_by_xpath('//a[@href="#live-player-home-passing"]')
            browser.execute_script("arguments[0].click();",container)

            container = browser.find_element_by_xpath('//a[@href="#live-player-away-passing"]')
            browser.execute_script("arguments[0].click();",container)
            
            try:
                wait = WebDriverWait(browser, 10)
            except TimeoutException as e:
                print('passing webdriver wait break')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            try:
                element = wait.until(EC.element_to_be_clickable((By.ID, 'statistics-table-home-passing')))
            except TimeoutException as e:
                print('summary webdriver wait break 5')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break

            try:
                home_table_row_tags = browser.find_element_by_id("statistics-table-home-passing").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                print('passing table timeout error')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            
            player_count = 0;
            for tag in home_table_row_tags:
                
                player_count = player_count + 1
                
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                    
                if "Total Passes" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    passes_T = int(tag.find_element_by_class_name("TotalPasses").text.replace(" ",""))
                    crosses_T = int(tag.find_element_by_class_name("PassCrossTotal").text.replace(" ",""))
                    accCrosses_T = int(tag.find_element_by_class_name("PassCrossAccurate").text.replace(" ",""))
                    longBalls_T = int(tag.find_element_by_class_name("PassLongBallTotal").text.replace(" ",""))
                    accLongBalls_T = int(tag.find_element_by_class_name("PassLongBallAccurate").text.replace(" ",""))
                    throughBalls_T = int(tag.find_element_by_class_name("PassThroughBallTotal").text.replace(" ",""))
                    accThroughBalls_T = int(tag.find_element_by_class_name("PassThroughBallAccurate").text.replace(" ",""))
                else:
                    passes_T = 0
                    crosses_T = 0
                    accCrosses_T = 0
                    longBalls_T = 0
                    accLongBalls_T = 0
                    throughBalls_T = 0
                    accThroughBalls_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Total Passes"] = passes_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Crosses"] = crosses_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Crosses"] = accCrosses_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Long Balls"] = longBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Long Balls"] = accLongBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Through Balls"] = throughBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Through Balls"] = accThroughBalls_T

            #Repeat the same steps for the Passing stats for the away team.
            try:
                away_table_row_tags = browser.find_element_by_id("statistics-table-away-passing").find_element_by_id('player-table-statistics-body').find_elements_by_tag_name('tr')
            except:
                print('passing table timeout error')
                obj.put(Body=json.dumps(team_data))
                #with open('players_data.json','w') as w_file:
                #    json.dump(team_data,w_file)
                break
            player_count = 0;
            for tag in away_table_row_tags:

                player_count = player_count + 1
                try:
                    player_name_split = tag.find_element_by_class_name("pn").find_element_by_tag_name("a").text.split(' ')
                except:
                    break
                
                player_name = ""
                for name in player_name_split:
                    #print(name)
                    if name.isalpha():
                        player_name = player_name + name + " "
                player_name = player_name[0:len(player_name)-1]

                if current_date_time not in list(team_data[player_name]["Match Stats"].keys()):
                    team_data[player_name]["Match Stats"][current_date_time] = {}
                
                if "Total Passes" in team_data[player_name]["Match Stats"][current_date_time]:
                    continue
                
                if player_count < 15:
                    passes_T = int(tag.find_element_by_class_name("TotalPasses").text.replace(" ",""))
                    crosses_T = int(tag.find_element_by_class_name("PassCrossTotal").text.replace(" ",""))
                    accCrosses_T = int(tag.find_element_by_class_name("PassCrossAccurate").text.replace(" ",""))
                    longBalls_T = int(tag.find_element_by_class_name("PassLongBallTotal").text.replace(" ",""))
                    accLongBalls_T = int(tag.find_element_by_class_name("PassLongBallAccurate").text.replace(" ",""))
                    throughBalls_T = int(tag.find_element_by_class_name("PassThroughBallTotal").text.replace(" ",""))
                    accThroughBalls_T = int(tag.find_element_by_class_name("PassThroughBallAccurate").text.replace(" ",""))
                else:
                    passes_T = 0
                    crosses_T = 0
                    accCrosses_T = 0
                    longBalls_T = 0
                    accLongBalls_T = 0
                    throughBalls_T = 0
                    accThroughBalls_T = 0
                    
                team_data[player_name]["Match Stats"][current_date_time]["Total Passes"] = passes_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Crosses"] = crosses_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Crosses"] = accCrosses_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Long Balls"] = longBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Long Balls"] = accLongBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Total Through Balls"] = throughBalls_T
                team_data[player_name]["Match Stats"][current_date_time]["Accurate Through Balls"] = accThroughBalls_T
            browser.close()
            browser.quit()

    try:
        browser.close()
        browser.quit()
    except:
        pass

    #with open('players_data.json','w') as updated_file:
    #    json.dump(team_data,updated_file)
    obj.put(Body=json.dumps(team_data))
    db_cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Scraped Successfully!')
    }
