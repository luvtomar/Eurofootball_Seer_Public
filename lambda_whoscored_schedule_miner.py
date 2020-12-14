import sys
import time
import json
import pymysql
import boto3
import os
import shutil
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC

def lambda_handler(event, context):
    #RDS connection
    HOST = os.environ.get('HOST')
    USERNAME = os.environ.get('USERNAME')
    PASSWORD = os.environ.get('PASSWORD')
    DATABASE = os.environ.get('DATABASE')

    conn = pymysql.connect(HOST, user=USERNAME, port=3306, passwd=PASSWORD, database=DATABASE)
    db_cursor = conn.cursor()

    months = ["Jul ","Aug ", "Sep ", "Oct ", "Nov ", "Dec ", "Jan ", "Feb ", "Mar ", "Apr ", "May ", "Jun "]
    months_index=[6,7,8,9,10,11,0,1,2,3,4,5]

    year1 = '2020'

    year2 = '2021'

    month1 = input('Enter starting month of the season:\n')
    month1 = month1 + " "

    month2 = input('Enter ending month of the season:\n')
    month2 = month2 + " "

    first_month_started = False
    schedule = dict()
    for month in months:
        if months.index(month) > months.index(month2):
            break
        if months.index(month) < months.index(month1):
            continue
        else:
            first_month_started = True
        
        #Using Selenium ChromeDriver, set up the automated browser
        link = 'https://www.whoscored.com/Regions/252/Tournaments/2/Seasons/8228/Stages/18685/Fixtures/England-Premier-League-2020-2021'
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1280x1696')
        chrome_options.add_argument('--user-data-dir=/tmp/user-data')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')
        browser = webdriver.Chrome(executable_path=ChromeDriverManager().install(), options=chrome_options)
        if months_index[months.index(month)] >= 6:
            this_year = year1
        else:
            this_year = year2

        browser.get(link)
        time.sleep(1)
        try:
            container = browser.find_element_by_xpath('//button[@class="'+ "qc-cmp-button" +'"]')
            browser.execute_script("arguments[0].click();",container)
        except:
            print('Cookies accepted')

        #Click on the button to change the date
        container = browser.find_element_by_xpath('//a[@id="date-config-toggle-button"]')
        browser.execute_script("arguments[0].click();",container)

        #Click on the button to select the year
        container = browser.find_element_by_xpath('//td[@data-value='+ this_year + ']')
        browser.execute_script("arguments[0].click();",container)

        #Click on the button to select the month if it is not greyed out
        if browser.find_element_by_xpath('//td[@data-value=' + str(months_index[months.index(month)])+ ']').value_of_css_property("color") != "rgba(136, 136, 136, 1)":
            container = browser.find_element_by_xpath('//td[@data-value=' + str(months_index[months.index(month)])+ ']')
            browser.execute_script("arguments[0].click();",container)
        else:
            browser.close()
            browser.quit()
            continue

        #Click on the button to change the date to exit the date menu
        wait = WebDriverWait(browser, 10)
        element = wait.until(EC.element_to_be_clickable((By.ID, 'tournament-fixture')))

        time.sleep(3)
        table = browser.find_element_by_class_name('divtable-body')
        print(month)
        if month in table.text:
            print(month + 'in table')
            rows = table.find_elements_by_xpath('//div[contains(@class, "divtable-row")]')
            current_date = ''
            for row in rows:
                line = row.text
                if ("Monday" in line) or ("Tuesday" in line) or ("Wednesday" in line) or ("Thursday" in line) or ("Friday" in line) or ("Saturday" in line) or ("Sunday" in line):
                    words = line.split(' ')
                    new_date = words[2]
                    if len(new_date) < 2: new_date = '0'+new_date

                    if month == "Aug ":
                        new_month = "08"
                    if month == "Sep ":
                        new_month = "09"
                    if month == "Oct ":
                        new_month = "10"
                    if month == "Nov ":
                        new_month = "11"
                    if month == "Dec ":
                        new_month = "12"
                    if month == "Jan ":
                        new_month = "01"
                    if month == "Feb ":
                        new_month = "02"
                    if month == "Mar ":
                        new_month = "03"
                    if month == "Apr ":
                        new_month = "04"
                    if month == "May ":
                        new_month = "05"
                    if month == "Jun ":
                        new_month = "06"
                    if month == "Jul ":
                        new_month = "07"

                    current_date = this_year + '-' + new_month + '-' + new_date + ' '

                elif ('vs' in line) or ('FT' in line): #A row containing match
                    future_match = True
                    if 'FT' in line: future_match = False

                    time1 = line.split('\n')[0].split('FT')[0].replace(' ','') + ':00'
                    date_time = current_date + time1

                    home_team = line.split('\n')[1]
                    away_team = line.split('\n')[3]

                    if home_team[0].isnumeric() and home_team[1].isalpha():
                        home_team = home_team[1:]
                    if away_team[len(away_team)-1].isnumeric() and away_team[len(away_team)-2].isalpha():
                        away_team = away_team[:len(away_team)-1]

                    if future_match == False: #If the match is complete and results in
                        data_id = row.get_attribute('data-id')
                        match_link = 'https://www.whoscored.com/Matches/'+data_id+'/Live/England-Premier-League-'+year1+'-'+year2+'-'+home_team.replace(" ","-")+'-'+away_team.replace(" ","-")
                        result = line.split('\n')[2]
                        home_score = result.split(' : ')[0]
                        away_score = result.split(' : ')[1]

                        if int(home_score) == int(away_score):
                            home_result = 'D'
                            away_result = 'D'
                        elif int(home_score) > int(away_score):
                            home_result = 'W'
                            away_result = 'L'
                        else:
                            home_result = 'L'
                            away_result = 'W'

                        #Check if the match's home team, away team and date / time are already in the database.
                        db_cursor.execute("Select home_team, away_team, date_time, whoscored_link from english_premier_league_schedule where date_time >= '2020-09-01' and date_time < '2021-07-01'")
                        match_rows = db_cursor.fetchall()
                        match_in_database_without_result = False
                        for row in match_rows:
                            if row[0]==home_team and row[1]==away_team and row[3] == None:
                                match_in_database_without_result = True

                        #If the match is already in the database, but without results, update the associated record
                        if match_in_database_without_result == True:
                            db_cursor.execute("update english_premier_league_schedule set home_score=" + home_score + ", away_score=" +\
                                              away_score + ", home_result='" + home_result + "', away_result='" +\
                                              away_result + "', whoscored_link='" + match_link+ "' where home_team='" + home_team +\
                                              "' and away_team='" + away_team + "' and date_time>='2020-09-01'")
                            conn.commit()
                            print('Updating an existing match record with the results')
                            print('Date & Time: ' + date_time)
                            print('Home Team: ' + home_team)
                            print('Away Team: ' + away_team)
                            print('Home Score: ' + home_score)
                            print('Away Score: ' + away_score)
                            print('Home Result: ' + home_result)
                            print('Away Result: ' + away_result)
                            print('WhoScored Link: ' + match_link)
                            print('\n')
                        else: #If there's no information on this future match at all, insert a new record

                            #As a final check, do not insert a new record if there is an existing whoscored match link.
                            db_cursor.execute("Select whoscored_link from english_premier_league_schedule")
                            all_links = db_cursor.fetchall()
                            link_exists = False
                            
                            for link in all_links:
                                if link[0] == match_link:
                                    link_exists = True
                                    print('This is in the database.')

                            if link_exists == False:

                                db_cursor.execute("insert into english_premier_league_schedule (date_time, home_team, away_team, home_score, away_score,"+\
                                                  " home_result, away_result, whoscored_link) values ('" + date_time + "', '" + home_team +\
                                                  "', '" + away_team + "', " + home_score + ", " + away_score + ", '" + home_result + "', '" +\
                                                  away_result + "', '" + match_link + "')")
                                conn.commit()
                                print('Inserting a new match record with the results')
                                print('Date & Time: ' + date_time)
                                print('Home Team: ' + home_team)
                                print('Away Team: ' + away_team)
                                print('Home Score: ' + home_score)
                                print('Away Score: ' + away_score)
                                print('Home Result: ' + home_result)
                                print('Away Result: ' + away_result)
                                print('WhoScored Link: ' + match_link)
                                print('\n')

                    else: #If this is a future match with no results to analyze
                        #Check if the future match's home team, away team and date / time are already in the database.
                        db_cursor.execute("Select home_team, away_team from english_premier_league_schedule where date_time >= '2020-09-01' and date_time < '2021-07-01'")
                        match_rows = db_cursor.fetchall()
                        future_match_in_database = False
                        for row in match_rows:
                            if row[0]==home_team and row[1]==away_team:
                                future_match_in_database = True
                                print('Future match already in database')

                        if future_match_in_database == False:
                            db_cursor.execute("insert into english_premier_league_schedule (date_time, home_team, away_team)"+\
                                              " values ('" + date_time + "', '" + home_team + "', '" + away_team + "')")
                            conn.commit()
                            print('Inserting a new match record without the results')
                            print('Date & Time: ' + date_time)
                            print('Home Team: ' + home_team)
                            print('Away Team: ' + away_team)
                            print('\n')
        browser.close()
        browser.quit()

    browser.quit()
    db_cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Scraped Successfully!')
    }
