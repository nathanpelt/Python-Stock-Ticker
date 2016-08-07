import schedule
import time
import datetime
import mysql.connector
import os
import urllib
import csv

# updates stock tables in datawarehouse at 4:00pm    
def do_update():
    
    # runs the function update at 4:00pm
    schedule.every().day.at("16:00").do(update)
    #while true waits and a minute then checks if its time to update
    while True: 
        schedule.run_pending()
        time.sleep(60) 
        
# updates the stock tables
def update():
    
    # gets current date and time from computer
    current_date = datetime.datetime.now()
    current_date = str(current_date)
    # creates current date in desired format
    end_date = current_date[5] + current_date[6] + "/" + current_date[8] + current_date[9] + \
    "/" + current_date[0] + current_date[1] + current_date[2] + current_date[3]
    # connects to the MYSQL database  
    conn = mysql.connector.connect(user='root',password='root',host='localhost',database='datawarehouse')
    mycursor=conn.cursor()
    # shows tables 
    mycursor.execute("show tables")
    o = mycursor.fetchall()
    url = ""
    # checks to see if the table count is greater than 0 if so it updates a table until all are updated
    while int(len(o)) > 0:
        stock_symbol = str(o.pop(0))
        # replaces some characters so that stock symbol can be used
        stock_symbol = stock_symbol.replace("(","").replace(")","").replace(",","").replace("'","")
        # gets max date from table
        mycursor.execute("SELECT Max(Date) FROM " + stock_symbol)
        # sets start date equal to Max(date)
        start_date = mycursor.fetchall()
        # converts to string
        start_date = str(start_date)
        # replaces some characters so that start_date can be used
        start_date = start_date.replace("(","").replace(")","").replace("[","").replace("]","").replace(",","").replace("'","")
        #changes the date format from 2015-10-12 to 10/12/2015
        number = (int(start_date[8]) + int(start_date[9]) + 1)
        if number < 10:
            number = "0" + str(number)
        start_date = start_date[5] + start_date[6] + "/" + number + "/" + start_date[0] + start_date[1] + start_date[2] + start_date[3]
        daily = "daily"
        # url generator generates url to extract from
        url = url_generator(stock_symbol, start_date, end_date, daily)
        # directory is changed to C:\Datawarehouse\Update 
        os.chdir(r"C:\Datawarehouse\Update")
        # extracts the csv file from yahoo finance
        try:
            urllib.request.urlretrieve(url, 'mytable.csv')
        except:
            print("error")
        # transforms data so it can be used to update table 
        with open('mytable.csv') as csvinput:
            with open('update_output.csv', 'w') as csvoutput:
                mywriter = csv.writer(csvoutput)
                myreader = csv.reader(csvinput)
                mylist = next(myreader)
                # makes column name the stock symbol name
                mywriter.writerow(mylist+[stock_symbol]) 
                # makes rest of the column the stock symbol name
                for row in csv.reader(csvinput):
                    mywriter.writerow(row+[stock_symbol])
    # moves update_output.csv 
    os.rename(r"C:\Datawarehouse\Update\update_output.csv", r"C:\mysql\data\datawarehouse\update_output.csv")
    # deletes mytable.csv
    os.remove(r"C:\Datawarehouse\Update\mytable.csv")
    # adds quotes to stock name so MYSQL has no problems with unusual symbols
    new = "`" + stock_symbol  + "`"
    # loads csv file into table
    mycursor.execute("load data infile 'update_output.csv' into table " + new + " fields terminated by" \
    " ',' lines terminated by '\n' (Date, Open, High, Low, Close, Volume, AdjClose, Company)")
    # deletes top column from csv file that was loaded into database (only contained titles, no actual data)
    mycursor.execute("delete from " + new + " where date = 'date'")
    # closes MYSQL connection
    conn.commit()
    conn.close()
    mycursor.close()
    # deletes update_output.csv
    os.remove(r"C:\mysql\data\datawarehouse\update_output.csv")
    
# generates a url to extract stock data from, takes in Stock_symbol, Start_date, End_date
def url_generator(stock_symbol, start_date, end_date, frequency):
    
    # removes '/' from the start date
    start_month, start_day, start_year = start_date.split('/')
    # removes '/' from the end date
    end_month, end_day, end_year = end_date.split('/')
    # generates the url based on user input to extract stock data
    url = "http://real-chart.finance.yahoo.com/table.csv?s=" + stock_symbol + "&a=" + month_format(start_month) + "&b=" \
    + start_day + "&c=" + start_year + "&d=" + month_format(end_month) + "&e=" + end_day + "&f=" + end_year + "&g=" + frequency[0] \
    + "&ignore=.csv" 
    # returns url 
    return url

# takes month as input and check to see if its in correct format
def month_format(month):
    
    # makes month an int as x
    x = int(month)
    # subtract 1 from x 
    x = x - 1
    # if x is less than 10 add a 0 in from of it
    if x < 10:
        z = str(x)
        y = "0" + z
        return y;
    # otherwise return th number
    else: 
        z = str(x)
        return z;
    
    
# function that updates all current stock tables at 4:00pm
do_update(update)
