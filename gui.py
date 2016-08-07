'''
Created on Oct 20, 2015 
Modified last on Dec 13, 2015

@author: Nathan Pelt
'''

# import modules
import os
import sys
import urllib
import datetime
import csv
import mysql.connector
from PyQt4.QtGui import *
from urllib import request


def submit():
    # sets stock_symbol equal to text from textbox
    stock_symbol = textbox.displayText()
    # sets start_date equal to text from textbox2
    start_date = textbox2.displayText()
    # sets end_date equal to text from textbox3
    end_date = textbox3.displayText()
    #sets frequency equal to text from combobox
    frequency = combo_box_frequency.currentText()
    
    #checks to see if start_date and end_date have the correct format
    if date_format(start_date) == True and date_format(end_date) == True: 
        # checks to see if day format is correct
        start_date = day_format(start_date)
        end_date = day_format(end_date)
        # generates url based on input
        url = url_generator(stock_symbol, start_date, end_date, frequency)
        # does ETL process
        extract(url)
        transform(stock_symbol) 
        load(stock_symbol)
    else:
        # displays error message if unsuccessful
        Error_window = QWidget()
        Error_window.setwindowIcon(QIcon("error.ico"))
        QMessageBox.warning(Error_window, "Error", "Incorrect date format entered")
        # sets textboxes to nothing
        textbox.setText("")
        textbox2.setText("")
        textbox3.setText("")

# shows the user input window 
def show_user_input_window():
    
    # shows some window objects
    text.show() 
    text2.show()   
    text3.show() 
    text_example.show()
    textbox.show() 
    textbox2.show() 
    textbox3.show() 
    button.show()
    combo_box_frequency.show()
    # moves back button
    back_button.move(200, 30)
    # shows back button
    back_button.show()
    
    # hides menu buttons
    menu_button_textfile.hide()
    menu_button_inputfile.hide()
    menu_button_show_tables.hide()

# shows file dialog for user to load in txt file for ETL process
def show_file_dialog_window():
    # creates file dialog object
    filename = QFileDialog.getOpenFileName(window, 'Open File', '/')
    # opens file
    with open(filename) as file:
        text_file = file.read().split(',')
        file.close()
    # divides amount of txt file words by 3 to get count on how many times to do ETL process
    x = int(len(text_file) / 3)
    y = 0
    # while there are still stocks to do ETL process on continue
    while (y < x):
        # gets 3 strings from text file
        Stock_symbol = text_file.pop(0)
        Start_date = text_file.pop(0)
        End_date = text_file.pop(0)
        #strings have white space in front of them so this deletes the whitespace
        Stock_symbol = Stock_symbol.replace(" ","")
        Start_date = Start_date.replace(" ","")
        End_date = End_date.replace(" ","")
        y = y + 1
        # checks to see if datr format is correct
        if date_format(Start_date) == True and date_format(End_date) == True: 
            daily = "daily"
            # generates url based on input
            url = url_generator(Stock_symbol, Start_date, End_date, daily)
            # does ETL process
            extract(url)
            transform(Stock_symbol) 
            load(Stock_symbol)
        else:
            # errors list to show what stocks did not do ETL process (unfinished)
            errors.append(Stock_symbol)
            errors.append(Start_date)
            errors.append(End_date)
    # message says stock data added to database
    QMessageBox.information(window, "Message", "Stock data added to database")    
        
# show the menu window        
def show_menu_window():
    
    # show menu buttons
    menu_button_textfile.show()
    menu_button_inputfile.show()
    menu_button_show_tables.show()
    
    # sets textbox to nothing
    textbox.setText("")
    textbox2.setText("")
    textbox3.setText("")
    
    # hides some window objects
    back_button.hide()
    text.hide() 
    text2.hide()  
    text3.hide()
    textbox.hide()
    textbox2.hide() 
    textbox3.hide()
    button.hide()
    combo_box.hide()
    delete_button.hide()
    combo_box_frequency.hide()
    text_example.hide()
    # removes objects from combobox
    y = 0
    while (y < combo_box.count()):
        combo_box.removeItem(0)
        combo_box.removeItem(0)
        y = y + 1

# updates the stock tables in the combobox
def show_stock_window():
    
    # hides some window objects
    menu_button_textfile.hide()
    menu_button_inputfile.hide()
    menu_button_show_tables.hide()
    # connects to MYSQL database
    conn = mysql.connector.connect(user='root',password='root',host='localhost',database='datawarehouse')
    mycursor=conn.cursor()
    mycursor.execute("show tables")
    o = mycursor.fetchall() 
    x = str(o)
    # closes database connection
    conn.commit()
    conn.close()
    mycursor.close()
    # replaces characters in string
    x = x.replace("(","").replace(")","").replace("[","").replace("]","").replace(",","").replace("'","")
    o = x.split (' ')
    # gets the size of the string o as an int
    x = int(len(o))
    y = 0
    # deletes contents of combobox
    while (y < x): 
        combo_box.addItem(o.pop(0))
        y = y + 1
        
    # shows some window contents and moves back button
    combo_box.show()
    back_button.move(200, 30)
    back_button.show()
    delete_button.show()

# delete table and updates combobox
def delete_table():
    # connects to MYSQL database
    conn = mysql.connector.connect(user='root',password='root',host='localhost',database='datawarehouse')
    mycursor=conn.cursor()
    # gets combobox current text and sets it to Drop_table
    Drop_table = combo_box.currentText()
    # adds quotes to stock name so MYSQL has no problems with unusual symbols
    Drop_table = "`" + Drop_table + "`"
    # executes MYSQL query 
    mycursor.execute("drop table" + Drop_table)
    # closes database connection
    conn.commit()
    conn.close()
    mycursor.close()
    # deletes contents of combobox
    y = 0
    while (y < combo_box.count()):
        combo_box.removeItem(0)
        combo_box.removeItem(0)
        y = y + 1
    # reloads stock tables in combobox so combobox is displaying current the current tables
    show_stock_window()

# checks to see if date format is correct
def date_format(date):
    if date[0].isdigit() == True and date[1].isdigit() == True and date[2] == "/" and date[3].isdigit() == True and \
    date[4].isdigit() == True and date[5] == "/" and date[6].isdigit() == True and date[7].isdigit() == True and    \
    date[8].isdigit() == True and date[9].isdigit() == True and (int(len(date))) == 10: 
        return True
    else:
        return False

# checks to see if day_format is correct and if not changes it
def day_format(date):
    if date[3] == "0" and date[4].isdigit():
        new_date = date[0] + date[1] + date[2] + "0" + date[4] + date[5] + date[6] + date[7] + date[8] + date[9]
        return new_date
    else:
        return date

# extracts csv file from yahoo finance
def extract(url):
    # checks to see if mytable.csv exists and if so deletes it
    if os.path.exists(r"C:\Datawarehouse\ETL\mytable.csv") == True:
        os.remove(r"C:\Datawarehouse\ETL\mytable.csv")
        # checks to see if output.csv exists and if so deletes it
    if os.path.exists(r"C:\Datawarehouse\ETL\output.csv") == True:
        os.remove(r"C:\Datawarehouse\ETL\output.csv")
    # changes the directory to C:\Datawarehouse\ETL
    os.chdir(r"C:\Datawarehouse\ETL")
    # trys to download data
    try:
        urllib.request.urlretrieve(url, 'mytable.csv')
    except:
        # Shows error message if fails
        Error_window = QWidget()
        # Sets error message icon
        Error_window.setwindowIcon(QIcon("error.ico"))
        # Shows error message text
        QMessageBox.warning(Error_window, "Error", "Unable to retrieve data")
        # sets textboxes to nothing
        textbox.setText("")
        textbox2.setText("")
        textbox3.setText("")

# creates directorys and database if they do not exist already
def create_directory_and_database():
    # if C:\Datawarehouse directory does not exist it is created
    if os.path.exists(r"C:\Datawarehouse") == False:
        os.mkdir(r"C:\Datawarehouse") 
    # if C:\Datawarehouse\ETL directory does not exist it is created
    if os.path.exists(r"C:\Datawarehouse\ETL") == False:
        os.mkdir(r"C:\Datawarehouse\ETL")
    # if C:\Datawarehouse\Update directory does not exist it is created
    if os.path.exists(r"C:\Datawarehouse\Update") == False:
        os.mkdir(r"C:\Datawarehouse\Update")
        # if datawarehouse database does not exist it is created
    if os.path.exists(r"C:\mysql\data\datawarehouse") == False:
        conn = mysql.connector.connect(user='root',password='root',host='localhost',database='nathantest')
        mycursor=conn.cursor()
        mycursor.execute("create database datawarehouse")
        # closes MYSQL connection 
        conn.commit()
        conn.close()
        mycursor.close()
    
# transforms the csv file               
def transform(Stock_name):
    
    # opens csv file 
    with open('mytable.csv') as csvinput:
        with open('output.csv', 'w') as csvoutput:
            mywriter = csv.writer(csvoutput)
            myreader = csv.reader(csvinput)
            # adds Company at top on new columns
            mylist = next(myreader)
            mywriter.writerow(mylist+['Company'])
            # adds stock name to newly created column called Company
            for row in csv.reader(csvinput):
                mywriter.writerow(row+[Stock_name])
                
def load(Stock_name):
    
    # moves output file to mysql table file
    os.rename(r"C:\Datawarehouse\ETL\output.csv", r"C:\mysql\data\datawarehouse\output.csv") 
    # deletes mytable.csv
    os.remove(r"C:\Datawarehouse\ETL\mytable.csv")
    # MYSQL connection established
    conn = mysql.connector.connect(user='root',password='root',host='localhost',database='datawarehouse')
    # cursor connector object
    mycursor=conn.cursor()
    # adds quotes to stock name so MYSQL has no problems with unusual symbols
    new = "`" + Stock_name  + "`"
    #creates table for csv file
    mycursor.execute("create table " + new +" (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
    "Date VARCHAR(25), Open VARCHAR(25), High VARCHAR(25), Low VARCHAR(25), Close VARCHAR(25), " \
    "Volume VARCHAR(25), AdjClose VARCHAR(25), Company VARCHAR(25) )")
    # loads csv file into table
    mycursor.execute("load data infile 'output.csv' into table " + new + " fields terminated by" \
    " ',' lines terminated by '\n' (Date, Open, High, Low, Close, Volume, AdjClose, Company)")
    mycursor.execute("delete from " + new + "limit 1")
    # closes MYSQL connection
    conn.commit()
    conn.close()
    mycursor.close()
    # removes file output.csv
    os.remove(r"C:\mysql\data\datawarehouse\output.csv")
    
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


           

# a list (will contain errors from loading text file)
errors = []
# creates directory 'datawarehouse' and database 'datawarehouse' if they do not exist
create_directory_and_database()


# creates PyQt4 Application object
app = QApplication(sys.argv)
# creates window object
window = QWidget()
# sets window title to "Stock Datawarehouse"
window.setWindowTitle("Stock Datawarehouse") 
# sets changes default window icon to my icon
window.setWindowIcon(QIcon("datawarehouse.ico")) 
# sets windows dimensions (height, depth)
window.resize(320, 240)

# creates text that says "Enter a stock symbol"
text = QLabel("Enter a stock symbol", window)
# text is moved to coordinates 21, 30
text.move(21, 30)

# creates text that says "Eg. 12/04/2014"
text_example = QLabel("Eg. 12/04/2014", window)
# text is moved to coordinates 21, 30
text_example.move(160, 84)

# creates textbox for stock symbol to be entered
textbox = QLineEdit(window)
# textbox is moved to coordinates 20, 44
textbox.move(20, 44)

# creates text that says "Enter a start date"
text2 = QLabel("Enter a start date", window)
# text is moved to coordinates 21, 65
text2.move(21, 65)

# creates textbox for start date to be entered
textbox2 = QLineEdit(window)
# textbox is moved to coordinates 20, 79
textbox2.move(20, 79)

# creates text that says "Enter an end date"
text3 = QLabel("Enter an end date", window)
# text moved to coordinates 21, 100
text3.move(21, 100)

# creates textbox for end date to be entered
textbox3 = QLineEdit(window)
# textbox is moved to coordinates 20, 114 
textbox3.move(20, 114)

# creates button object called "Submit"
button = QPushButton('Submit', window)
# moves button (Right, Down)
button.move(200, 180)
# when button's clicked executes function called sumbit
button.clicked.connect(submit)

# creates combobox object
combo_box = QComboBox(window)
# moves combobox to middle of window
combo_box.move(60,31)

# creates combo_box_frequency object
combo_box_frequency = QComboBox(window)
# moves combo_box_frequency to bottom of window
combo_box_frequency.move(20, 139)
# adds values to combo_box_frequency
combo_box_frequency.addItem("daily")
combo_box_frequency.addItem("weekly")
combo_box_frequency.addItem("monthly")

# creates button object called "back_button"
back_button = QPushButton('Back', window)
# moves button (right, up)
back_button.move(30, 30)
# when button's clicked executes function called show_menu_window
back_button.clicked.connect(show_menu_window)

# creates button object called "delete_button"
delete_button = QPushButton('Delete', window)
# moves button (Right, Down)
delete_button.move(200, 180)
# when button's clicked executes function called delete_table
delete_button.clicked.connect(delete_table)

# creates button object called "menu_button_textfile"
menu_button_textfile = QPushButton('Download from text file', window)
# Moves button to middle of window
menu_button_textfile.move(90, 50)
# when button's clicked executes function called show_file_dialog_window
menu_button_textfile.clicked.connect(show_file_dialog_window)

# creates button object called "menu_button_inputfile"
menu_button_inputfile = QPushButton('Download from user input', window)
# moves button to middle of window
menu_button_inputfile.move(85, 75)
# when button's clicked executes function called show_user_input_window
menu_button_inputfile.clicked.connect(show_user_input_window)

# creates button object called "menu_button_show_tables"
menu_button_show_tables = QPushButton('Show stock tables', window)
# Moves button to middle of window
menu_button_show_tables.move(105, 100)
# when button's clicked executes function called show_stock_window
menu_button_show_tables.clicked.connect(show_stock_window)

# hides some of the window objects
text.hide() 
text2.hide()  
text3.hide()
textbox.hide()
textbox2.hide() 
textbox3.hide()
button.hide()
combo_box.hide()
back_button.hide()
delete_button.hide()
combo_box_frequency.hide()
text_example.hide()

# Displays window
window.show()
# Needed so the gui window stays open until user closes it
app.exec_()

