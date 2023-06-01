#non GUI related libs
import csv
from dateutil import tz
import logging
import os
import json
import datetime
import time
import traceback
import re
import pyhaystack
import pandas as pd
import pytz
import requests


from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QCalendarWidget, QLabel, QLineEdit, QComboBox, QMessageBox, QHBoxLayout, QSizePolicy, QSpacerItem
from PyQt5.QtCore import QDate
from PyQt5.QtWidgets import QFileDialog


timezone = pytz.timezone("Australia/Perth")

def writeCallBack(operation, **kwargs):
    return print(operation)


class MyApp(QWidget):
    def __init__(self):
        super().__init__()

        #Initialising these vars, so that all functions seen in this class can access them
        self.startDate = None
        self.endDate = None
        self.enumValue = None
        self.delta = datetime.timedelta(days=1)

        self.timezoneString = "Default"

        self.initUI()


    from PyQt5.QtWidgets import QSizePolicy, QSpacerItem

    def initUI(self):
        vbox = QVBoxLayout()

        hbox_calendars = QHBoxLayout()

        self.cal1 = QCalendarWidget(self)
        self.cal1.setGridVisible(True)
        self.cal1.clicked[QDate].connect(self.showDate1)
        hbox_calendars.addWidget(self.cal1)

        self.cal2 = QCalendarWidget(self)
        self.cal2.setGridVisible(True)
        self.cal2.clicked[QDate].connect(self.showDate2)
        hbox_calendars.addWidget(self.cal2)

        vbox.addLayout(hbox_calendars)

        hbox1 = QHBoxLayout()
        hbox1.addWidget(QLabel("Start Date (YYYY-MM-DD):"))
        self.date_lbl1 = QLineEdit(self)
        date1 = self.cal1.selectedDate()
        self.startDate = date1.toString("yyyy-MM-dd")
        self.date_lbl1.setText(self.startDate)
        hbox1.addWidget(self.date_lbl1)
        vbox.addLayout(hbox1)

        hbox2 = QHBoxLayout()
        hbox2.addWidget(QLabel("End Date (YYYY-MM-DD):"))
        self.date_lbl2 = QLineEdit(self)
        date2 = self.cal2.selectedDate()
        self.endDate = date2.toString("yyyy-MM-dd")
        self.date_lbl2.setText(self.endDate)
        hbox2.addWidget(self.date_lbl2)
        vbox.addLayout(hbox2)

        hbox_enum = QHBoxLayout()  # Create a horizontal box layout
        hbox_enum.addWidget(QLabel("Interval size for data point:"))  # Add the label to this layout

        self.combo = QComboBox(self)
        self.combo.addItem('15min')
        self.combo.addItem('30min')
        self.combo.addItem('1hour')
        self.combo.addItem('1day')
        self.combo.activated[str].connect(self.onActivated)
        hbox_enum.addWidget(self.combo)  # Add the combo box to this layout

        hbox_enum.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Expanding, QSizePolicy.Minimum))  # Add a spacer

        vbox.addLayout(hbox_enum)  # Add the entire horizontal box layout to the main vertical layout

        self.enum_lbl = QLabel(self)
        self.enumValue = " "  # Default enum
        self.enum_lbl.setText(self.enumValue)

        vbox.addWidget(self.enum_lbl)

        # new CHANGES START
        hbox_enum = QHBoxLayout()  # Create a horizontal box layout
        hbox_enum.addWidget(QLabel("Timezone to use for CSV template creation: "))  # Add the label to this layout

        self.new_combo = QComboBox(self)  # Declare the new QComboBox
        self.new_combo.addItem('Perth')
        self.new_combo.addItem('Melbourne')
        self.new_combo.addItem('Sydney')
        self.new_combo.addItem('Brisbane')
        self.new_combo.addItem('Adelaide')
        self.new_combo.activated[str].connect(self.onNewActivated)  # Connect a method to the activated signal
        hbox_enum.addWidget(self.new_combo)  # Add the new QComboBox to the layout

        hbox_enum.addSpacerItem(QSpacerItem(0, 0, QSizePolicy.Expanding, QSizePolicy.Minimum))  # Add a spacer

        vbox.addLayout(hbox_enum)  # Add the entire horizontal box layout to the main vertical layout
        # new CHANGES END

        self.btn = QPushButton('Create CSV template!', self)
        self.btn.clicked.connect(self.btnUpload_clicked)
        vbox.addWidget(self.btn)

        self.btn_load_csv = QPushButton('Choose pre-prepared CSV to upload to SkySpark', self)
        self.btn_load_csv.clicked.connect(self.btnLoadCsv_clicked)
        vbox.addWidget(self.btn_load_csv)

        self.setLayout(vbox)

        self.setWindowTitle('CSV data upload template Maker for SkySpark')
        self.setGeometry(300, 300, 800, 600)
        self.show()

    def onNewActivated(self, text):
        self.timezoneString = text
        print(f'New timezone value: {self.timezoneString}')

    def loadCsv(self, file_path):
        # Load the selected CSV file
        df = pd.read_csv(file_path)
        print(df)

    def btnLoadCsv_clicked(self):
        # Open a file dialog and get the selected file path
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")

        if file_path:
            # If a file was selected, load it here
            print(f"Loaded CSV file: {file_path}")
            # Get the file name from the file path
            file_name = os.path.basename(file_path)
            # Get the first row of the CSV file
            first_row = self.getFirstRow(file_path)
            # Show the confirmation message box
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText(f"You have selected {file_name}. Data in this file will be uploaded to permanently to SkySpark to the points {first_row} and cannot be undone. Do you wish to proceed?")
            msg.setWindowTitle("Warning")
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            result = msg.exec_()
            if result == QMessageBox.Yes:
                self.loadCsv(file_path)
        else:
            print("No CSV file selected.")

    def getFirstRow(self, file_path):
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            first_row = next(reader)
        return first_row

    def loadCsv(self, file_path):
        # Load the selected CSV file, as previous safety checks have been passed
        df = pd.read_csv(file_path)
        print(df)

        # Extract column names except for the 'ts' column
        columns = df.columns[1:]

        # Initialize lists for meter_name and point_types
        meter_name = []
        point_types = []
        unmodified_col_names = []  # unmodified_col_names will be used to iterate through dataframe, for eventual hisWrites

        # create counter to keep track of location in lists
        cur_list_index = 0;

        # Iterate over the columns and split the strings
        for col in columns:
            unmodified_col_names.append(col)  # Save the original column name
            meter, point_type = col.split('*')
            meter_name.append(meter)
            point_types.append(point_type)

        # Print the lists
        print("meter_name =", meter_name)
        print("point_types =", point_types)
        print("unmodified_col_names =", unmodified_col_names)


        # Extract column names except for the 'ts' column
        columns = df.columns[1:]

        # TODO connect to SkySpark server + advisory project
        # Connect to SkySpark server + advisory project
        try:
            session = pyhaystack.connect(implementation='skyspark', uri='https://analytics.ecosavewatch.com.au',
                                         username='erol_ewatch', password='TBKYD-V9FBV-ZA27G',
                                         project='catholicSchool', pint=True)
            print("successfully connected!")
        except:
            print("Failed to connect, trying to connect again...")
            session = pyhaystack.connect(implementation='skyspark', uri='https://analytics.ecosavewatch.com.au',
                                         username='erol_ewatch', password='TBKYD-V9FBV-ZA27G',
                                         project='catholicSchool', pint=True)
            print("successfully connected, from reattempt!")

        # Iterate over the columns and create temporary dataframes
        for col in columns:
            temp_df = df[['ts', col]]

            # DEBUG Print out temporary dataframe
            print(temp_df)

            # TODO getEval pyhaystack calls to "getPoint_python, etc to find correct point to write to
            # make sure this is a GLOBAL VARIABLE?

            # Constructing the string for pyhaystack call
            # eg. getPoint_python("
            str_for_eval = 'getPoint_python("' + str(meter_name[cur_list_index]) + '","' + str(
                point_types[cur_list_index]) + '")'
            print("str_for_eval = ")
            print(str_for_eval)

            # Use constructed string to determine point to write to
            try:
                print("trying to find the correct point to write to.....")
                op = session.get_eval(str_for_eval)
                op.wait(timeout=20)
            except pyhaystack.util.state.NotReadyError:
                op = session.get_eval(str_for_eval)
                op.wait(timeout=20)
            except:
                op = session.get_eval(str_for_eval)
                op.wait(timeout=20)
            else:
                skyspark_returned_grid = op.result
                # print("skyspark_returned_grid = ", end="")  # uncomment to get point diagnostics
                # print(skyspark_returned_grid)

                skyspark_point = skyspark_returned_grid[0]["id"].name
                # print("skyspark_point = ", end="")
                # print(skyspark_point)  # uncomment to get point diagnostics
                # print('\n')

            # TODO: iterate through "temp_df"
            # Iterate through each row of temp_df
            for _, row in temp_df.iterrows():
                current_ts_raw = row[0]
                val_to_write_to_ss = row[1]
                # DEBUG print out current raw timestamp and associated value to write to SkySpark
                # print(f"current_ts_raw: {current_ts_raw}, val_to_write_to_ss: {val_to_write_to_ss}")


                # Define the format of the date and time string
                date_format = "%Y-%m-%dT%H:%M:%S%z"

                # Remove the timezone name (e.g., Brisbane) using regex
                current_ts_raw_no_tz_name = re.sub(r'\s+\w+$', '', current_ts_raw)

                # Parse the current_ts_raw and save it to the cur_date variable
                cur_date = datetime.datetime.strptime(current_ts_raw_no_tz_name, date_format)

                # Convert cur_date to the desired timezone (if necessary)
                desired_timezone = pytz.timezone("Australia/Perth")
                cur_date_converted = cur_date.astimezone(desired_timezone)

                # Remove the timezone information to create a naive datetime object
                cur_date_naive = cur_date_converted.replace(tzinfo=None)


                # Write row to SkySpark, etc
                skyspark_series = []
                skyspark_series.append([cur_date_naive, val_to_write_to_ss])
                op = session.his_write_series(skyspark_point, skyspark_series,
                                              callback=writeCallBack)  # Removed tz="Perth"
                # time.sleep(0.3)

                print("Sending the following to skyspark... skyspark_series = ")
                print(skyspark_series)

                op.wait(timeout=20)
                folio_write_response = op.result
                print("folio_write_response = ")
                print(folio_write_response)

            print("Finished processing current dataframe")

            # Increment index so next pair of ts and datacolumn can be processess
            cur_list_index = cur_list_index + 1

        # Finished uploading data, show dialog box to confirm to user...
        self.showUploadCompleteDialog()



    def showUploadCompleteDialog(self):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setText("Data upload to SkySpark complete")
        msg.setWindowTitle("Upload Complete")
        msg.setStandardButtons(QMessageBox.Ok)
        msg.exec_()

    def showDate1(self, date):
        self.date_lbl1.setText(date.toString("yyyy-MM-dd"))

    def showDate2(self, date):
        self.date_lbl2.setText(date.toString("yyyy-MM-dd"))

    def onActivated(self, text):
        self.enum_lbl.setText(text)

    def generateCsvTemplate(self):
        if os.path.isfile("interval_data_template.csv"):
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText("The file 'interval_data_template.csv' already exists in the local directory. If you proceed it will be overwritten. Do you want to overwrite it?")
            msg.setWindowTitle("Warning")
            msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            result = msg.exec_()

            if result == QMessageBox.No:
                return

        print('generateCsvTemplate - Start Date:', self.startDate)
        print('generateCsvTemplate - End Date:', self.endDate)
        print('generateCsvTemplate - Enum:', self.enumValue)

        if (self.enumValue == "1day"):
            self.delta = datetime.timedelta(days=1)
        elif (self.enumValue == "1hour"):
            self.delta = datetime.timedelta(hours=1)
        elif (self.enumValue == "30min"):
            self.delta = datetime.timedelta(minutes=30)
        elif (self.enumValue == "15min"):
            self.delta = datetime.timedelta(minutes=15)

        # Timezone will get overridden, so this shouldn't matter
        local_timezone = tz.gettz("Australia/Brisbane")

        start_date = self.startDate
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=local_timezone)

        end_date = self.endDate
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=local_timezone) + datetime.timedelta(
            days=1)

        data = []

        current_date = start_date

        with open("skyspark_point_names.txt", "r") as file:
            column_names = [line.strip() for line in file.readlines()]

        interval_count = [0] * len(column_names)
        while current_date < end_date:
            local_time = current_date.strftime("%Y-%m-%dT%H:%M:%S%z")

            #row = [f"{local_time} Brisbane"] + interval_count
            row = [f"{local_time} {self.timezoneString}"] + interval_count  #testing user selectable timezone, rather than hardcoded "Brisbane"

            data.append(row)

            current_date += self.delta

        # Create and write to CSV file
        with open("interval_data_template.csv", "w", newline="") as csvfile:
            header = "ts," + ",".join(column_names) + "\n"
            csvfile.write(header)
            for row in data:
                csvfile.write(",".join([str(x) for x in row]) + "\n")

        print("CSV file created successfully.")

    def btnUpload_clicked(self):
        # When the 'Upload!' button is clicked, check if the second date is earlier than the first
        date1 = QDate.fromString(self.date_lbl1.text(), "yyyy-MM-dd")
        date2 = QDate.fromString(self.date_lbl2.text(), "yyyy-MM-dd")
        if date2 < date1:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Error: End date cannot be earlier than the Start date. Or another error occurred when creating the CSV template!")
            msg.setWindowTitle("Error")
            msg.exec_()
        else:
            self.startDate = self.date_lbl1.text()
            self.endDate = self.date_lbl2.text()
            self.enumValue = self.enum_lbl.text()

            print('Start Date:', self.startDate)
            print('End Date:', self.endDate)
            print('Enum:', self.enumValue)

            #call csv genenation function now that we have obtained startDate, endDate and enum
            self.generateCsvTemplate()  # Call the new function here

if __name__ == '__main__':
    import sys

    app = QApplication(sys.argv)
    ex = MyApp()
    sys.exit(app.exec_())