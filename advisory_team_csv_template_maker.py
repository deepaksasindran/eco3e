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

timezone = pytz.timezone("Australia/Perth")


def writeCallBack(operation, **kwargs):
    return print(operation)

def main():

    #Timezone will get overridden
    local_timezone = tz.gettz("Australia/Brisbane")

    start_date = input("Please enter the start date (YYYY-MM-DD): ")
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=local_timezone)

    end_date = input("Please enter the end date (YYYY-MM-DD): ")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=local_timezone) + datetime.timedelta(days=1)

    desired_interval = input("Please enter the desired interval (15min, 30min, 1hr, 1day): ")



    if desired_interval == "15min":
        delta = datetime.timedelta(minutes=15)
    elif desired_interval == "30min":
        delta = datetime.timedelta(minutes=30)
    elif desired_interval == "1hr":
        delta = datetime.timedelta(hours=1)
    elif desired_interval == "1day":
        delta = datetime.timedelta(days=1)
    else:
        print("Invalid interval. Please try again.")
        return

    data = []

    current_date = start_date
    with open("skyspark_point_names.txt", "r") as file:
        column_names = [line.strip() for line in file.readlines()]

    interval_count = [0] * len(column_names)
    while current_date < end_date:
        local_time = current_date.strftime("%Y-%m-%dT%H:%M:%S%z")
        row = [f"{local_time} Brisbane"] + interval_count
        data.append(row)

        current_date += delta

    # Create and write to CSV file
    with open("interval_data_template.csv", "w", newline="") as csvfile:
        header = "ts," + ",".join(column_names) + "\n"
        csvfile.write(header)
        for row in data:
            csvfile.write(",".join([str(x) for x in row]) + "\n")

    print("CSV file created successfully.")

    ##pause execution, until user fills out template
    input("Please load in your data into the generated template, then save it (in the same folder) with the file name 'interval_data_actual.csv' Then press Enter to continue...")

    # Read the CSV file
    file_path = "interval_data_actual.csv"
    df = pd.read_csv(file_path, nrows=0)

    # Extract column names except for the 'ts' column
    columns = df.columns[1:]

    # Initialize lists for meter_name and point_types
    meter_name = []
    point_types = []
    unmodified_col_names = [] # unmodified_col_names will be used to iterate through dataframe, for eventual hisWrites

    #create counter to keep track of location in lists
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
    print("unmodified_col_names =",unmodified_col_names)


    #create seperate dataframes,
    # Read the CSV file
    file_path = "interval_data_actual.csv"
    df = pd.read_csv(file_path)

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


        #DEBUG Print out temporary dataframe
        print(temp_df)





        #TODO getEval pyhaystack calls to "getPoint_python, etc to find correct point to write to
        #make sure this is a GLOBAL VARIABLE?

        #Constructing the string for pyhaystack call
        #eg. getPoint_python("
        str_for_eval = 'getPoint_python("' + str(meter_name[cur_list_index]) + '","' + str(point_types[cur_list_index]) + '")'
        print("str_for_eval = ")
        print(str_for_eval)

        #Use constructed string to determine point to write to
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
            #print("skyspark_returned_grid = ", end="")  # uncomment to get point diagnostics
            #print(skyspark_returned_grid)

            skyspark_point = skyspark_returned_grid[0]["id"].name
            #print("skyspark_point = ", end="")
            #print(skyspark_point)  # uncomment to get point diagnostics
            # print('\n')


        #TODO: iterate through "temp_df"
        # Iterate through each row of temp_df
        for _, row in temp_df.iterrows():
            current_ts_raw = row[0]
            val_to_write_to_ss = row[1]
            #DEBUG print out current raw timestamp and associated value to write to SkySpark
            #print(f"current_ts_raw: {current_ts_raw}, val_to_write_to_ss: {val_to_write_to_ss}")

            #construct cur_date from parsing current_ts_raw

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

            # DEBUG Print the cur_date datetime object
            #print(cur_date)

            # Write row to SkySpark, etc
            skyspark_series = []
            skyspark_series.append([cur_date_naive, val_to_write_to_ss])
            op = session.his_write_series(skyspark_point, skyspark_series, callback=writeCallBack)  # Removed tz="Perth"
            #time.sleep(0.3)

            print("Sending the following to skyspark... skyspark_series = ")
            print(skyspark_series)

            op.wait(timeout=20)
            folio_write_response = op.result
            print("folio_write_response = ")
            print(folio_write_response)

        print("Finished processing current dataframe")

        #Increment index so next pair of ts and datacolumn can be processess
        cur_list_index = cur_list_index + 1


if __name__ == "__main__":
    main()
