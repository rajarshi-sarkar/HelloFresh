"""-----------------------------------------------------------------------------------------------------------------------
Description:
    This program assigns number of ice packs required for a package bsed on temperature of a postcode on a delivery date
    This program uses googleapis and meteostat api for deducing temperature
version- v1.0
created by-Rajarshi Sarkar
creted date-20th April 2022
Review date-
Review by-
--------------------------------------------------------------------------------------------------------------------------"""

import argparse
import pandas
import requests
import datetime
import logging
import time
from datetime import datetime as datetime

#--------- Set API Keys -----------
google_api_key="AIzaSyAcUySoPfcSDiX3f-B6_exj235d1I_AHL8"
meteostat_api_key="a6868d1abbmshfee492c5327d7bep172ea5jsn96fc15490634"
#--------- Logging Configuration ----------
logging.basicConfig(filename="./app_log/applog_{}.log".format(datetime.now().strftime("%d%m%Y_%H_%M_%S")),
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)   
#------------ API call configuration -------------               
meteostat_url = "https://meteostat.p.rapidapi.com/point/daily"
google_api_url_first="https://maps.googleapis.com/maps/api/geocode/json?address="
google_api_url_last="+United+Kingdom&key="+google_api_key
BACKOFF_TIME = 30
meteostat_headers = {
	    "X-RapidAPI-Host": "meteostat.p.rapidapi.com",
	    "X-RapidAPI-Key": meteostat_api_key
    }

def read_csv(csv_location):
    # Function to read input CSV files and return same as Pandas Dataframe
    return pandas.read_csv(csv_location, header=0)


def add_pouch_count(row,temperature_bands_df):
    ''' Function to deduce temperature of a speciic postcode on a specific date
        And, assign ice pack count based on deduced temperature and Cool Pouch Size
        Input- rows from dataframe containing postcode,delivery_date,cool pack size,box_id(uses-Boxes_df rows)
               temperature_bands Dataframe containing temperature ranges and associated ice pack size and count 
        Output- returns temperature for each input row containing delivery_date and postcode'''   
    
    lat=None
    lng=None
    cool_pouch_size=pandas.isna(row['Cool Pouch Size'])
    post_code=pandas.isna(row['postcode'])
    delivery_date=pandas.isna(row['delivery_date'])

    #Check if mandatory values are none
    if (post_code == True) or (cool_pouch_size == True) or (delivery_date ==True):
        logger.warning("{}, invalid data".format(row['box_id'])) 
        return(None)       
    else:
        cool_pouch_size=row['Cool Pouch Size']
        outward_code=row['postcode'][:-3]
        delivery_date=datetime.strptime(row['delivery_date'],'%d/%m/%Y %H:%M').date()

    #Generate postcode co-ordinate from google API
        try:
            coordinate_response = requests.get(google_api_url_first+outward_code+google_api_url_last)
            resp_json = coordinate_response.json()
        except Exception as e:
            logger.warning(e)
        if resp_json['status'] == 'OVER_QUERY_LIMIT':
            logger.warning('OVER_QUERY_LIMIT')
            time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes if over the query limit
        else:    
            if resp_json['status'] != 'OK':
                logger.warning('check Status')
                return(None)
            else:    
                coordinate=resp_json['results'][0]['geometry']['location']
                lat=coordinate['lat']
                lng=coordinate['lng']   
                querystring = {"lat":lat,"lon":lng,"start":delivery_date,"end":delivery_date}

    #Generate postcode temperature (delivery date temperature) from meteostat API
                try:
                    temp_response = requests.get(meteostat_url, headers=meteostat_headers, params=querystring)
                
                except Exception as e:
                    logger.warning(e)
                    if("exceeded the MONTHLY quota" in temp_response.json()['message']):
                        logger.warning('meteostat_api_key OVER_QUERY_LIMIT')
                temperature=temp_response.json()['data'][0]['tmax']  

    #Find temperature_min and temperature_max range based on temperature variable
                temperature_bands_df.loc[(temperature_bands_df['temperature_min'] <= temperature) & (temperature_bands_df['temperature_max'] > temperature),'temp_range']='true'
                temperature_bands_df.loc[(temperature_bands_df['temperature_min'] > temperature) | (temperature_bands_df['temperature_max'] < temperature),'temp_range']='false'
                temp_temperature_bands_df=temperature_bands_df.loc[temperature_bands_df['temp_range'] == 'true']
                pouch_count=temp_temperature_bands_df[cool_pouch_size].values[0]
            return (pouch_count)


def generate_transformed_data(boxes_df, output_location):
    ''' Function to generate final dataframe followed by output CSV file containing postcode and assigned ice pack count
        Input- Intermediate Dataframe containing box_id,delivery_date,cool pouch count,postcode,temperature,pouch_count(uses-intermediate DF created)
        Output- generate final output CSV file containing postcode and assigned ice pack count, return the file name''' 
    
    #Generate the file location
    file_name=output_location + 'result_' + datetime.now().strftime("%d%m%Y_%H_%M_%S")+'.csv'
    
    #Filter required columns from input dataframe and write to CSV file
    data=[boxes_df['box_id'],boxes_df['pouch_count']]
    headers = ["box_id", "pouch_count"]
    df_final = pandas.concat(data, axis=1, keys=headers)
    df_final.to_csv(file_name, index=False)
    return(file_name)

def run_transformations(Temperature_bands, Boxes, output_location):
    ''' Function to validate input file columns and call other available functions
        Input- Raw input file and output file location s
        Output- na'''
    
    # validate input file quality    
    boxes_column=['box_id','delivery_date','Cool Pouch Size','postcode'] # mandatory fields for transformation from boxes raw file
    Temperature_bands_column=['temperature_min','temperature_max']# mandatory fields for transformation from Temperature_bands raw file
    temperature_bands_df=read_csv(Temperature_bands)
    boxes_df=read_csv(Boxes)
    for col in (Temperature_bands_column):
        if col not in temperature_bands_df.columns:
            logger.error("{}column missing in file-{}".format(col,Temperature_bands))
    for col in (boxes_column):
        if col not in boxes_df.columns:
            logger.error("{}column missing in file-{}".format(col,Boxes))
    
    
    #Call add_pouch_count function to update the dataframe then call generate_transformed_data to generate the final o/p
    boxes_df=boxes_df.head(10)#Please use it to limit API calls
    boxes_df['pouch_count']=boxes_df.apply(lambda row:add_pouch_count(row,temperature_bands_df),axis=1)
    output_file=generate_transformed_data(boxes_df,output_location)
    print("output_file",output_file)
    logger.info("output_file-{}".format(output_file))

if __name__ == "__main__":   
    
    #Execution starts here
    parser = argparse.ArgumentParser(description='DataTransformation')
    parser.add_argument('--Temperature_bands', required=False, default="./input_data/Temperature_bands.csv")
    parser.add_argument('--Boxes', required=False, default="./input_data/Boxes.csv")
    parser.add_argument('--output_location', required=False, default="./output_data/")
    args = vars(parser.parse_args())
    run_transformations(args['Temperature_bands'], args['Boxes'],args['output_location'])
