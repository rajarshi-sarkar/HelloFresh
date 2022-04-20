import argparse
import pandas
import IcePackAssignment
from os import path
from pandas import testing

def read_csv(csv_location):
    return pandas.read_csv(csv_location, header=0)

def input_file_check():

    '''Validate if both input files Boxes and Temperature_brands are present or not- Optional Test'''

    print("input file Boxes present-",str(path.isfile('./input_data/Boxes.csv')))
    print("input file Temperature_brands present-",str(path.isfile('./input_data/Temperature_bands.csv')))     

def check_add_pouch_count(test_dataset, test_temperature_bands):
    '''Validate if correct ice pac count is retrived after API calls and ice pack coount assignment- Mandatory Test'''
    output=[]  
    expected_result=[]  
    test_dataset_df=read_csv(test_dataset)
    test_temperature_bands_df=read_csv(test_temperature_bands)
    expected_result=test_dataset_df['pouch_count']
    for i, row in test_dataset_df.iterrows():
        output.append(IcePackAssignment.add_pouch_count(row,test_temperature_bands_df))
    testing.assert_series_equal(pandas.Series(expected_result),pandas.Series(output),check_names=False)      

def check_generate_transformed_data(test_dataset, output_location):
    test_dataset_df=read_csv(test_dataset)
    output_file=IcePackAssignment.generate_transformed_data(test_dataset_df,output_location)
    output=read_csv(output_file)
    #print(output)
    expected_result=test_dataset_df[['box_id','pouch_count']]
    #print(expected_result)
    testing.assert_frame_equal(output,expected_result,check_index_type=False) 


if __name__== "__main__":
    '''Execution starts here'''
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--test_temperature_bands', required=False, default="./unit_test/Input/test_temperature_bands.csv")
    parser.add_argument('--test_dataset', required=False, default="./unit_test/Input/test_dataset.csv")
    parser.add_argument('--output_location', required=False, default="./unit_test/Output/")
    args = vars(parser.parse_args())
    input_file_check()
    check_add_pouch_count(args['test_dataset'], args['test_temperature_bands'])   
    check_generate_transformed_data(args['test_dataset'],args['output_location'])