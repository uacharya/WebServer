'''
Created on May 26, 2016

@author: Ujjwal Acharya
'''
from Create_required_text_file import Data;

if __name__ == '__main__':
    
    file_path_of_total_data = "C:\\Users\\walluser\\bash_scripts\\1972_combined.txt";
    
    file_path_of_country_and_station_name_with_atrributes = "C:\\Users\\walluser\\Global_Weather_Dataset\\station_names_into_countries.txt";
    
    new_object = Data();
    
    new_object.create_only_required_data(file_path_of_total_data, file_path_of_country_and_station_name_with_atrributes);
    
    
    
    