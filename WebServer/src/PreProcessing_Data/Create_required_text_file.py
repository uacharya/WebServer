'''
Created on May 25, 2016

@author: Ujjwal Acharya
'''

class Data():
    
    def __init__(self):
        pass;
    
    
    def create_countries_dictionaries(self,file_path):
        try:
            file_to_read = open(file_path,"r");
            total_countries = file_to_read.readlines();
            
            countries_name_with_abbrevations = {};
            
            for counter in range(len(total_countries)):
                if(counter>1  and total_countries[counter].strip()!=""):
                    required_fields = total_countries[counter].strip().split(" ");
                    key = required_fields[0];
                    value = "";
                    for index in range(len(required_fields)):
                        if(index>0 and required_fields[index]!=""):
                            value = value+required_fields[index]+" ";
                    
                    countries_name_with_abbrevations[key] = value;
            
            return countries_name_with_abbrevations;        
            
        except IOError:
            print("countries name file cannot be found");
            
            
    def create_only_stations_name(self,file_path,countries_name):
        try:
            file_to_read = open(file_path,"r");
            total_lines = file_to_read.readlines();
            
            station_id_into_country_name={};
            
            for counter in range(len(total_lines)):
                
                if(counter>=22):
                    total_fields = total_lines[counter].split(" ");
                    for index in range(len(total_fields)):
                        if(countries_name.get(total_fields[index])!=None):
                            key = total_fields[0]+" "+total_fields[1];
                            value = countries_name.get(total_fields[index]).strip()+",";
                     
                            for station_index in range(index):
                                if(station_index>1 and station_index<index and total_fields[station_index]!=""):
                                    value = value+total_fields[station_index]+" ";
                            
                            station_id_into_country_name[key] = value;
            
            
            print(len(station_id_into_country_name));
            self._write_to_a_file(station_id_into_country_name);

        except IOError:
            print("file was not found");
            
            
    def _write_to_a_file(self,data):
        try:
        
            file_path = "C:/Users/walluser/Global_Weather_Dataset/station_names_into_countries.txt";
            
            file_open = open(file_path,"w");
            
            file_open.write("STN--- WBAN\t COUNTRYSTATION\n");
            
            for key in data:
                file_open.write(key+"\t"+data[key]+"\n");
            
            file_open.flush();
            file_open.close();
        except IOError:
            print("something went wrong while writing");
        

if __name__ == '__main__':
    file_path_stations = "C:/Users/walluser/Global_Weather_Dataset/List_of_stations.txt";
    file_to_get_countries_name = "C:/Users/walluser/Global_Weather_Dataset/country-list.txt";
    
    create_usable_stations_list = Data();
    
    countries_name = create_usable_stations_list.create_countries_dictionaries(file_to_get_countries_name);
    
    create_usable_stations_list.create_only_stations_name(file_path_stations,countries_name);
     
    
    
    