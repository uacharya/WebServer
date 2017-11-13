'''
Created on May 25, 2016

@author: Ujjwal Acharya
'''
import math



class Data():
    
    def __init__(self):
        pass;
    
    
    def create_countries_dictionaries(self, file_path):
        try:
            file_to_read = open(file_path, "r");
            total_countries = file_to_read.readlines();
            
            countries_name_with_abbrevations = {};
            
            for counter in range(len(total_countries)):
                if(counter > 1  and total_countries[counter].strip() != ""):
                    required_fields = total_countries[counter].strip().split(" ");
                    key = required_fields[0];
                    value = "";
                    for index in range(len(required_fields)):
                        if(index > 0 and required_fields[index] != ""):
                            value = value + required_fields[index] + " ";
                    
                    countries_name_with_abbrevations[key] = value;
                    
            file_to_read.close();
            
            return countries_name_with_abbrevations;        
            
        except IOError:
            print("countries name file cannot be found");
            
            
    def create_only_stations_name(self, file_path, countries_name):
        try:
            file_to_read = open(file_path, "r");
            total_lines = file_to_read.readlines();
            
            station_id_into_country_name = {};
            station_latitiude = {};
            station_longitude = {};
            station_elevation = {};
            
            for counter in range(len(total_lines)):
           
                if(counter >= 22):
                    country_abbreviation = total_lines[counter][43:46].strip();
                
                    if(countries_name.get(country_abbreviation) != None):
                        
                        station_id = total_lines[counter][:12].strip();
                      
                        value = countries_name.get(country_abbreviation).strip() + ",";
                        value = value + total_lines[counter][12:43];
                        
                        station_id_into_country_name[station_id] = value.strip();
                        
                        latitude = total_lines[counter][56:64].strip();
                        longitude = total_lines[counter][64:73].strip();
                        elevation = total_lines[counter][73:81].strip();
                        
                        station_latitiude[station_id] = latitude;
                        station_longitude[station_id] = longitude;
                        station_elevation[station_id] = elevation;              
         
#             print(len(station_id_into_country_name), len(station_latitiude), len(station_longitude), len(station_elevation));

            file_to_read.close();
            
            self._write_to_a_file(station_id_into_country_name, station_latitiude, station_longitude, station_elevation);

        except IOError:
            print("file was not found");
            
            
    def _write_to_a_file(self, station_full_name, station_latitude, station_longitude, station_elevation):
        try:
        
            file_path = "C:/Users/walluser/Global_Weather_Dataset/station_names_into_countries.txt";
            
            file_open = open(file_path, "w");
            
            file_open.write("STN--- WBAN\tCOUNTRYSTATION\tLATITUDE\tLONGITUDE\tELEVATION\n");
            
            for key in station_full_name:
                file_open.write(key + "\t" + station_full_name[key] + "\t" + station_latitude[key] + "\t" + station_longitude[key] + "\t" + station_elevation[key] + "\n");
            
            file_open.flush();
            file_open.close();
        except IOError:
            print("something went wrong while writing");
            
    def create_only_required_data(self, total_dataset, station_name):
        try:
            file_open_for_creating_dictionaries = open(station_name, "r");
            total_list_of_stations = file_open_for_creating_dictionaries.readlines();
            
            countries_name_with_station_id = {};
            station_latitiude = {};
            station_longitude = {};
            station_elevation = {};
            
            for counter in xrange(len(total_list_of_stations)):
                if(counter > 0):
                    total_fields = total_list_of_stations[counter].split("\t");
                    station_id = total_fields[0].split(' ')[0];
                    if not station_id in countries_name_with_station_id:
                        countries_name_with_station_id[station_id] = total_fields[1];
                        station_latitiude[station_id] = total_fields[2];
                        station_longitude[station_id] = total_fields[3];
                        station_elevation[station_id] = total_fields[4];
                    
            
            print("here ");
            self._create_final_global_dataset(total_dataset, countries_name_with_station_id, station_latitiude, station_longitude, station_elevation);
            
        except IOError:
            print("The file was not found for creating dictionaries");
        finally:
            file_open_for_creating_dictionaries.close();
        
    
    def _create_final_global_dataset(self, total_dataset, station_name, latitude, longitude, elevation):
        try:
            dataset_open = open(total_dataset, "r");
        
            output_file = "C:\\Users\\walluser\\final_total_weather_dataset_preprocessed_1113.txt";
            file_to_write_to = open(output_file, "w");
            
            file_to_write_to.write("STN\tSTATION\tYEARMODA\tTEMPERATURE\tDEW\tSEALEVELPRESSURE\tSTATIONPRESSURE\tDENSITY\tVISIBILITY\tWINDSPEED\tMAXWINDSPEED\tWINDGUST\tMAXTEMP\tMINTEMP\tPRECIPITATION\tSNOWDEPTH\tLATITUDE\tLONGITUDE\tELEVATION");
            file_to_write_to.write("\n");
            for line in dataset_open:
                if(station_name.get(line[:6].strip()) != None):
                    name = station_name.get(line[:6].strip()).strip();
                    id = line[:6].strip();

                    temperature_in_kelvin = self.convert_temperature_to_kelvin(line[25:31]);
                    
                    if(temperature_in_kelvin==9999.9):
                        continue;
                    

                    if(float(line[57:64]) == 9999.9):
                        station_pressure = self.get_station_pressure(line[46:53], elevation.get(line[:6].strip()), temperature_in_kelvin);
                        if station_pressure==9999.9:
                            continue;
                        else:
                            station_pressure=station_pressure*100;
                    else:
                        station_pressure = float(line[57:64])*100;
                        
                    wind_velocity=self.convert_to_ms(line[79:84]);
                    
                    if wind_velocity==999.9:
                        continue;
                    
                    density = self.get_the_density(station_pressure, temperature_in_kelvin);    
                    sea_level_pressure_in_pascals = float(line[46:53])*100;
                        
                        
                    file_to_write_to.write(id+"\t"+ name + "\t" + line[14:23]
                        + "\t" + str(temperature_in_kelvin) + "\t" + line[36:42] + "\t" + str(sea_level_pressure_in_pascals) + "\t" + str(station_pressure) + "\t" + str(density) + "\t" + line[69:74] + "\t"
                        + str(wind_velocity) + "\t" + str(self.convert_to_ms(line[89:94])) + "\t" + line[95:101] + "\t" + 
                        str(self.convert_temperature_to_kelvin(line[103:108])) + "\t" + str(self.convert_temperature_to_kelvin(line[111:116]))
                        + "\t" + line[118:123] + "\t" + line[125:131] + "\t" + latitude.get(line[:6].strip()) + "\t" + longitude.get(line[:6].strip()) + "\t" + elevation.get(line[:6].strip()));


            file_to_write_to.flush();
            file_to_write_to.close();
                            
            
        except IOError:
            print("total dataset file was not found");
        finally:
            dataset_open.close();
            
    
    def get_the_density(self, pressure, temperature):
        return pressure / (287.058 * temperature);
        
            
    def convert_to_ms(self, wind_speed):
        if(float(wind_speed) == 999.9):
            return float(999.9)
        else:
            return float(wind_speed) * 0.514444;
        
        
    def convert_temperature_to_kelvin(self, temperature_in_farheheit):
        if(float(temperature_in_farheheit) == 9999.9):
            return float(9999.9);
        else:
            return ((float(temperature_in_farheheit) - 32) * (float(5) / float(9))) + 273.15;
        
    def get_station_pressure(self, sea_level_pressure, elevation, temperature):
        
        if(float(sea_level_pressure) == 9999.9 or not elevation.strip() or temperature==9999.9):
            return float(9999.9);
        else:
            exponent = float(elevation) / float(temperature * 29.263)
            return float(sea_level_pressure) * math.exp(-exponent);
    
            
        

if __name__ == '__main__':
    file_path_stations = "C:/Users/walluser/Global_Weather_Dataset/List_of_stations.txt";
    file_to_get_countries_name = "C:/Users/walluser/Global_Weather_Dataset/country-list.txt";
    
    create_usable_stations_list = Data();
    
    countries_name = create_usable_stations_list.create_countries_dictionaries(file_to_get_countries_name);
    
    create_usable_stations_list.create_only_stations_name(file_path_stations, countries_name);
    

     
    
    
    
