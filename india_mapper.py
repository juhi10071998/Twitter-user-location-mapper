import sys

import unicodedata
import argparse
import os
import re
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType
spark = SparkSession.builder \
        .appName('row_counter') \
        .getOrCreate()

def user_lookup(*args):
    ## here args is the dictionaries mapping the location to whatever info you can get in terms of city, district, state, country.
    ## we start looking up from the most granular level, if we have something like Mumbai, India -> we would like to resolve it at the city level and not at the country level becuase from city we get all teh info about city, district, state, country. from country we only know which country.
    ## Order followed in resolving

    # city_district_map --> gives more info on resolving as compared to just city 
    # city_state_map   
    # city_state_country_map       
    # city_country_map
    # city_map
    # district_state_map
    # district_state_country_map 
    # district_country_map
    # state_country_map
    # country_map  --> useful for country level analysis.
    # city_alias_map  --> Bombay -> Mumbai -> lod	okup in city_map. Design choice to put in end as the cases are rare.
    # city_alias_clean_map

    def f(user_location,coord):

        location = str(user_location)

    ## first check if coordinate is presnet, if yes resolve with that.


#        if coord is not None:
#            if coord != '':
#                if any(c.isalpha() for c in coord) is False:
#                    clean_coord = coord[1:-1].split(',')
#                    # reverse geocoder taken in long first and then latitue,   
#                    coord_lst = [float(x) for x in clean_coord]
#                    #coord_lst = [85.2134, 75.816]
#                    coord_lst.reverse()
#                    tup = tuple(coord_lst)
#                    return rg.search(tup)[0]['name'] + "," + rg.search(tup)[0]['admin2'] + "," + rg.search(tup)[0]['admin1'] + "," + rg.search(tup)[0]['cc']

        exclude_list = ['America','USA','U.S.A','United States of America','CA','New York','UK','Canada','Aruba', 'Afghanistan', 'Angola', 'Anguilla', 'Åland Islands', 'Albania', 'Andorra', 'United Arab Emirates', 'Argentina', 'Armenia', 'American Samoa', 'Antarctica', 'French Southern Territories', 'Antigua and Barbuda', 'Australia', 'Austria', 'Azerbaijan', 'Burundi', 'Belgium', 'Benin', 'Bonaire, Sint Eustatius and Saba', 'Burkina Faso', 'Bangladesh', 'Bulgaria', 'Bahrain', 'Bahamas', 'Bosnia and Herzegovina', 'Saint Barthélemy', 'Belarus', 'Belize', 'Bermuda', 'Bolivia, Plurinational State of', 'Brazil', 'Barbados', 'Brunei Darussalam', 'Bhutan', 'Bouvet Island', 'Botswana', 'Central African Republic', 'Canada', 'Cocos (Keeling) Islands', 'Switzerland', 'Chile', 'China', "Côte d'Ivoire", 'Cameroon', 'Congo, The Democratic Republic of the', 'Congo', 'Cook Islands', 'Colombia', 'Comoros', 'Cabo Verde', 'Costa Rica', 'Cuba', 'Curaçao', 'Christmas Island', 'Cayman Islands', 'Cyprus', 'Czechia', 'Germany', 'Djibouti', 'Dominica', 'Denmark', 'Dominican Republic', 'Algeria', 'Ecuador', 'Egypt', 'Eritrea', 'Western Sahara', 'Spain', 'Estonia', 'Ethiopia', 'Finland', 'Fiji', 'Falkland Islands (Malvinas)', 'France', 'Faroe Islands', 'Micronesia, Federated States of', 'Gabon', 'United Kingdom', 'Georgia', 'Guernsey', 'Ghana', 'Gibraltar', 'Guinea', 'Guadeloupe', 'Gambia', 'Guinea-Bissau', 'Equatorial Guinea', 'Greece', 'Grenada', 'Greenland', 'Guatemala', 'French Guiana', 'Guam', 'Guyana', 'Hong Kong', 'Heard Island and McDonald Islands', 'Honduras', 'Croatia', 'Haiti', 'Hungary', 'Indonesia', 'Isle of Man', 'British Indian Ocean Territory', 'Ireland', 'Iran, Islamic Republic of', 'Iraq', 'Iceland', 'Israel', 'Italy', 'Jamaica', 'Jersey', 'Jordan', 'Japan', 'Kazakhstan', 'Kenya', 'Kyrgyzstan', 'Cambodia', 'Kiribati', 'Saint Kitts and Nevis', 'Korea, Republic of', 'Kuwait', "Lao People's Democratic Republic", 'Lebanon', 'Liberia', 'Libya', 'Saint Lucia', 'Liechtenstein', 'Sri Lanka', 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Macao', 'Saint Martin (French part)', 'Morocco', 'Monaco', 'Moldova, Republic of', 'Madagascar', 'Maldives', 'Mexico', 'Marshall Islands', 'North Macedonia', 'Mali', 'Malta', 'Myanmar', 'Montenegro', 'Mongolia', 'Northern Mariana Islands', 'Mozambique', 'Mauritania', 'Montserrat', 'Martinique', 'Mauritius', 'Malawi', 'Malaysia', 'Mayotte', 'Namibia', 'New Caledonia', 'Niger', 'Norfolk Island', 'Nigeria', 'Nicaragua', 'Niue', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'New Zealand', 'Oman', 'Pakistan', 'Panama', 'Pitcairn', 'Peru', 'Philippines', 'Palau', 'Papua New Guinea', 'Poland', 'Puerto Rico', "Korea, Democratic People's Republic of", 'Portugal', 'Paraguay', 'Palestine, State of', 'French Polynesia', 'Qatar', 'Réunion', 'Romania', 'Russian Federation', 'Rwanda', 'Saudi Arabia', 'Sudan', 'Senegal', 'Singapore', 'South Georgia and the South Sandwich Islands', 'Saint Helena, Ascension and Tristan da Cunha', 'Svalbard and Jan Mayen', 'Solomon Islands', 'Sierra Leone', 'El Salvador', 'San Marino', 'Somalia', 'Saint Pierre and Miquelon', 'Serbia', 'South Sudan', 'Sao Tome and Principe', 'Suriname', 'Slovakia', 'Slovenia', 'Sweden', 'Eswatini', 'Sint Maarten (Dutch part)', 'Seychelles', 'Syrian Arab Republic', 'Turks and Caicos Islands', 'Chad', 'Togo', 'Thailand', 'Tajikistan', 'Tokelau', 'Turkmenistan', 'Timor-Leste', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Tuvalu', 'Taiwan, Province of China', 'Tanzania, United Republic of', 'Uganda', 'Ukraine', 'United States Minor Outlying Islands', 'Uruguay', 'United States', 'Uzbekistan', 'Holy See (Vatican City State)', 'Saint Vincent and the Grenadines', 'Venezuela, Bolivarian Republic of', 'Virgin Islands, British', 'Virgin Islands, U.S.', 'Viet Nam', 'Vanuatu', 'Wallis and Futuna', 'Samoa', 'Yemen', 'South Africa', 'Zambia', 'Zimbabwe']
        res = any(ele in location for ele in exclude_list)
        ## remove city from name, if the location mentions any of the terms in the exclude_list remove them in the beginning itself, saves a lot of computtaion as these are already calculated.
        ## the dictionaries here are specific to India, city, district, state and country mapping. 
        if res:
            return " , , , "

        ## For the counry we lookup by India, replace other country abb of India by India.
        location = location.replace("Bharat", "India")
        location = location.replace("Hindustan", "India")
        location = location.replace("Indian", "India")
        location = location.replace("IncredibleIndia","India")
        location = location.replace("City"," ")
        location = location.replace("city"," ")

        ## after cleaning the user_location string, we break it into tokens and then take all the possible combinations of our tokens and lookup in the dictionary.

        location = re.sub('\W+',' ',location)
        location_lower = location.lower()
        location_lower = location_lower.replace("india","India")
        location_lower = location_lower.split()
        location = location.split()
        le = len(location)

        #print(location)
        for value in args:
            ### If the given location entered by the user is a city alias name, then
            ## Get the city corresponding to the alias (eg: Bombay is mapped to Mumbai)
            ## once you get the actual city lookuo in the city_map. if it is a fampus city in world, it will be present in the city_map. 
            ## Mumbai gets resolved to Mumbai in India and not Mumbai in US.
            if value == 'city_alias_map' or value == 'city_alias_clean_map':
                location = str(x)
                location = re.sub('\W+',' ',location)
                location = location.split()
                le = len(location)
                ct = ''
                for i in range(le, 0, -1):
                    for j in range(0,i):
                        subloc = ' '.join(location[j:i])
                        if subloc in value.keys():
                            ct =  value.get(subloc)

                if ct != '':
                    city = re.sub('\W+',' ', ct)
                    city = city.split()
                    le = len(city)
                    for i in range(le, 0, -1):
                        for j in range(0,i):
                            subloc = ' '.join(city[j:i])
                            if subloc in city_map.keys():
                                return city_map.get(subloc)
            ### for others, we simply iterate over all other combinations, and check for lower and upper case both.
            else:
                for case in range(0,2):
                    if case == 0:
                        for i in range(le, 0, -1):
                            for j in range(0,i):
                                subloc = ' '.join(location[j:i])
                                if subloc in value.keys():
                                    return value.get(subloc)
                    if case == 1:
                        for i in range(le, 0, -1):
                            for j in range(0,i):
                                subloc = ' '.join(location_lower[j:i])
                                if subloc in value.keys():
                                    return value.get(subloc)

        return " , , , "
    return F.udf(f)

def main():
    parser = argparse.ArgumentParser(description='Create tables for countries')
    parser.add_argument(
       'table', help='Table name (a CSV file in HDFS) containing Twitter data.')
    parser.add_argument(
        'output', help='output file')
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName('Geomap') \
        .getOrCreate()
    # First read input data, on which you have to perform the mapping

    table = spark\
            .read\
            .option("header","true")\
            .option('escape','"')\
            .format("csv").load(args.table)
    table.printSchema() 
     
    ## Read the csv files from hadoop to and create maps and then broadcats them,
    # 1. CITY-DISTRICT MAPPING    
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/city_district_mapping_india.csv")
    mp.show(4)
    city_district_map = {x['city_district']: x['mapped_data'] for x in mp.select('city_district', 'mapped_data').collect()}
      
    # 2. CITY STATE MAPPING 
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/city_state_mapping_india.csv")
    mp.show(4)
    city_state_map = {x['city_state']: x['mapped_data'] for x in mp.select('city_state', 'mapped_data').collect()}

    # 3. CITY STATE COUNTRY MAPPING 
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/city_state_mapping_india.csv")
    mp.show(4)
    city_state_country_map = {x['city_state_country']: x['mapped_data'] for x in mp.select('city_state_country', 'mapped_data').collect()}


   # 4. CITY COUNTRY MAPPING
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/city_country_mapping_india.csv")
    mp.show(4)
    city_country_map = {x['city_country']: x['mapped_data'] for x in mp.select('city_country', 'mapped_data').collect()}

    
    # 5. ONLY CITY MAPPING 
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/city_mapping_india.csv")
    mp.show(4)
    city_map = {x['city']: x['mapped_data'] for x in mp.select('city', 'mapped_data').collect()}

    #6. CITY ALIAS 2 CITY
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/cityAlias2city.csv")
    mp.show(4)
    city_alias_map = {x['city_alias']: x['city'] for x in mp.select('city_alias','city').collect()}

    # 7. CITY ALIAS clean 2 CITY
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/cityAlias2city.csv")
    mp.show(4)
    city_alias_clean_map = {x['city_alias_clean']: x['city'] for x in mp.select('city_alias_clean', 'city').collect()}
    
    #8 district state mapping
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/district_state_India.csv")
    mp.show(4)
    district_state_map = {x['district_state']: x['mapped_data'] for x in mp.select('district_state', 'mapped_data').collect()}
 
 
    # 9. district_state_countryaaa
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/district_state_India.csv")
    mp.show(4)
    district_state_country_map = {x['district_state_country']: x['mapped_data'] for x in mp.select('district_state_country', 'mapped_data').collect()}
 
 
    # 9. district_country
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/district_country_India.csv")
    mp.show(4)
    district_country_map = {x['district_country']: x['mapped_data'] for x in mp.select('district_country', 'mapped_data').collect()}
 
 
    #10. state and state country mapping
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/state_country_India.csv")
    mp.show(4)
    state_country_map  = {x['state_country']: x['mapped_data'] for x in mp.select('state_country', 'mapped_data').collect()}
    
    mp = spark\
     .read\
     .option('header',True)\
     .option('inferSchema',True)\
     .csv("new_Mapping/country_mapping.csv")
    mp.show(4) 
    country_map = {x['country']: x['mapped_data'] for x in mp.select('country', 'mapped_data').collect()}

    output_name = args.output

    ## call the user_lookup to resolve the GEOLOCATION first followed by  user_location, here we pass on the respective dictionaries too.
    table_new = table.withColumn('state_abbreviation', user_lookup(city_district_map, city_state_map, city_state_country_map, city_country_map, city_map, district_state_map, district_state_country_map,district_country_map,  state_country_map, country_map, city_alias_map, city_alias_clean_map)(F.col('user_location'),F.col('coordinates')))

    table_new = table_new.withColumn("user_city", F.split(F.col("state_abbreviation"), ",").getItem(0)).withColumn("user_admin2", F.split(F.col("state_abbreviation"), ",").getItem(1)).withColumn("user_admin1", F.split(F.col("state_abbreviation"), ",").getItem(2)).withColumn("user_country", F.split(F.col("state_abbreviation"), ",").getItem(3))
    table_new_filtered = table_new.filter(table_new["state_abbreviation"].isNotNull())
    table_new_filtered.write.csv(output_name, mode = 'append')
    

if __name__ == "__main__":
    main()
