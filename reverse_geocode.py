
import time
import pickle
import requests
import pathlib
import numpy as np
def reverse_geocode(base_url=None,query_params=None,k = 1,num_cpus=1,print_lim=20000,data=None):
    
    
    path = pathlib.Path().absolute()/'reverse-data-map'/'not_present'/'remaining'
    
    lat_long = data[['Latitude','Longitude']]

    start_time = time.time()
    chunk = lat_long.shape[0]//num_cpus
    
    start = None
    
    end = None
    
    prev = start
    print(chunk,start,end)
    indices, zipcodes = [],[]
    index,zipcode = [],[]
    
    latitude,longitude = [],[]
    
    latitudes,longitudes = [],[]
    
    counter = 0
    print(lat_long.shape)
    for i,row in lat_long.iloc[:,:].iterrows():
        counter+=1
        
        if start is None:
            start = i
            prev = start
        
        
        #print(row[0],row[1])
        if np.isnan(row[0]) or np.isnan(row[1]):
            
            continue
        #url = base_url + "point.lon=" + str(row[1]) + "&point.lat=" + str(row[0]) + query_params
        url = base_url + str(row[0]) + "," + str(row[1]) + query_params
        #print(url)
        
        res = requests.request('GET',url).json()
        #print(res)
        
        if counter%print_lim == 0:
            taken = (time.time()-start_time)
            percent_done = (counter)*100/lat_long.shape[0]
            print("{} done after {} on cpu {}".format(percent_done,taken,k))
            
            name= str(prev)+"result"+str(i)+".pkl"
            
            where_save = path / name
            prev = i
            
            with open(where_save, 'wb') as handle:
                pickle.dump(zip(index,zipcode,latitude,longitude), handle, protocol=pickle.HIGHEST_PROTOCOL)
            
            index = []
            zipcode = []
            latitude = []
            longitude = []
            
        features = res['results']
        
        postal_code = None
        
        for feature in features:
            address = feature["address_components"]
            
            for info in address:
                if len(info["types"])==1 and info["types"][0] == "postal_code":
                    
                    postal_code = info["long_name"]
        
        
    
        if postal_code is None:
            #not_present[i] = 1
            print(url)
            print("postcode not present for i = {} on cpu {}".format(i,k))
            continue

       
        
        index.append(i)
        zipcode.append(postal_code)
        indices.append(i)
        zipcodes.append(postal_code)
        
        
        latitudes.append(row[1])
        latitude.append(row[1])
        
        
        longitudes.append(row[0])
        longitude.append(row[0])
        
        
    taken = (time.time()-start_time)
    print(" cpu {} done after {}".format(k,taken))
    return indices,zipcodes,latitudes,longitudes