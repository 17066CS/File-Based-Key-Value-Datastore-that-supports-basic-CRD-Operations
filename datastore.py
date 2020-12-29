import time
import json
from threading import *
import concurrent.futures



class DataStoreClass:
    def __init__(self):
        self.datastore = {} #Dictionary to store data

    # Create operation
    # Use syntax : datastore_obj.create(key, value, time_to_live) where time_to_live is optional and is in seconds
    def create(self, key, value, time_to_live=0):
        t1 = Thread(target=(self.create_method), args=(
            key, value, time_to_live))
        t1.daemon = True
        t1.start()

    # method for performing create operation
    def create_method(self, key, value, time_to_live=0):
        if key in self.datastore:
            print('error: This key already exists.')
        else:
            if len(self.datastore) < 1000000000 and len(value) < 16384:#here constraints of 1GB and 16KB
                if time_to_live == 0:
                    data = {'value': value, 'time_to_live': time_to_live}
                else:
                    data = {'value': value,
                            'time_to_live':  time.time() + time_to_live}
                if len(key) <= 32:#the key must be of 32 charcters checking
                    self.datastore[key] = data
                    # output to file
                    with open("datastore.json", "w") as outfile:#opening json file in write mode
                        outfile.write(json.dumps(self.datastore))
                else:
                    print('The keys must be 32 characters in length')
            else:
                print('error: Memory limit exceeded')

    # Read operation
    # Use syntax : datastore_obj.read(key)
    def read(self, key):
        # Returned value from the utility method is to be returned by this read() method
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self.read_method, key)
            return future.result()

    # method for performing read operation
    def read_method(self, key):
        try:
            with open("datastore.json", "r") as openfile:
                self.datastore = json.load(openfile)
        except Exception:
            print('File not Found.')
        if key not in self.datastore:
            print('error: Given key does not exist in datastore')
            return
        else:
            data = self.datastore[key]
            if data['time_to_live'] != 0:
                if time.time() < data['time_to_live']:
                    return json.dumps({key: self.datastore[key]['value']})
                else:
                    print('error: Time of key ' + key + ' has expired')
                    return
            else:
                data = self.datastore[key]
                return json.dumps({key: self.datastore[key]['value']})

    # Delete operation
    def delete(self, key):
        t3 = Thread(target=(self.delete_method),
                    args=(key,))
        t3.daemon = True
        t3.start()

    #  method for performing delete operation
    def delete_method(self, key):
        try:
            with open("datastore.json", "r") as openfile:
                self.datastore = json.load(openfile)
        except Exception:
            print('File not Found.')
        if key not in self.datastore:
            print('error: Given key does not exist in datastore')
        else:
            data = self.datastore[key]
            if data['time_to_live'] != 0:
                if time.time() < data['time_to_live']:
                    del self.datastore[key]
                    with open("datastore.json", "w") as outfile:
                        outfile.write(json.dumps(self.datastore))
                    print('Success : The record with key ' +
                          key + ' is successfully deleted.')
                else:
                    print('error: Time of key ' + key + ' has expired')
            else:
                del self.datastore[key]
                with open("datastore.json", "w") as outfile:
                    outfile.write(json.dumps(self.datastore))
                print('Success : The record with key ' +
                      key + ' is successfully deleted.')

   

    