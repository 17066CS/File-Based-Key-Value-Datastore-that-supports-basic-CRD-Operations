from datastore import *

ds = DataStoreClass()#Class Datastore called

# Creation operation
ds.create('New Delhi', 'India Gate', 120)

# Read Operation
print(ds.read('New Delhi'))

# Delete Operation
ds.delete('New Delhi')

#ds.create('New Delhi', 'India Gate', 120)
#to display the error

