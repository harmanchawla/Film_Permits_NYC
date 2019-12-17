import csv
import json
import requests

# Set pincode parameters
pincode = '21320'
data_id = 1576491609556
counter = 7000

# Set page limit
page_start = 1
page_end = 111

# File object
file = csv.writer(open('state-film-permits.csv', 'w'))
file.writerow([
    'State', 
    'Year', 
    'Category', 
    'Project', 
    'Company', 
    'Jobs Created', 
    'Qualified Expenditures', 
    'Incentive Issued'
])

for page in range(page_start, page_end):
    url = 'http://rochester.nydatabases.com/ajax/' + pincode + '?c1%5B%5D=&textsearch=&start=' + str(page) + '&_=' + str(data_id)
    response = (requests.get(url)).json()
    
    for key, value in response['result'].items():

        # Incase you get face an encoding error, uncomment below
        # data = [val.encode("utf8") for val in value]

        file.writerow(value)

    data_id += counter