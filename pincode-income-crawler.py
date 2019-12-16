from bs4 import BeautifulSoup
import requests
import csv

base_url1 = 'https://www.incomebyzipcode.com/search?utf8=%E2%9C%93&search%5Bterms%5D='
base_url2 = 'https://www.incomebyzipcode.com/'
pincodes = ['78701', '18357']

session = requests.Session()

with open('pincode-data.csv', 'w') as file:
    writer = csv.writer(file) 

    for pincode in pincodes:
        data = [pincode]

        soup = BeautifulSoup(session.get(base_url1 + pincode).content, 'html.parser')
        ref = soup.find('td').a['href']

        soup = BeautifulSoup(session.get(base_url2 + ref).content, 'html.parser')

        for el in soup.find_all(class_ = "hilite")[:4]:
            data += el.contents

        writer.writerow(data)