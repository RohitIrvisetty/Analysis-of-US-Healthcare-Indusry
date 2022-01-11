##################################
#Author: Karthik Srinatha (ksa166)
#Modified by: Rovenna Chu (ytc17)
##################################


import os
import wget
import urllib.request
from pathlib import Path
from zipfile import ZipFile
from bs4 import BeautifulSoup

def scrape_and_structure():

    #This function scrapes the data required and stores in required structure for etl

    #Url of the project dataset
    url = "https://www.cms.gov/cciio/resources/data-resources/marketplace-puf"
    document = urllib.request.urlopen(url)
    soup = BeautifulSoup(document)

    #Look for all URLs:
    found_urls = [link["href"] for link in soup.find_all("a", href=True)]

    #Look only for URLs to *.zip files:
    found_zip_urls = [link["href"] for link in soup.find_all("a", href=True) if link["href"].endswith(".zip")]

    #Required years for analysis
    years = ['2020', '2019', '2018', '2017', '2016']

    #Downloading the required years data to the local path given 
    for year in years:
        respective_year = "./health_insurance_cmpt/" + year
        if not os.path.exists(respective_year):
            os.makedirs(respective_year)
        matching = [url for url in found_zip_urls if year in url]
        print( matching)
        for each_url in matching:
            wget.download(each_url, respective_year)

    #The zip files which we are concentrating
    required_zip  = ['benefits-and-cost-sharing-puf.zip', 'rate-puf.zip', 'plan-attributes-puf.zip', 'plan-id-crosswalk-puf.zip']

    print("\n")
    print("Extracting zips")
    print("\n")

    #Structuring the files in prepared dataset folder
    for year in years:
        for req_zip in required_zip:

            prepared_dataset = "./prepared_dataset/" + req_zip[:-len('.zip')] + '/' + year
            prepared_dataset_2 = "./prepared_dataset/" + req_zip[:-len('.zip')]

            #Creating folders to structure
            if not os.path.exists(prepared_dataset):
                os.makedirs(prepared_dataset)
            zipped_file = './health_insurance_cmpt/' + year + '/' + req_zip
            with ZipFile(zipped_file, 'r') as zipObj:

               # Extract all the contents of zip file in current directory
                zipObj.extractall(prepared_dataset)

                # Structuring
                rename = os.listdir(prepared_dataset)
                csv_file_name = rename[0]
                if (req_zip == 'plan-id-crosswalk-puf.zip'):
                    new_csv_file_name = 'Plan_ID_Crosswalk_PUF.csv'
                    path_to_plan_csv = prepared_dataset + '/' + csv_file_name
                    renamed_csv = prepared_dataset + '/' + new_csv_file_name
                    Path(path_to_plan_csv).rename(renamed_csv)
                    csv_file_name = new_csv_file_name
                csv_file_name_updated = csv_file_name[:-len('.csv')] + year + '.csv'
                path_to_csv = prepared_dataset + '/' + csv_file_name
                updated_csv = prepared_dataset_2 + '/' + csv_file_name_updated
                Path(path_to_csv).rename(updated_csv)
                #Deleting the empty folders
                os.rmdir(prepared_dataset)

    print("\n")
    print("Download and structuring done, proceed with further steps...")


if __name__ == '__main__':

    #Function call to scrape and structure the data
    scrape_and_structure()