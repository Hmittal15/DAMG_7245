import boto3
import os
import logging
from dotenv import load_dotenv
import sqlite3
import io
import re

load_dotenv()

# Define logging format
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

# Establish connection to user bucket
s3client = boto3.client('s3', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

def write_db_to_bucket():

    # Connect to the SQLite database
    conn = sqlite3.connect("filenames_nexrad.db")
    
    # Export the database to an SQL file
    with io.StringIO() as f:
        for line in conn.iterdump():
            f.write(line + "\n")
        f.seek(0)
        sql_content = f.read()

    # Upload the SQL file to the S3 bucket
    s3client.put_object(Bucket=os.environ.get('USER_BUCKET_NAME'), Key="database_nexrad.db", Body=sql_content.encode("utf-8"))

    # Clean up
    conn.close()

def create_metadata_nexrad():
    """Gathers data from the NEXRAD S3 bucket and creates metadata containing file paths for every files present 
    in that bucket in the form of a database"""
    logging.debug("fetching objects in NEXRAD s3 bucket")
    folders = ['2022/', '2023/']
    paginator = s3client.get_paginator('list_objects_v2')
    for year in folders:
        nexrad_bucket = paginator.paginate(Bucket='noaa-nexrad-level2', Prefix=year)

        # Connect to the database
        conn = sqlite3.connect("filenames_nexrad.db")
        c = conn.cursor()

        # Create a table if it does not exist
        c.execute("""CREATE TABLE IF NOT EXISTS filenames_nexrad (Year text, Month text, Day text, Station text, PKey text primary key)""")
        
        # Truncates the table before filling metadata
        c.execute("""DELETE FROM filenames_nexrad""")
        logging.info("Printing Files in NEXRAD bucket")

        # Fills the data in the database
        for count, page in enumerate (nexrad_bucket):
            files = page.get("Contents")
            if (count%5 == 0):
                conn.commit()
            for file in files:
                print('\t' * 4 + file['Key'])
                filename = file['Key'].split('/')
                print(filename)
                pkey = "" + filename[0] + filename[1] + filename[2] + filename[3]
                c.execute("INSERT OR IGNORE INTO filenames_nexrad (Year , Month , Day , Station , PKey ) VALUES ('{}', '{}', '{}', '{}', '{}')"
                          .format(filename[0], filename[1], filename[2], filename[3], pkey))
    
    conn.commit()
    conn.close()





def list_files_in_user_bucket() :
    """Lists all the files present in the user's S3 bucket along with its file path"""
    logging.debug("fetching objects in user s3 bucket")
    my_bucket = s3client.list_objects(Bucket=os.environ.get('USER_BUCKET_NAME'))
    files = my_bucket.get('Contents')
    logging.info("Printing Files in User bucket")
    for file in files:
        print('\t' * 4 + file['Key'])

def list_files_in_nexrad_bucket():
    """Lists a few files present in the NEXRAD S3 bucket along with its file path"""
    logging.debug("fetching objects in NEXRAD s3 bucket")
    paginator = s3client.get_paginator('list_objects_v2')
    nexrad_bucket = paginator.paginate(Bucket='noaa-nexrad-level2', PaginationConfig={"PageSize": 2})
    logging.info("Printing Files in NEXRAD bucket")
    for count, page in enumerate(nexrad_bucket):
        files = page.get("Contents")
        for file in files:
            print('\t' * 4 + file['Key'])
        if count == 2:
            break

def main():
    # return
    # list_files_in_user_bucket()
    # list_files_in_nexrad_bucket()
    create_metadata_nexrad()
    # write_db_to_bucket()

if __name__ == "__main__":
    # logging.info("Script starts")
    main()
    # logging.info("Scripts ends")