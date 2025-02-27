# Introduction
This project was undertaken with the goal of building a data exploration tool that leverages publicly available data, making it easier for data analysts to download the data. The data sources were located on the NOAA website, and the NexRad, GOES satellite datasets were selected for exploration.

### Tasks accomplished

* __GEOS data__
1. Selected datasets for the GOES satellite dataset were explored and downloaded.
2. The hyperlink of the data location was constructed based on a given filename.
3. Unit tests were written for all use cases.
4. The tool was tested using multiple files.

* __NexRad data__
1. Selected datasets for the NexRad dataset were explored and downloaded.
2. The hyperlink of the data location was constructed based on a given filename.
3. Unit tests were written for all use cases.
4. The tool was tested using multiple files.

* __Plotting the NexRad Locations__<br>
We have used the <code>folium</code> library to facilitate visualization of these NEXRAD radar sites. Folium makes it easy to visualize data that’s been manipulated in Python on an interactive leaflet map.

### Files
* <code>nexrad.py</code> : This file generates the UI using streamlit packages to display various filters for the user to use and fetch the required file from the NexRad public S3 bucket. It also enables the user to enter the name of the required file and provides the user with the URL to download it.
* <code>goes18.py</code> : This file generates the UI using streamlit packages to display various filters for the user to use and fetch the required file from the NOAA GOES 18 public S3 bucket. It also enables the user to enter the name of the required file and provides the user with the URL to download it.
* <code>main_goes18.py</code> : This file contains the functions to display all object keys present in any bucket given that appropriate credentials are provided, list a few object keys present in NOAA GOES 18 public S3 bucket, scrape metadata from the NOAA GOES 18 bucket and store it in a database and write the created database to the user's bucket.
* <code>main_nexrad.py</code> : This file contains the functions to display all object keys present in any bucket given that appropriate credentials are provided, list a few object keys present in NexRad public S3 bucket, scrape metadata from the NexRad bucket and store it in a database and write the created database to the user's bucket.
* <code>mapping.py</code>: We scraped the URL https://en.wikipedia.org/wiki/NEXRAD#Operational_locations for radar sites. Stored the location table in <code>SQLite DB</code> and plotted an interactive map using <code>folium</code> package.<br>
* <code>testing_goes.py</code>: This file contains the testing module for file download URL generation logic. We have used <code>pytest</code> module to test if the URLs generated for GOES files are correct or not.<br>
* <code>testing_nexrad.py</code>: This file contains the testing module for file download URL generation logic. We have used <code>pytest</code> module to test if the URLs generated for NEXRAD files are correct or not.<br>
* <code>ddl.sql</code>: Contanis the SQL query to create table in SQLite DB for NEXRAD radar locations.

### Codelab documentation for detailed explanation:
https://codelabs-preview.appspot.com/?file_id=1h3qk0v7yIGpkfLxBOwHtnqFBZLvGPE3tjMRI4p3Za8w#9

### Streamlit UI Cloud URL for app:
https://bigdataia-spring2023-t-assignment1streamlit-filesgoes18-d3db6h.streamlit.app/
https://docs.google.com/document/d/1w0Szc-RnqDr9uFn8fcpUhQUQXInuCwkk6QaPhrjnpMY/edit?usp=sharing

### Attestation:
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK
Contribution:
* Ananthakrishnan Harikumar: 25%
* Harshit Mittal: 25%
* Lakshman Raaj Senthil Nathan: 25%
* Sruthi Bhaskar: 25%
