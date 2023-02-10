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
* <code>mapping.py</code>: We scraped the URL https://en.wikipedia.org/wiki/NEXRAD#Operational_locations for radar sites. Stored the location table in <code>SQLite DB</code> and plotted an interactive map using <code>folium</code> package.<br>
* <code>testing_goes.py</code>: This file contains the testing module for file download URL generation logic. We have used <code>pytest</code> module to test if the URLs generated for GOES files are correct or not.<br>
* <code>testing_nexrad.py</code>: This file contains the testing module for file download URL generation logic. We have used <code>pytest</code> module to test if the URLs generated for GOES files are correct or not.<br>
* <code>ddl.sql</code>: Contanis the SQL query to create table in SQLite DB for NEXRAD radar locations.
