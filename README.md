# DAMG_7245

### Plotting NexRad location:-
NEXRAD or Nexrad (Next-Generation Radar) is a network of 160 high-resolution S-band Doppler weather radars operated by the National Weather Service (NWS). NEXRAD detects precipitation and atmospheric movement or wind. It returns data which when processed can be displayed in a mosaic map which shows patterns of precipitation and its movement. The radar system operates in two basic modes, selectable by the operator – a slow-scanning clear-air mode for analyzing air movements when there is little or no activity in the area, and a precipitation mode, with a faster scan for tracking active weather. The site locations were strategically chosen to provide overlapping coverage between radars in case one failed during a severe weather event.<br><br>
We have used the <code>folium</code> library to facilitate visualization of these NEXRAD radar sites. Folium makes it easy to visualize data that’s been manipulated in Python on an interactive leaflet map. It enables both the binding of data to a map for visualizations as well as passing rich vector/HTML visualizations as markers on the map. The library has a number of built-in tilesets from OpenStreetMap, Mapbox, and Stamen, and supports custom tilesets with Mapbox or Cloudmade API keys.<br><br>

#### Documentation
https://python-visualization.github.io/folium/

#### Steps followed to plot the sites:-
1. Installed and imported <code>BeautifulSoup</code> Python library to scrape information from web page. We scraped the URL https://en.wikipedia.org/wiki/NEXRAD#Operational_locations, and read all the tables present. We stored the table with title "List of NEXRAD sites and their coordinates" into a pandas dataframe.

2. Next, we created a SQLite database and stored this information into a table, with slight manipulations.
<img width="106" alt="image" src="https://user-images.githubusercontent.com/108916132/217642125-8b3f2321-5b02-4a7f-b759-a5edf47f1430.png">

3. Then we move on to designing the folium map for these locations. We read the latitude and longitude data from DB for making circular markers for each site. We enhanced the map by adding pop-up markers with respective city names.

4. Upon adding all these features, we get an interactive map with all radar site marked up. This map is then integrated with the streamlit framework to generate an attractive web-app.

5. To facilitate <code>folium</code> support with <code>streamlit</code> we have used a package known as <code>streamlit-folium</code>, documentation for which can be found here: https://github.com/randyzwitch/streamlit-folium
<img width="542" alt="image" src="https://user-images.githubusercontent.com/108916132/217643979-734fb602-9d04-43e0-89e2-6bd30eea2661.png">
