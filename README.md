# youtube1
This is youtube data harvesting project using ETL pipeline
there are certain procedures to scrape the youtube data
Generation of Api key:
          An Api key is generated using the mail-Id. The YouTube API is an application programming interface that allows you to embed videos, curate playlists, and offer               other YouTube functionalities on your website. It helps businesses offer advanced video sharing features on their website or app without needing to write code from           scratch.
Get Data from Youtube:
          Get the channel detail, playlist id , video id , video details, comment details using the Api key generated. use an empty list and append all the datas into the             list. 
Store it in a Document Database:
          Use Mongo Db to store the data scraped. The datas will be in Json Format.
MIGRATE to DATAWAREHOUSE:
          Use MYSQL to store the datas in table format.And create the nescessary columns. SQL is very fast in extracting large amounts of data very efficiently. SQL is                 flexible as it works with multiple database systems from Oracle, IBM, Microsoft, etc. SQL helps you manage databases without knowing a lot of coding.
Approach: 
Set up a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
Connect to the YouTube API: You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.
Store data in a MongoDB data lake: Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
Migrate data to a SQL data warehouse: After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.
Query the SQL data warehouse: You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.
Display data in the Streamlit app: Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.
Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
