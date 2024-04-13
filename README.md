
Demo video:https://drive.google.com/file/d/1e2B7GNVERM5imWSgvpT6QKZCxlPWj_en/view?usp=drive_link

Problem Statement: 

The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features: 
1. Ability to input a YouTube channel ID and retrieve all the relevant data(Channel name, subscribers, total video count, playlist ID, video ID, likes,dislikes, comments of each video) using Google API. 
2. Ability to collect data for up to 10 different YouTube channels and store them in the mongodb by clicking a button. 
3. Option to store the data in a MYSQL or PostgreSQL. 
4. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.


To run this project,

1.You need to install the packages from requirements.txt
2.You have to create api key to collect data from youtube.
3.Connection string for mongodb.
4.Connection string for MySQL.


Retrieving data from the YouTube API:

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, videos and comments. By interacting with the Google API,collect the data in a dictionary format. 
Note: Fetch few comments from each channel so that while migrating to mysql it takes less time to insert.To fetch all the comment details from the channel you have to use pagination next_page_token uncomment the commented line in  get_comment_info function in youtube.py .
Note:Take small channel which have minimum videos because there is a limit quota for using api key. If it is exceeded create another api key .


Storing data in MongoDB:

The retrieved data is stored in a MongoDB database YouTube_Data with three collections channel , video and comments.For transfering channel details,I have created an empty list to store the channel id which transfered to mongodb. If I again transfer the same channel details to mongodb it shows message channel Id already exits.Using distinct stored the channel_id in a list for transferring video and comment details to mongodb for avoiding duplication.If I again click the button it shows message details already exists .

Migrating data to a SQL: 

The application allows users to migrate data from MongoDB to a MySQL.Now the information is segregated into separate tables, including channels, videos, and comments.From the input app migrates one particular channel to three tables.Setting primary key and using query count(* ) duplications will not be inserted throws message details already transformed.
Note: I have converted the published_date and duration in datetime and time format respectively while collecting data. Tags will be in list which will not insert in mysql so I have converted  to comma seperated.
Note: In colab mysql connection will not support. So kindly use visual code or pycharm.

Queries:

There are 10 queries with visualization insights.
