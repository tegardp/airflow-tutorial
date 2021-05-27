# Airflow-ETL

This project is about simulating ETL process using Apache Airflow. We have some data that comes with many forms as shown inside `./data/0. raw/` directory.
Our objective is to store them into a Data Warehouse as a single source.

## Installation
requirements:
You must have docker installed

```
docker-compose up build
```

You can access Airflow UI in `localhost:8080` on your favorite web browser. Input username and password `airflow` and `airflow` if asked

## Data
You can find the data in folder `./data/0. raw/`.

### **Chinook Database**
This database consists of 14 tables. After transforming it, we got 6 tables.

`Tracks` - Contains complete information of tracks. Built from joining tracks table with albums, artists, genres and media_types tables.

`Playlist_Track` - Contains information of playlist where the tracks belong. Built from joining playlist_track table with tracks and playlists tables.

`Invoice_Items` - Contains information of tracks transaction. Built from joining invoice_items table with invoices and tracks table.

`Invoices` - Contains information of invoices. Built from joining invoices table and customers table

`Customers` - Contains information of Customers

`Employees` - Contains information of Employees

### **Database**
This data formed in .sqlite format. this data is about music dataset

### **Disaster**
This data formed in .csv format, containing text about comments on disaster happened.

### **File_1000**
Information about particular platform's user

### **Reviews**
Data source consists of 5 files, 2 files named q1_reviews stored in .csv and .xlsx format and q2-q4 are .csv

### **Tweets**
Comment about Indosat Ooredo service. A json formatted data containing 30 fields.

# Explanation
## **What is ETL?**

![etl](https://glints.com/id/lowongan/wp-content/uploads/2020/09/proses-etl-extract-transform-load-geeksforgeeks-org.jpg "etl process")
© Geeksforgeeks.org

**ETL** stands for Extract, Transform, and Load. ETL allows business to gather data from multiple sources and store it into a single location. ETL also makes it possible for different types of data to work together

## What is **Airflow**?
![Apache Airflow](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRDWvcSHe-6ckHsf3FJ6W-3rv5MpS8mtXVTFU3YsJfPbnGzE66dSASnWFQpbdQVQm17BVY&usqp=CAU "Apache Airflow")

Did you just make first scraper for your company? Good.

you put it on cronjob so you can run it automatically? Great!

Some time later you have made tens or hundres of scraper and cronjob. Suddenly last night some critical job failed to run and you're confused where did it go wrong? why could it have failed? What the F*ck just happened?

That's where Airflow comes in.

**Airflow** is a tool for developers to schedule, execute and monitor their workflows. Originally it was developed by Airbnb. Currently it's part of Apache incubator project.

![airflow graphs](https://res.cloudinary.com/practicaldev/image/fetch/s--R50fHi-s--/c_limit%2Cf_auto%2Cfl_progressive%2Cq_auto%2Cw_880/https://thepracticaldev.s3.amazonaws.com/i/n23kfzklxkg2satqlh6x.png "airflow graphs")
© https://dev.to/zahidulislam 

look at the graph above, you can clearly see which one can run successfully and which one failed you!