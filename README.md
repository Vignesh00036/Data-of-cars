<div align="center">
    <img src="Image/bmw-png.png" alt="Logo" width="110" height="90">
</div>

<h3 align="center">Data Of Used Cars</h3>

## About The Project
This program is a batch processing solution designed to automate the data cleaning and The program processes large datasets and performing comprehensive data cleansing operations to eliminate errors,inconsistencies, and duplicates. After cleaning, the data is transformed into the appropriate format for efficient storage and querying. The final step involves securely inserting the processed data into a cloud data warehouse ensuring that it is ready for analysis, reporting, and business intelligence applications.

## Built With:
<p>Our program is developed using the most efficient programming languages, robust libraries, and top-tier database management tools to enhance performance and optimize storage solutions.</p>

<b>Languages:</b>
<ul>
    <li>Python</li>
    <li>MySQL</li>
</ul>
<p><b>Cloud storage:</b></p>
<ul>
    <li>Amazon aws s3</li>
    <li>Amazon aws Redshift</li>
</ul>

## ETL Journey/Workflow:
<p><b>1. Data Extracting Process (Extract):</b></p>
<ul><li>I utilized this file <a href='data/used_cars_data.csv'> Source</a> as my data source for scraping used cars stats. Using pandas dataframe I've extracted the data from source file and stored the data in a variable
</li></ul>
<p><b>2. Data Transforming Process (Transform):</b></p>
<ul>
    <li>
        During data validation, invalid characters and null values are replaced with '0' to ensure data integrity. For example, any instances of '?' identified as invalid are transformed to '0'.
    </li>
</ul>
<p><b>3. Data Loading Process (Load):</b></p>
<ul>
    <li>
        Once the data is cleaned and structured, it is loaded into a database management system using MySql, where all data is systematically inserted into table and Data is simultaneously extracted to the destination.
    </li>
    <li>
        we will upload the extracted data to an Amazon S3 bucket, and from there, manually load the data into a Redshift table.
    </li>
</ul>

## How To Run:
To execute this program, please ensure that all required Python libraries and tools are installed. Once the libraries are installed, open the main file located at <a href="main.py"> Main File</a> in your preferred Python environment. I recommend using Visual Studio Code (VS Code) for ease of use.
