# Databular - A data processing package


## Overview
Databular is a data manipulation package that assists in simplifying the process of tabularizing, manipulating, and data analysis. CSV files are accepted by Databular, which enables users to alter the data by slicing, filtering, searching, and replacing before displaying the results in a lovely tabular format. Additionally, Databular provides a feature that enables users to export modified data as a CSV file.

## Functionality
The Databular package is designed to provide an extremely streamlined form of data representation. It is also built with a rigid structure which handles only string, float and integer type values only. It consists of one class and fourteen main methods that can be used for working with and understanding the data. 

The entire functionality of Databular can be grouped into 4 distinct functions.
  - *File Manipulation*: [Functions -> import_csv, export_csv]. These two functions enable the user to directly import csv data into Databular and after all the analysis ad the manipulation is carried out, the user can then export the csv file to a path of their choosing on their local computer.
  - *Data Description*: [Functions -> display, describe, search, top, bottom]. These functions provide the user to view the data and also search for specifc records based on certain conditions and also get a consolidated analysis of the data for each column. 
  - *Data Manipulation*: [Functions -> merge, filter, to_str_type, add_record, add_column]. These functions enable the user to manipulate the data within a Databular table which will help to aiding different analysis carried out by the user.
  - *Null Value Manipulation*: [Functions -> has_null, impute]. These function explicity deal with null values by identifying if a column has null values and also the total count and the user can choose to impute the null values with a value of their choosing.

## Installing the package:
The user can install the package using the following commands:
```
$ git clone https://github.com/Raj-Mahesh/Databular
$ cd Databular
$ python3 setup.py install
```

## Testing the package
There is an explicit testing suite which has been curated for this package. In order to execute that testing suite, the user can do it in two ways:
  - Method 1: Open the file ```run_tests.py``` and execute it.
  - Method 2: Use the following command at the directory level - ```python3 -m run_tests.py```
 
 The user can also find the list of testcases written in the file ```test_databular.py``` under the Databular\Tests

## Usage
The following snippets will help to better understand how the package can be imported and used for data manipulation and analysis:

### Initializing by passing column names and row data
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184518918-98ee39fc-4ad2-405c-9616-65ca4a3e59e3.png">

### Displaying the data
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184518976-e46122ed-8435-4e20-bf87-d1fef0d76ca2.png">

### Displaying the top n records
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519015-82e7993d-0878-4844-8b07-2f8d6e8a20da.png">

### Displaying the bottom n records
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519027-e88ca8f6-b49a-4eb5-a64d-9ed6c1f27fc5.png">

### Displaying descriptive stats about the data
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519051-b1e51489-26db-4f37-b727-ff9b37c27aca.png">

### Importing a csv file
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519155-da27af38-b111-4bcf-8ed2-52371dcf0366.png">

### Exporting the databular data to a csv file
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519201-cf707602-3e2e-46b8-bda5-d014d2d2718d.png">

### Searching for records matching a value in the data
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519408-6ec6f85a-f7ba-4c33-aa9c-1ea8a0b6ec0c.png">

### Merging two tables together
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519470-e779f87d-6019-4fcc-83fe-9851284aa815.png">

### Filtering the data with a certain condition
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519531-de20f8ff-732a-41f8-92f7-b247567560c5.png">

### Converting the datatype of a column to a string 
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519604-0dee70be-387f-4895-a327-f294e181ea4f.png">

### Adding a seperate column with records
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519649-424a698d-ca2e-4241-bb91-ce60b9083af0.png">

### Adding a record to the data
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519672-a4e0bd61-7d57-45e8-ba32-40ccba2e5d5c.png">

### Checking if a column has null values
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519705-f5fea0d1-b75c-4aee-9ea0-854337920936.png">

### Imputing the null values with a particular value
<img width="955" alt="image" src="https://user-images.githubusercontent.com/41161698/184519727-2e75686c-a7d9-4bf8-bdc4-e19df9bcb4b3.png">





