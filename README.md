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


