from typing import List, Dict, Union


class Databular:
    def __init__(self, row_data: List[List[Union[int, float, str, None]]] = None, column_names: List[str] = None) -> None:
        """Initializes an empty databular table which can be used to handle csv values for type str, int and float
        and perform various manipulations in the data in a vectorized manner

        :param row_data: list of lists with each list denoting a single record of a table in order
        :param column_names: list containing the column names of the table in order
        """
        if column_names is not None:
            if len(set(column_names)) != len(column_names):
                raise ValueError('The column names should be unique and should not have duplicates.')

            if any('' == str(s).strip() for s in column_names):
                raise TypeError('Column names should not be empty strings.')

        if (row_data is None and column_names is not None):
            if set(map(self.__check_type, column_names)) != {str}:
                raise TypeError('The columns names should be of string type and must not start with a numeric value and must not be empty')

        if row_data is not None and column_names is not None:
            if (len(row_data) != 0) and (len(min(row_data, key = len)) != len(column_names)):
                raise ValueError('The number of columns in the records does not match the number of column names specified.')

            elif (len(row_data) != 0) and (len(max(row_data, key = len)) != len(column_names)):
                raise ValueError('The number of column names specified is less than the number of columns in the data.')

            elif len(row_data) != 0 and len(column_names) == 0:
                raise ValueError('The column names are not specified.')

            elif len(row_data) == 0 and len(column_names) == 0:
                raise ValueError('There should be at least one record and one column in the databular table')

        self.__columns = column_names
        self.__rows = row_data
        if column_names:
            self.__preserve_dtype()

    @staticmethod
    def __check_type(value: Union[int, float, str, None]) -> (int or float or str or None):
        """This helper function is used to check the type of the value passed

        :param value: the value whose type needs to be checked
        :return: the type of the value
        """
        value = str(value)
        if value == "None" or value.strip() == "":
            return None

        elif value.isnumeric():
            return int
        try:
            val = float(value)
            return int if val == int(val) else float
        except (TypeError, ValueError):
            return str

    def __preserve_dtype(self) -> None:
        """This helper function is used to preserve the data type of all the columns of the table by iterating through
        the values and determining the type

        :return: None
        """
        if not self.__rows:
            return None
        columns = list(zip(*self.__rows))
        column_types = []
        for values in columns:
            values_type = list(set(map(self.__check_type, values)))
            column_type = str
            if len(values_type) == 1:
                if values_type[0] is None:
                    column_type = str
                else:
                    column_type = values_type[0]

            elif len(values_type) == 2:
                store_types = set(values_type)
                if store_types == {int, float} or store_types == {None, float}:
                    column_type = float

                elif store_types == {None, int}:
                    column_type = int

            elif len(values_type) == 3 and set(values_type) == {int, float, None}:
                column_type = float

            column_types.append(column_type)

        columns = [list(map(lambda x: None if str(x) in {"", "None"} else column_types[i](x), val))
                   for i, val in enumerate(columns)]
        self.__rows = [list(row) for row in zip(*columns)]
        self.__column_dtypes = column_types

    def __construct_table_base(self) -> List[List[Union[int, float, str, None]]]:
        """This helper function helps to construct a table like matrix with index values as well for the table to be
        manipulated

        :return: a matrix of the table needed to be printed
        """
        table = [
            [" "] + self.__columns
        ]
        if not self.__rows:
            table.append([""] * (len(self.__columns) + 1))
        else:
            for ind, row in enumerate(self.__rows):
                table.append([ind] + row)

        return table

    @staticmethod
    def __pretty_print(table: List[List[Union[int, float, str, None]]]) -> str:
        """This helper function helps to create a pretty version of the table for display purposes

        :param table: list of lists with the first list denoting the column names and each other list denoting a single
        record of a table in order
        :return: a prettified string version of the table for display
        """
        max_len = [len(x) for x in table[0]]
        for row in table[1:]:
            for ind, col in enumerate(row):
                max_len[ind] = max(max_len[ind], len(str(col)))

        line_length = len('|' + ''.join([h + ' ' * (l - len(h)) + ' | ' for h, l in zip(table[0], max_len)]))
        seperator = '+' + '-' * (line_length - 3) + '+' + ' \n'

        final_table = seperator
        final_table += '|' + ''.join([h + ' ' * (l - len(h)) + ' | ' for h, l in zip(table[0], max_len)]) + '\n'
        final_table += seperator

        for row in table[1:]:
            final_table += '|' + ''.join([str(c) + ' ' * (l - len(str(c))) + ' | ' for c, l in zip(row, max_len)])
            final_table += '\n'

        final_table += seperator
        return final_table

    def __str__(self) -> str:
        """This method is used to return an organised stringified version of the entire table

        :return: an organised string version of the table for display
        """
        table = self.__construct_table_base()

        longest_cols = [
            (max([len(str(row[i])) for row in table]) + 3)
            for i in range(len(table[0]))
        ]
        row_format = "".join(["{:<" + str(longest_col) + "}" for longest_col in longest_cols])
        return "\n".join([row_format.format(*map(str, row)) for row in table])

    def display(self) -> str:
        """This function invokes several helper functions to produce a prettified version of the table and displays it

        :return: a prettified string version of the table for display
        """
        table = self.__construct_table_base()
        return self.__pretty_print(table)

    def __getitem__(self, index: Union[str, int]) -> Dict:
        """This method is used to retrieve a record based on the index or column name provided.

        :param index: string or integer: the value of which the record must be retrieved
        :return: a dictionary where the key is the column name and the values are the row value(s) in a list.
        """
        record = {}
        if type(index) == int:
            if index < 0:
                index = ~index

            if index > len(self.__rows):
                raise ReferenceError("The index provided exceeds the number of records in the Databular table.")


            for column_name, row_val in zip(self.__columns, self.__rows[index]):
                record[column_name] = [row_val]

        elif type(index) == str:
            if index not in self.__columns:
                raise ValueError('The column name does not exist in the databular table.')

            ind = self.__columns.index(index)
            records = [val[ind] for val in self.__rows]
            record[index] = records

        else:
            raise ValueError("The index is not of acceptable format. Please pass an integer or a string.")

        return record

    def import_file(self, file_path: str) -> None:
        """
        The function allows to access the csv data file into the databular instance that needs to be manipulated.

        :param file_path: The path of the data file that needs to be accessed.
        :return: None
        """
        if not file_path.endswith("csv"):
            raise TypeError("The file type is not csv. Please upload a csv file.")

        column_names, row_data = [], []

        with open(file_path, 'r') as file:
            lines = file.readlines()
            column_names = lines[0].strip().split(",")
            for line in lines[1:]:
                row_data.append(line.strip().split(","))

        self.__columns, self.__rows = column_names, row_data
        self.__preserve_dtype()

    def export_file(self, file_path: str, exclude_index: bool = False) -> None:
        """This function allows to export the final manipulated data in a csv format to the location specified.

        :param file_path: The path where the final manipulated data would be stored in the csv format.
        :param exclude_index: If the indexes/index column should be removed while exporting the data. The default value
        is set to False and all index numbers will be included.
        :return: None
        """
        if not file_path.endswith("csv"):
            raise TypeError("The file type is not csv. Please enter the path for a csv file.")

        table = self.__construct_table_base()
        if exclude_index:
            [row.__delitem__(0) for row in table]

        with open(file_path, 'w') as file:
            for row in table:
                file.write(f"{','.join(map(str, row))}\n")

    def top(self, n: int = 5) -> str:
        """This function displays the top 5 or specified number of records of the data.

        :param n:  the number of rows to be displayed from the top.
        :return: None
        """
        if n < 1:
            raise IndexError('Index value should always be more than 1.')
        table = self.__construct_table_base()
        cut_table = table[:n+1]
        return self.__pretty_print(cut_table)

    def bottom(self, n: int = 5) -> str:
        """This function displays the bottom 5 or specified number of records of the data.

        :param n:  the number of rows to be displayed from the top.
        :return: None
        """
        if n < 1:
            raise IndexError('Index value should always be more than 1.')
        table = self.__construct_table_base()
        cut_table = [table[0]]
        cut_table.extend(table[~(n - 1):])
        return self.__pretty_print(cut_table)

    def filter(self, column_name: str, operator: str, value: Union[str, float, int]) -> None:
        """This function makes an overridden change to the data by filtering it out based on the provided conditions, i.e
        by making changes to the actual data.

        :param column_name: the column name where the filtering is needed.
        :param operator: the operator takes in the "==" as an input to keep the values which are needed.
        :param value: the value that needs to be filtered/retained.
        :return: None
        """
        if not column_name or not operator or not value:
            raise ValueError('Please pass the appropriate values for filtering.')

        if type(column_name) != str or column_name not in self.__columns:
            raise ValueError('Column not found in databular table.')

        column_index = self.__columns.index(column_name)
        col_type = self.__column_dtypes[column_index]
        if (type(value) == str) and col_type != str:
            raise ArithmeticError("The specified column is not of string type, please pass a numeric value.")

        if (type(value) == str) and operator != '==':
            raise ArithmeticError("For string comparison, please use only == operator.")

        if col_type == str and (type(value) != str):
            raise ArithmeticError("The specified column is of string type, please pass a string value.")

        column_index = self.__columns.index(column_name)
        filtered_rows = []
        for row in self.__rows:
            row_val = str(row[column_index]) if row[column_index] is None or row == 'None' else col_type(row[column_index])
            if col_type == str:
                if row_val == value:
                    filtered_rows.append(row)
            else:
                if row_val is not None and row_val != "None":
                    if eval(f"{row_val} {operator} {value}"):
                        filtered_rows.append(row)

        self.__rows = filtered_rows

    @staticmethod
    def __mean(column) -> float or str:
        """This helper function computes the mean of attributes of the dataset

        :param column:  Numeric column to compute the mean values
        :return: the mean value of the column or '-' if no numeric values exist in the column
        """
        column = [val for val in column if val is not None]
        if len(column) == 0:
            return '-'
        else:
            mean = sum(column) / len(column)
            return round(mean, 5)

    def __std(self, column) -> float or str:
        """ This function computes the standard deviation of attributes from the mean

        :param column: a numeric column to compute the standard deviation values
        :return: the standard deviation value of the column or '-' if no numeric values exist in the column
        """
        mean_val = self.__mean(column)
        if mean_val == '-':
            return mean_val
        else:
            column = [val for val in column if val is not None]
            variance = sum([((val - mean_val) ** 2) for val in column]) / len(column)
            stddev = variance ** 0.5
            return round(stddev, 5)

    def describe(self) -> str:
        """this function gives descriptive statistics which includes the DataType, Count, Frequency, Mean,
        Standard Deviation and shape of a dataset’s distribution

        :return: None
        """
        desc = [self.__columns]
        columns = [list(row) for row in zip(*self.__rows)]
        desc.append([f"Data Type: {col_type.__name__}" for col_type in self.__column_dtypes])
        desc.append([f"Total Count: {len(col)}" for col in columns])
        desc.append([f"Top Freq Value: {max(col, key=col.count)}" for col in columns])
        desc.append([f"Top Freq Count: {col.count(max(col, key=col.count))}" for col in columns])
        desc.append([f"Mean: {'-' if self.__column_dtypes[i] == str else self.__mean(col)}"
                     for i, col in enumerate(columns)])
        desc.append([f"STD: {'-' if self.__column_dtypes[i] == str else self.__std(col)}"
                     for i, col in enumerate(columns)])
        desc.append([f"Has None Values: {None in col}" for col in columns])
        return self.__pretty_print(desc)

    def add_record(self, record: List[Union[str, int, float]]) -> None:
        """This function is used to add a record into the existing data

        :param record: a data row/record that needs to get added into the existing data
        :return: None
        """
        if len(record) != len(self.__columns):
            raise ValueError('The number of columns in record does not match the number of columns in the databular table.')

        else:
            self.__rows.append(record)
            self.__preserve_dtype()
            print("Successfully inserted a record in the databular table.")

    def add_column(self, column_name: str, column_data: List[Union[str, int, float]]) -> None:
        """This function is used to add a column into the existing data

        :param column_name: the desired name of the column that has to get appended into the data
        :param column_data: the column data to be appended into the existing data
        :return: None
        """
        length = 0
        columns = []
        if self.__check_type(column_name) != str or len(column_name) == 0:
            raise TypeError('The columns name should be of string type and must not start with a numeric value and must not be empty')

        if self.__rows is not None and len(self.__rows):
            columns = [list(row) for row in zip(*self.__rows)]
            length = len(columns[0])

        if length != len(column_data):
            raise ValueError('The number of records passed does not match the number of records in the databular table.')

        elif column_name in self.__columns:
            raise ValueError('The column name given already exists in the databular table.')

        else:
            columns.append(column_data)
            self.__columns.append(column_name)
            self.__rows = [list(row) for row in zip(*columns)]
            self.__preserve_dtype()
            print("Successfully inserted a column in the databular table.")

    def search(self, column_name: str, operator: str, value: Union[str, float, int]) -> str:
        """this function identifies the records matching the condition passed and displays those identified results

        :param column_name: the column name that has to be searched and filtered in the data
        :param operator: the arithmetic operator to compare with column values
        :param value: the value to be compared with the column values
        :return: a resultant identified data based on the condition given
        """
        if not column_name or not operator or not value:
            raise ValueError('Please pass the appropriate values for filtering.')

        if type(column_name) != str or column_name not in self.__columns:
            raise ValueError('Column not found in databular table.')

        column_index = self.__columns.index(column_name)
        col_type = self.__column_dtypes[column_index]
        if (type(value) == str) and col_type != str:
            raise ArithmeticError("The specified column is not of string type, please pass a numeric value.")

        if (type(value) == str) and operator != '==':
            raise ArithmeticError("For string comparison, please use only == operator.")

        if col_type == str and (type(value) != str):
            raise ArithmeticError("The specified column is of string type, please pass a string value.")

        column_index = self.__columns.index(column_name)
        filtered_rows = []
        for row in self.__rows:
            row_val = str(row[column_index]) if row[column_index] is None or row == 'None' else col_type(row[column_index])
            if col_type == str:
                if row_val == value:
                    filtered_rows.append(row)
            else:
                if row_val is not None and row_val != "None":
                    if eval(f"{row_val} {operator} {value}"):
                        filtered_rows.append(row)

        table = [
            [" "] + self.__columns
        ]
        if not self.__rows:
            table.append([""] * (len(self.__columns) + 1))
        else:
            for ind, row in enumerate(filtered_rows):
                table.append([ind] + row)

        return self.__pretty_print(table)

    def get_data(self) -> Dict:
        """This function is used to give the copy of the entire data.

        :return: a dictionary with the column names as the keys and the row data as values.
        """
        return {
            'column_names': self.__columns,
            'row_data': self.__rows
        }

    def merge(self, table) -> None:
        """This function is used to merge one databular table to another.

        :param table: the databular tables to be merged with the other databular objects.
        """
        if type(table).__name__ != 'Databular':
            raise ValueError('The parameter passed is not a databular object.')
        data = table.get_data()

        if self.__columns != data['column_names']:
            raise ValueError('The columns between the databular tables do not match.')

        if len(data['row_data']) == 0:
            raise ValueError('There are no records in the databular object which is passed.')

        self.__rows.extend(data['row_data'])
        self.__preserve_dtype()
        print("The tables have been merged successfully.")

    def to_str_type(self, column_name: str) -> None:
        """This function checks the specified column's type and converts that column's data to string type.

        :param column_name: the column name where the conversion is needed.
        """
        if column_name not in self.__columns:
            raise ReferenceError('The column name is not found in the databular table.')

        column_ind = self.__columns.index(column_name)
        prev_type = self.__column_dtypes[column_ind]
        if prev_type == str:
            print(f"The column '{column_name}' is already of type str.")

        else:
            self.__column_dtypes[column_ind] = str
            for i in range(len(self.__rows)):
                self.__rows[i][column_ind] = str(self.__rows[i][column_ind])

            print(f"The column '{column_name}' has been converted from type {prev_type} to type str.")

    def has_null(self, column_name: str) -> bool:
        """This function checks if the column has null values or not.

        :param column_name: the column name to check for null values.
        :return: a boolean value (True or False) which denotes if the column had null values.
        """
        if column_name not in self.__columns:
            raise ReferenceError('The column name is not found in the databular table.')

        null_count = 0
        column_ind = self.__columns.index(column_name)
        for i in range(len(self.__rows)):
            if self.__rows[i][column_ind] is None or self.__rows[i][column_ind] == "None":
                null_count += 1

        print(f"The column '{column_name}' has {null_count} null values.")
        return True if null_count != 0 else False

    def impute(self, column_name: str, value: Union[str, int, float]) -> None:
        """ This function checks if the specified column has null values and imputes the wih the condition specified

        :param column_name: the column name of the table which has the null values to be replaced.
        :param value: the replacement condition (mean, min, max) or the exact replacement value
        :return: None
        """
        has_null_values = self.has_null(column_name)
        if not has_null_values:
            print('There are no null values to be imputed with.')

        else:
            column_ind = self.__columns.index(column_name)
            if type(value) == str:
                impute_val = value
                if value in {"min", "max", "mean"}:
                    if self.__column_dtypes[column_ind] == str:
                        impute_val = value
                    else:
                        col_values = [row[column_ind] for row in self.__rows if row[column_ind] not in {None, "None"}]

                        if len(col_values) == 0:
                            impute_val = None

                        elif value == "min":
                            impute_val = min(col_values)

                        elif value == "max":
                            impute_val = max(col_values)

                        elif value == 'mean':
                            impute_val = self.__mean(col_values)

                for i in range(len(self.__rows)):
                    if self.__rows[i][column_ind] in {None, "None"}:
                        self.__rows[i][column_ind] = impute_val

            elif type(value) in {float, int}:
                if self.__column_dtypes[column_ind] == str:
                    raise TypeError(f'The specified column is of type str and the value is of type{type(value).__name__}')

                else:
                    for i in range(len(self.__rows)):
                        if self.__rows[i][column_ind] in {None, "None"}:
                            self.__rows[i][column_ind] = value

        self.__preserve_dtype()

