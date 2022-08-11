from typing import List, Dict, Union


class Databular:
    def __init__(self, row_data: List[List[Union[int, float, str, None]]] = None, column_names: List[str] = None) -> None:
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
        max_len = [len(x) for x in table[0]]
        for row in table[1:]:
            for ind, col in enumerate(row):
                max_len[ind] = max(max_len[ind], len(str(col)))

        line_length = len('|' + ''.join([h + ' ' * (l - len(h)) + ' | ' for h, l in zip(table[0], max_len)]))
        seperator = '+' + '-' * (line_length - 3) + '+' + '\n'

        final_table = seperator
        final_table += '|' + ''.join([h + ' ' * (l - len(h)) + ' | ' for h, l in zip(table[0], max_len)]) + '\n'
        final_table += seperator

        for row in table[1:]:
            final_table += '|' + ''.join([str(c) + ' ' * (l - len(str(c))) + ' | ' for c, l in zip(row, max_len)])
            final_table += '\n'

        final_table += seperator
        return final_table

    def __str__(self) -> str:
        table = self.__construct_table_base()

        longest_cols = [
            (max([len(str(row[i])) for row in table]) + 3)
            for i in range(len(table[0]))
        ]
        row_format = "".join(["{:<" + str(longest_col) + "}" for longest_col in longest_cols])
        return "\n".join([row_format.format(*map(str, row)) for row in table])
    
    def __getitem__(self, index: Union[str, int]) -> Dict:
        """
        The function is used to retrieve a record based on the index or column name provided. 
        
        :param index: string or integer: the value of which the record must be retrieved  
        :return record: a dictionary where the key is the column(s) and the values are the row value(s).
        
        :raises ReferenceError: The index provided is more than the total number of records created.
        :raises ValueError: If the column name provided does not match the existing columns.
        :raises ValueError: If the index passed to the function is not in an integer or a string format.
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
    
    def display(self) -> str:
        table = self.__construct_table_base()
        return self.__pretty_print(table)
               
    def import_file(self, file_path: str) -> None:
        """
        The function allows to access the csv data file that needs to be manipulated.
        
        :param file_path: string:  The path of the data file that needs to be accessed.
        
        :raises TypeError: If the file is not in a csv format.
        """
        if not file_path.endswith("csv"):
            raise TypeError("The file type is not csv. Please upload a csv file.")

        column_names, row_data = [], []
        with open(file_path) as file:
            lines = file.readlines()
            column_names = lines[0].strip().split(",")
            for line in lines[1:]:
                row_data.append(line.strip().split(","))

        self.__columns, self.__rows = column_names, row_data
        self.__preserve_dtype()

    def export_file(self, file_path: str, exclude_index: bool = False) -> None:
        """
        The function allows to export the final manipulated data in a csv format to the location specified.
        
        :param file_path: string:  The path where the final manipulated data would be stored in the csv format.
        :param exclude_index (optional): boolean:  If the indexes/index column should be removed while exporting the data.
        
        :raises TypeError: If the file is not in a csv format.
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
        """
        Displays the top 5(or specified number) records of the data.
        
        :param n (default 5): integer:  the number of rows to be displayed from the top.
        
        :raises IndexError: if the specified number of rows is less than 1.
        """
        if n < 1:
            raise IndexError('Index value should always be more than 1.')
        table = self.__construct_table_base()
        cut_table = table[:n+1]
        return self.__pretty_print(cut_table)

    def bottom(self, n: int = 5) -> str:
        """
        Displays the bottom 5(or specified number) records of the data.
        
        :param n (default 5): integer:  the number of rows to be displayed from the bottom.
        
        :raises IndexError: if the specified number of rows is less than 1.
        """
        if n < 1:
            raise IndexError('Index value should always be more than 1.')
        table = self.__construct_table_base()
        cut_table = [table[0]]
        cut_table.extend(table[~(n - 1):])
        return self.__pretty_print(cut_table)
        
    def filter(self, column_name: str, operator: str, value: Union[str, float, int]) -> None:
        """
        The function makes a permanent change to the data by filtering it out based on the provided conditions. i.e making changes to the actual data.
        
        :param column_name: string:  the column name where the filtering is needed.
        :param operator: string:  the operator takes in the "==" as an input to keep the values which are needed.
        :param value: string or float or integer:  the value that needs to be filtered/retained.
        
        :raises ValueError: if the column name or the operator or the value provided is not in the correct format.
        :raises ValueError: if the column name provided is not found in the data.
        :raises ArithmeticError: If the value provided is of string type but the column contains records of an integer type.
        :raises ArithmeticError: If the operator provided is anything other than "==".
        :raises ArithmeticError If the value provided is not of a string type but the column contains records in a string type.
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
                if eval(f"{row_val} {operator} {value}"):
                    filtered_rows.append(row)

        self.__rows = filtered_rows
        
    def get_data(self) -> Dict:
        """
        The function is used to give the copy of the entire data.
        :return: a dictionary with column_names and row_data to access the entire data as a copy
        """

        return {
            'column_names': self.__columns,
            'row_data': self.__rows
        }
     
    def merge(self, table) -> None:
        """
        The function imerges one databular table to another.
  
        :param data: gets the data to be merged.
        
        :raises ValueError: if the parameter passed is not a databular object.
        :raises ValueError: if the columns between the databular tables do not match.
        :raises ValueError: if there are no records in the databular object which is passed.
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
        """
        The function coverts a column’s data to string type.
        
        :param column_name: the column name where the conversion is needed.
        :param column_ind: index of the column that is to be converted.
        :param prev_type: previous type of the column before conversion.
        
        :raises ReferenceError: if the column name is not found in the databular table.
        
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
        """
        The function checks if the column has null values.
        
        :param column_name: the column name to check for null values.
        :param column_ind: index of the column.
        :param null_count: number of null values in the column.
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
        """ 
        The function replaces the null values with specific values.
        
        :param has_null_values: has the null values.
        :param value: has the value used to check the type.
        :param impute_val: values used for substituting the null values.
        :param col_values: column value.
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

  