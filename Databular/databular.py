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