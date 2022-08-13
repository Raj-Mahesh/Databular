from ..databular import Databular as db
import unittest
import os


def get_databular_instance(column_names=None, row_data=None):
    # Returns an initialized instance of the databular object
    return db(column_names=column_names, row_data=row_data)


class Test_Initalization(unittest.TestCase):
    # test_initialization 1-3 are used to initialize a Databular object and see if it successfully initialized
    def test_initialization1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.display(),
            "".join(
                ['+----------------------+ \n',
                 '|   | a  | b    | c    | \n',
                 '+----------------------+ \n',
                 '|0  | 10 | 2    | a    | \n',
                 '|1  | 1  | 2    | a    | \n',
                 '|2  | 1  | 2    | a    | \n',
                 '|3  | 1  | 55   | a    | \n',
                 '|4  | 1  | None | a    | \n',
                 '|5  | 1  | 2    | a    | \n',
                 '|6  | 1  | 2    | a    | \n',
                 '|7  | 1  | 2    | 12   | \n',
                 '|8  | 1  | 2    | None | \n',
                 '|9  | 1  | 2    | 500  | \n',
                 '|10 | 1  | 2    | abcd | \n',
                 '|11 | 1  | 2    | 3    | \n',
                 '+----------------------+ \n']
            )
        )

    def test_initialization2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.display(),
            "".join(
                ['+--------------+ \n',
                 '|  | a | b | c | \n',
                 '+--------------+ \n',
                 '|0 | 4 | 2 | 3 | \n',
                 '|1 | 1 | 2 | 3 | \n',
                 '+--------------+ \n']
            )
        )

    def test_initialization3(self):
        dt = get_databular_instance(
            column_names=[1, 2, 3],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        self.assertRaises(TypeError, dt.display)


class Test_Import(unittest.TestCase):
    # test_import 1-3 are used to import a csv object and see if the data is successfully imported
    def test_import1(self):
        dt = get_databular_instance()
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/cities.csv'
        dt.import_file(file_path=file_path)
        self.assertEqual(
            dt.display(),
            "".join(
                ['+------------------------------------+ \n',
                 '|  | LatD | latm | LatS | MS | State | \n',
                 '+------------------------------------+ \n',
                 '|0 | 41   | 5    | 59   | N  |  OH   | \n',
                 '|1 | 42   | 52   | 48   | N  |  SD   | \n',
                 '|2 | 46   | 35   | 59   | S  |  WA   | \n',
                 '|3 | 42   | 16   | 12   | W  |  MA   | \n',
                 '+------------------------------------+ \n']
            )
        )

    def test_import2(self):
        dt = get_databular_instance()
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/homes.csv'
        dt.import_file(file_path=file_path)
        self.assertEqual(
            dt.display(),
            "".join(
                ['+----------------------------------------------------+ \n',
                 '|  | Sell | List | Living | Room | Bed | Baths | Age | \n',
                 '+----------------------------------------------------+ \n',
                 '|0 | 142  | 160  | 28     | 10   | 5   | 3     | 60  | \n',
                 '|1 | 175  | 180  | 18     | 8    | 4   | 1     | 12  | \n',
                 '|2 | 129  | 132  | 13     | 6    | 3   | 1     | 41  | \n',
                 '|3 | 138  | 140  | 17     | 7    | 3   | 1     | 22  | \n',
                 '|4 | 232  | 240  | 25     | 8    | 4   | 3     | 5   | \n',
                 '|5 | 135  | 140  | 18     | 7    | 4   | 3     | 9   | \n',
                 '|6 | 150  | 160  | 20     | 8    | 4   | 3     | 18  | \n',
                 '|7 | 207  | 225  | 22     | 8    | 4   | 2     | 16  | \n',
                 '|8 | 271  | 285  | 30     | 10   | 5   | 2     | 30  | \n',
                 '+----------------------------------------------------+ \n']
            )
        )

    def test_import3(self):
        dt = get_databular_instance()
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/wholesale_trade.csv'
        dt.import_file(file_path=file_path)
        self.assertEqual(
            dt.display(),
            "".join(
                [
                    '+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ \n',
                    '|  | Series_reference | Period  | Data_value | Suppressed | STATUS | UNITS   | Magnitude | Subject                      | Group                                                              | Series_title_1             | Series_title_2           | Series_title_3 | Series_title_4 | Series_title_5 | \n',
                    '+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ \n',
                    '|0 | WTSQ.SFA1CA      | 1995.03 | 2368.69    | None       | F      | Dollars | 6         | Wholesale Trade Survey - WTS | Industry by variable - (ANZSIC06) - Subannual Financial Collection | Basic material wholesaling | Sales (operating income) | Current prices | Unadjusted     | None           | \n',
                    '|1 | WTSQ.SFA1CA      | 1995.06 | 2100.44    | None       | F      | Dollars | 6         | Wholesale Trade Survey - WTS | Industry by variable - (ANZSIC06) - Subannual Financial Collection | Basic material wholesaling | Sales (operating income) | Current prices | Unadjusted     | None           | \n',
                    '|2 | WTSQ.SFA1CA      | 1995.09 | 2070.21    | None       | F      | Dollars | 6         | Wholesale Trade Survey - WTS | Industry by variable - (ANZSIC06) - Subannual Financial Collection | Basic material wholesaling | Sales (operating income) | Current prices | Unadjusted     | None           | \n',
                    '|3 | WTSQ.SFA1CA      | 1995.12 | 2284.77    | None       | F      | Dollars | 6         | Wholesale Trade Survey - WTS | Industry by variable - (ANZSIC06) - Subannual Financial Collection | Basic material wholesaling | Sales (operating income) | Current prices | Unadjusted     | None           | \n',
                    '+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+ \n']
            )
        )


class Test_Export(unittest.TestCase):
    # test_export 1-3 are used to export the databular datat and see if the data is successfully export
    def test_export1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"]]
        )
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/export1.csv'
        dt.export_file(file_path=file_path)
        self.assertTrue(os.path.exists(file_path))

    def test_export2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/export2.csv'
        dt.export_file(file_path=file_path)
        self.assertTrue(os.path.exists(file_path))

    def test_export3(self):
        dt = get_databular_instance(
            column_names=[1, 2, 3],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        file_path = "/".join(os.path.dirname(__file__).split('/')[:-2]) + '/Sample Data/export3.csv'
        dt.export_file(file_path=file_path)
        self.assertTrue(os.path.exists(file_path))


class Test_Display(unittest.TestCase):
    # test_display 1-2 are used to see if the databular data is successfully display as expected
    def test_display1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.display(),
            "".join(
                ['+----------------------+ \n',
                 '|   | a  | b    | c    | \n',
                 '+----------------------+ \n',
                 '|0  | 10 | 2    | a    | \n',
                 '|1  | 1  | 2    | a    | \n',
                 '|2  | 1  | 2    | a    | \n',
                 '|3  | 1  | 55   | a    | \n',
                 '|4  | 1  | None | a    | \n',
                 '|5  | 1  | 2    | a    | \n',
                 '|6  | 1  | 2    | a    | \n',
                 '|7  | 1  | 2    | 12   | \n',
                 '|8  | 1  | 2    | None | \n',
                 '|9  | 1  | 2    | 500  | \n',
                 '|10 | 1  | 2    | abcd | \n',
                 '|11 | 1  | 2    | 3    | \n',
                 '+----------------------+ \n']
            )
        )

    def test_display2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.display(),
            "".join(
                ['+--------------+ \n',
                 '|  | a | b | c | \n',
                 '+--------------+ \n',
                 '|0 | 4 | 2 | 3 | \n',
                 '|1 | 1 | 2 | 3 | \n',
                 '+--------------+ \n']
            )
        )

    # test_top 1-2 are used to see if the top n of databular data is successfully display as expected
    def test_top1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.top(),
            "".join(
                ['+------------------+ \n',
                 '|  | a  | b    | c | \n',
                 '+------------------+ \n',
                 '|0 | 10 | 2    | a | \n',
                 '|1 | 1  | 2    | a | \n',
                 '|2 | 1  | 2    | a | \n',
                 '|3 | 1  | 55   | a | \n',
                 '|4 | 1  | None | a | \n',
                 '+------------------+ \n']

            )
        )

    def test_top2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.top(3),
            "".join(
                ['+---------------+ \n'
                 '|  | a  | b | c | \n'
                 '+---------------+ \n'
                 '|0 | 10 | 2 | a | \n'
                 '|1 | 1  | 2 | a | \n'
                 '|2 | 1  | 2 | a | \n'
                 '+---------------+ \n']
            )
        )

    # test_display 1-2 are used to see if the bottom n of databular data is successfully display as expected
    def test_bottom1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.bottom(),
            "".join(
                ['+------------------+ \n',
                 '|   | a | b | c    | \n',
                 '+------------------+ \n',
                 '|7  | 1 | 2 | 12   | \n',
                 '|8  | 1 | 2 | None | \n',
                 '|9  | 1 | 2 | 500  | \n',
                 '|10 | 1 | 2 | abcd | \n',
                 '|11 | 1 | 2 | 3    | \n',
                 '+------------------+ \n']
            )
        )

    def test_bottom2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.bottom(3),
            "".join(
                ['+------------------+ \n',
                 '|   | a | b | c    | \n',
                 '+------------------+ \n',
                 '|9  | 1 | 2 | 500  | \n',
                 '|10 | 1 | 2 | abcd | \n',
                 '|11 | 1 | 2 | 3    | \n',
                 '+------------------+ \n']
            )
        )

    # test_str 1-2 are used to see if the stringified version of the databular data is successfully obtained
    def test_str1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            str(dt),
            "".join(
                ['     a    b      c      \n',
                 '0    10   2      a      \n',
                 '1    1    2      a      \n',
                 '2    1    2      a      \n',
                 '3    1    55     a      \n',
                 '4    1    None   a      \n',
                 '5    1    2      a      \n',
                 '6    1    2      a      \n',
                 '7    1    2      12     \n',
                 '8    1    2      None   \n',
                 '9    1    2      500    \n',
                 '10   1    2      abcd   \n',
                 '11   1    2      3      ']
            )
        )

    def test_str2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )

        self.assertEqual(
            str(dt),
            "".join(
                ['    a    b   c   \n'
                 '0   10   2   a   \n'
                 '1   1    2   a   \n'
                 '2   1    2   a   ']
            )
        )

    # test_getitem 1-2 are used to see if the accessing of specific index or column of data is successfull
    def test_getitem1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            dt[3],
            {'a': [1],
             'b': [55],
             'c': ['a']}
        )

    def test_getitem2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            dt['c'],
            {'c': ['a', 'a', 'a', 'a', 'a', 'a', 'a', '12', None, '500', 'abcd', '3']}
        )


class Test_Information(unittest.TestCase):
    # test_describe 1-2 are used to check if the descriptive analysis of the data are displayed as expected
    def test_describe1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.describe(),
            "".join(
                ['+-----------------------------------------------------------------------+ \n',
                 '|a                      | b                     | c                     | \n',
                 '+-----------------------------------------------------------------------+ \n',
                 '|Data Type: int         | Data Type: int        | Data Type: str        | \n',
                 '|Total Count: 12        | Total Count: 12       | Total Count: 12       | \n',
                 '|Top Freq Value: 1      | Top Freq Value: 2     | Top Freq Value: a     | \n',
                 '|Top Freq Count: 11     | Top Freq Count: 10    | Top Freq Count: 7     | \n',
                 '|Mean: 1.75             | Mean: 6.81818         | Mean: -               | \n',
                 '|STD: 2.48747           | STD: 15.23643         | STD: -                | \n',
                 '|Has None Values: False | Has None Values: True | Has None Values: True | \n',
                 '+-----------------------------------------------------------------------+ \n']
            )
        )

    def test_describe2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[4, 2, 3],
                      [1, 2, 3]]
        )
        self.assertEqual(
            dt.describe(),
            "".join(
                ['+-------------------------------------------------------------------------+ \n',
                 '|a                      | b                      | c                      | \n',
                 '+-------------------------------------------------------------------------+ \n',
                 '|Data Type: int         | Data Type: int         | Data Type: int         | \n',
                 '|Total Count: 2         | Total Count: 2         | Total Count: 2         | \n',
                 '|Top Freq Value: 4      | Top Freq Value: 2      | Top Freq Value: 3      | \n',
                 '|Top Freq Count: 1      | Top Freq Count: 2      | Top Freq Count: 2      | \n',
                 '|Mean: 2.5              | Mean: 2.0              | Mean: 3.0              | \n',
                 '|STD: 1.5               | STD: 0.0               | STD: 0.0               | \n',
                 '|Has None Values: False | Has None Values: False | Has None Values: False | \n',
                 '+-------------------------------------------------------------------------+ \n']
            )
        )

    # test_search 1-2 are used to check if a specific search of the data yields results as expected
    def test_search1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            dt.search('b', '>', 2),
            "".join(
                ['+---------------+ \n'
                 '|  | a | b  | c | \n'
                 '+---------------+ \n'
                 '|0 | 1 | 55 | a | \n'
                 '+---------------+ \n']
            )
        )

    def test_search2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            dt.search('c', '==', 'a'),
            "".join(
                ['+------------------+ \n'
                 '|  | a  | b    | c | \n'
                 '+------------------+ \n'
                 '|0 | 10 | 2    | a | \n'
                 '|1 | 1  | 2    | a | \n'
                 '|2 | 1  | 2    | a | \n'
                 '|3 | 1  | 55   | a | \n'
                 '|4 | 1  | None | a | \n'
                 '|5 | 1  | 2    | a | \n'
                 '|6 | 1  | 2    | a | \n'
                 '+------------------+ \n']
            )
        )

    # test_getdata 1-2 are used to check if the copy of the data is obtained without any data misplacement
    def test_getdata1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )

        self.assertEqual(
            dt.get_data(),
            {'column_names': ['a', 'b', 'c'],
             'row_data': [[10, 2, 'a'],
                          [1, 2, 'a'],
                          [1, 2, 'a'],
                          [1, 55, 'a'],
                          [1, None, 'a'],
                          [1, 2, 'a'],
                          [1, 2, 'a'],
                          [1, 2, '12'],
                          [1, 2, None],
                          [1, 2, '500'],
                          [1, 2, 'abcd'],
                          [1, 2, '3']]}
        )

    def test_getdata2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"]]
        )

        self.assertEqual(
            dt.get_data(),
            {'column_names': ['a', 'b', 'c'],
             'row_data': [[10, 2, 'a'],
                          [1, 2, 'a'],
                          [1, 2, 'a'],
                          [1, 55, 'a'],
                          [1, None, 'a']]}
        )


class Test_Manipulation(unittest.TestCase):
    # test_addrecord 1-2 are used to check if the addition of a new row is successfully carried out
    def test_addrecord1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )
        dt.add_record([3, 2, 1])
        self.assertEqual(
            dt.display(),
            "".join(
                ['+---------------+ \n',
                 '|  | a  | b | c | \n',
                 '+---------------+ \n',
                 '|0 | 10 | 2 | a | \n',
                 '|1 | 1  | 2 | a | \n',
                 '|2 | 1  | 2 | a | \n',
                 '|3 | 3  | 2 | 1 | \n',
                 '+---------------+ \n']
            )
        )

    def test_addrecord2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )
        dt.add_record(['a', 'b', 'c'])
        self.assertEqual(
            dt.display(),
            "".join(
                ['+---------------+ \n',
                 '|  | a  | b | c | \n',
                 '+---------------+ \n',
                 '|0 | 10 | 2 | a | \n',
                 '|1 | 1  | 2 | a | \n',
                 '|2 | 1  | 2 | a | \n',
                 '|3 | a  | b | c | \n',
                 '+---------------+ \n']
            )
        )

    # test_addcolumn 1-2 are used to check if the addition of a new column is successfully carried out
    def test_addcolumn1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )

        dt.add_column(
            column_name='d',
            column_data=[True, False, True]
        )

        self.assertEqual(
            dt.display(),
            "".join(
                ['+-----------------------+ \n',
                 '|  | a  | b | c | d     | \n',
                 '+-----------------------+ \n',
                 '|0 | 10 | 2 | a | True  | \n',
                 '|1 | 1  | 2 | a | False | \n',
                 '|2 | 1  | 2 | a | True  | \n',
                 '+-----------------------+ \n']
            )
        )

    def test_addcolumn2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )

        dt.add_column(
            column_name='_100',
            column_data=[100] * 3
        )

        self.assertEqual(
            dt.display(),
            "".join(
                ['+----------------------+ \n',
                 '|  | a  | b | c | _100 | \n',
                 '+----------------------+ \n',
                 '|0 | 10 | 2 | a | 100  | \n',
                 '|1 | 1  | 2 | a | 100  | \n',
                 '|2 | 1  | 2 | a | 100  | \n',
                 '+----------------------+ \n']
            )
        )

    # test_filter 1-2 are used to check if a specific filter of the data based on a condition yields results as expected
    def test_filter1(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        dt.filter('b', '>', 2)

        self.assertEqual(
            dt.display(),
            "".join(
                ['+---------------+ \n'
                 '|  | a | b  | c | \n'
                 '+---------------+ \n'
                 '|0 | 1 | 55 | a | \n'
                 '+---------------+ \n']
            )
        )

    def test_filter2(self):
        dt = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        dt.filter('c', '==', 'a')

        self.assertEqual(
            dt.display(),
            "".join(
                ['+------------------+ \n'
                 '|  | a  | b    | c | \n'
                 '+------------------+ \n'
                 '|0 | 10 | 2    | a | \n'
                 '|1 | 1  | 2    | a | \n'
                 '|2 | 1  | 2    | a | \n'
                 '|3 | 1  | 55   | a | \n'
                 '|4 | 1  | None | a | \n'
                 '|5 | 1  | 2    | a | \n'
                 '|6 | 1  | 2    | a | \n'
                 '+------------------+ \n']
            )
        )

    # test_merge 1-2 are used to see if two databular tables are successfully merging
    def test_merge1(self):
        dt1 = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1 * 10, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, "a"]]
        )

        dt2 = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1, '5' * 2, "a"],
                      [1, "", "a"],
                      [1, 2, "a"],
                      [1, 2, "a"],
                      [1, 2, '12'],
                      [1, 2, None],
                      [1, 2, 500],
                      [1, 2, "abcd"],
                      [1, 2, 3]]
        )
        dt1.merge(dt2)

        self.assertEqual(
            dt1.display(),
            "".join(
                ['+----------------------+ \n',
                 '|   | a  | b    | c    | \n',
                 '+----------------------+ \n',
                 '|0  | 10 | 2    | a    | \n',
                 '|1  | 1  | 2    | a    | \n',
                 '|2  | 1  | 2    | a    | \n',
                 '|3  | 1  | 55   | a    | \n',
                 '|4  | 1  | None | a    | \n',
                 '|5  | 1  | 2    | a    | \n',
                 '|6  | 1  | 2    | a    | \n',
                 '|7  | 1  | 2    | 12   | \n',
                 '|8  | 1  | 2    | None | \n',
                 '|9  | 1  | 2    | 500  | \n',
                 '|10 | 1  | 2    | abcd | \n',
                 '|11 | 1  | 2    | 3    | \n',
                 '+----------------------+ \n']
            )
        )

    def test_merge2(self):
        dt1 = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[4, 2, 3]]
        )
        dt2 = get_databular_instance(
            column_names=['a', 'b', 'c'],
            row_data=[[1, 2, 3]]
        )
        dt1.merge(dt2)
        self.assertEqual(
            dt1.display(),
            "".join(
                ['+--------------+ \n',
                 '|  | a | b | c | \n',
                 '+--------------+ \n',
                 '|0 | 4 | 2 | 3 | \n',
                 '|1 | 1 | 2 | 3 | \n',
                 '+--------------+ \n']
            )
            )
