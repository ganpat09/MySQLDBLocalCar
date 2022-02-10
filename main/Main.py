import os

from db.DbTableConnection import DbTableConnection
class Main:

    def __init__(self):

        dbtconx = DbTableConnection("localhost","root","root","cardataset")
        dbtconx.create_table("car","buying VARCHAR(10), maint VARCHAR(10), doors VARCHAR(10), persons VARCHAR(10), lug_boot VARCHAR(10), safety VARCHAR(10), extra VARCHAR(10)")
        dbtconx.show_tables()
        dbtconx.insert_data_from_file('asset//car.data')
        dbtconx.check_data_complete('asset//car.data')
        dbtconx.fetch_all_data()
        dbtconx.get_count_of_group_by("buying")
        dbtconx.exact_filter_on_single_col("doors","4")
        dbtconx.update(set_col_name="doors",set_col_value="8",search_col_name="doors",search_col_value="2")
        dbtconx.delete_table()
        dbtconx.delete_db()

if __name__ == '__main__':
    Main()