from datetime import datetime
import sys
# to import local fuctions
sys.path.insert(0, '../tools')

import sqlalchemy as db
import pandas as pd

class PostgresTable:
    def __init__(self, postgresql_url, table_name):
        self.postgresql_url = postgresql_url
        self.table_name = table_name
        self.engine = db.create_engine(self.postgresql_url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.tables_in_db = self.engine.table_names()

        # Check if table is in the database
        if self.table_name in self.tables_in_db:

            # Create instance of Table
            self.table = db.Table(self.table_name, self.metadata, autoload=True, autoload_with=self.engine)

        else:
            print('{} does not exist in the database!'.format(self.table_name))

    def load_data(self, data_to_load, id_columns, is_replace=False):
        """
        Function to load data into a PostgreSQL table if columns and table exist.

        Arguments:
        - data_to_load(list): List of dictionaries with values to upload, one key per column in table.
        - is_replace(boolean): Indicates whether it should completely replace the table and erase all data.

        Return:
        - query_result(sqlalchemy.engine.result): Result after running the query.
        """
        # Define upload time
        upload_time = datetime.now().strftime('%d/%m/%y %H:%M:%S')

        # If data_to_load is a DataFrame then convert it to a list with dictionaries
        if type(data_to_load) == pd.DataFrame:
            # Create Upload Datetime column
            data_to_load['upload_datetime'] = upload_time
            #Create Id Column
            data_to_load[id_columns] = data_to_load[id_columns].astype(str)
            data_to_load['id'] = data_to_load[id_columns].agg(''.join, axis=1).apply(lambda x: hash(x))
            # Convert DataFrame to list of dictionaries
            data_to_load = data_to_load.to_dict('records')

        # Pick First Row, list of keys and indexes of columns in id_columns
        first_row = data_to_load[0]

        # Check if columns match
        # Pull keys from database
        query = 'select * from ' + self.table_name + ' limit 1'
        table_cols = set(self.connection.execute(query).keys())

        # Get list of keys from first item in dictionary
        to_load_cols = set(first_row.keys())

        # Get differences between columns
        table_cols_diff = set(table_cols) - set(to_load_cols)
        to_load_cols_diff = set(to_load_cols) - set(table_cols)

        # If columns match, then we are good
        if table_cols == to_load_cols:

            # If the method is 'replace', then first it will drop all data in table
            if is_replace == True:

                # Drop all data from table
                # query_delete = db.delete(self.table)
                # self.connection.execute(query_delete)
                self.clean_table()

            # Insert data to table
            query_result = self.connection.execute(db.insert(self.table), data_to_load)

            return query_result

        else:
            print('Columns do not match!')
            print('In Table but not in data: {}'.format(table_cols_diff))
            print('In Data but not in Table: {}'.format(to_load_cols_diff))

    def read_table_to_pandas(self, drop_upload_date=False):
        """
        Function to read table from Postgres Database and return it as a Pandas DataFrame

        Arguments:
        - postgresql_url(str): PostgreSQL URL to access the database.
        - table_name(str): Table Name in the database.

        Return:
        - df_query(pd.DataFrame): DataFrame with all data from table_name.
        """
        # Pull all data from table and return it as a Pandas DataFrame
        query = f'select * from {self.table_name}'
        df_query = pd.read_sql_query(query, self.engine)

        # Drop upload_datetime column if requested
        if drop_upload_date == True:
            df_query = df_query.drop('upload_datetime', axis=1)

        return df_query

    def clean_table(self):
        # Drop all data from table
        query_delete = db.delete(self.table)
        self.connection.execute(query_delete)

    def max_column_value(self, column_name):
        # Calculate Max Upload Date
        query = f'select max({column_name}) from {self.table_name}'
        return [row for row in self.engine.connect().execute(query)][0][0]

    def extract_column_to_list(self, column_name):
        # Read table and bring all values in list
        query = f'select {column_name} from {self.table_name}'
        return [row[0] for row in self.connection.execute(query)]

    def remove_duplicates(self, id_column, partition_by_columns, order_by_column):
        # Remove duplicates based on when a record was created
        query = f"""
                    delete from {self.table_name} where {id_column} not in
                    (
                        with base as
                        (
                            select
                                {id_column}
                                , row_number() over (partition by {partition_by_columns} order by {order_by_column}) as row_number
                            from {self.table_name}
                        )
                        select {id_column} from base where row_number = 1
                    )
                """
        return self.connection.execute(query)

def connect_to_db(postgresql_url):
    """
    Connect to database and create instances of engine, connection and metadata
    """
    engine = db.create_engine(postgresql_url)
    connection = engine.connect()
    metadata = db.MetaData()
    return engine, connection, metadata

def create_new_table(postgresql_url, new_table_name, drop_current_table=False, table_reference_path='../docs/tables.csv'):
    """
    Function to create a new table in the database, with the option of dropping the existing one.
    """
    # Connect to Database: Create instances of engine, connection and metadata
    engine, connection, metadata = connect_to_db(postgresql_url)

    # Read Table Reference File
    table_reference = pd.read_csv(table_reference_path)

    # List of Tables in Database
    tables_in_db = engine.table_names()

    # Dictionary with Data Type string to Data Type Instance
    data_type_dictionary = {'integer': db.Integer()
                            , 'string': db.String()
                            , 'date': db.Date()
                            , 'float': db.Float()
                            , 'datetime': db.DateTime()}

    # Drop Table in Database
    # If asked to drop, see if it exists and drop it
    if new_table_name in tables_in_db and drop_current_table == True:
        drop_table = db.Table(new_table_name, metadata, autoload=True, autoload_with=engine)
        drop_table.drop(engine)
    # If not asked to drop, see if it exists in the database, because then it should be dropped first
    elif new_table_name in tables_in_db and drop_current_table == False:
        return new_table_name + ' exists in the database! Drop it to recreate it.'

    # Create New Table
    # Pull Reference from csv
    new_table_reference = table_reference[table_reference['table_name'] == new_table_name]

    # Check if table is in reference file
    if len(new_table_reference) == 0:
        return new_table_name + ' not in reference file!'

    # Create Lists: Column Names, Data Types, Nullable Flags, Primary Key Flags, Unique Flags and Postgres Data Types
    column_names = new_table_reference['column_name'].to_list()
    data_types_str = new_table_reference['data_type'].to_list()
    nullable_flags = new_table_reference['null'].to_list()
    primary_key_flags = new_table_reference['primary_key'].to_list()
    unique_flags = new_table_reference['unique'].to_list()
    data_types = [data_type_dictionary[i] for i in data_types_str]

    # Update database connection - It's necessary if table was dropped, otherwise, it will think that the table is still in the db.
    engine, connection, metadata = connect_to_db(postgresql_url)

    # Create instance of New Table
    db.Table(new_table_name, metadata,
                        *(db.Column(column_name,
                                    column_type,
                                    primary_key=primary_key_flag,
                                    nullable=nullable_flag,
                                    unique=unique_flag)

                                    for column_name,
                                        column_type,
                                        primary_key_flag,
                                        nullable_flag,
                                        unique_flag in zip(column_names,
                                                            data_types,
                                                            primary_key_flags,
                                                            nullable_flags,
                                                            unique_flags)))
    # Create New Table
    metadata.create_all(engine)

    # Check if Table was created
    tables_in_db = engine.table_names()
    if new_table_name in tables_in_db:
        return new_table_name + ' was created! :)'
    else:
        return new_table_name + ' was not found in the database! :('
