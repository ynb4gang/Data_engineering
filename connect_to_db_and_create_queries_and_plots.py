import psycopg2
from psycopg2 import sql
import pandas as pd
import matplotlib.pyplot as plt

class DatabaseHandler:
    def __init__(self, connection_params, base_table):
        """
        Initialized the DatabaseHandler with a connection to the PostgreSQL database.

        :param connection_params: A dictionary containing connection parameters.
        """
        self.iterator_for_count_temp_table = 0
        self.cursor = None
        self.schema_name = None
        self.table_name = None
        self.base_table = base_table
        self.connections_params = connection_params
        self.prev_temp_table = None
        self.columns = []
        self.filters = []
        self.temp_tables_history_create = []
        self.sql_queries = ""

        self.parse_base_table(connection_params, base_table)
        self.connect_to_database(connection_params)

    def connect_to_database(self, connection_params):
        """
        Establish connection to the PostgreSQL database.
        """
        try:
            conn = psycopg2.connect(
                dbname=connection_params.get('dbname'),
                user=connection_params.get('user'),
                password=connection_params.get('password'),
                host=connection_params.get('host'),
                port=connection_params.get('port')
            )

            self.cursor = conn.cursor()

            self.columns = self.fetch_columns()

            print("Database connection established successfully.")

        except Exception as e:
            print(f"Error connecting to the database: {e}")
            self.cursor = None

    def parse_base_table(self, connection_params, base_table):
        """
        Parse the base table to separate schema and table name.
        """
        if '.' in base_table:
            self.schema_name, self.table_name = base_table.split('.')
        else:
            self.schema_name = 'schema_name'
            self.table_name = base_table

    def fetch_columns(self):
        """
        Fetch column names from the specified table.
        """
        try:
            self.cursor.execute(
                sql.SQL("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    ORDER BY ordinal_position;
                    """), ['schema_name', 'table_name']
                )
            columns = [row[0] for row in self.cursor.fetchall()]
            return columns
        except Exception as e:
            print(f"Error fetching columns for table {self.base_table}: {e}")
            return []

    def set_filter(self, column_name, column_value, filter_type='include'):
        """
        Set filter conditions based on the column name, values, and type of filter.

        :param column_name: The column to filter on.
        :param column_value: The value(s) to filter.
        :param filter_type: The type of filter - 'include' or 'exclude'.
        """
        if isinstance(column_value, list):
            if filter_type == 'include':
                filter_str = f"{column_name} IN ({', '.join([repr(value) for value in column_value])})"
            elif filter_type == 'exclude':
                filter_str = f"{column_name} NOT IN ({', '.join([repr(value) for value in column_value])})"
        else:
            if filter_type == 'include':
                filter_str = f"{column_name} = '{column_value}'"
            elif filter_type == 'exclude':
                filter_str = f"{column_name} != '{column_value}'"

        self.filters.append(filter_str)

    def temp_table_name(self, column_name, column_value):
        """
        Generate a temporary table name.
        """
        self.iterator_for_count_temp_table += 1
        temp_table_name = f"temp{self.iterator_for_count_temp_table}"
        return temp_table_name

    def create_query(self, column_name, column_value, filter_type='include',*args):
        """
        Create SQL query based on filter criteria.
        """
        self.filters = []

        self.set_filter(column_name, column_value, filter_type)
        temp_name = self.temp_table_name(column_name, column_value)

        self.columns = [col for col in self.columns if col != column_name]

        if temp_name not in self.temp_tables_history_create:
            self.temp_tables_history_create.append(temp_name)

            drop_query = f"DROP TABLE IF EXISTS {temp_name}"

            columns_str = ", ".join(self.columns)

            group_by_columns_str = ", ".join(self.columns)

            filters_str = " AND ".join(self.filters)

            source_table = self.prev_temp_table if self.prev_temp_table else f"{self.schema_name}.{self.table_name}"

            create_query = f"""
                CREATE TEMPORARY TABLE {temp_name}
                               WITH (appendonly=true, compresstype=zstd, compresslevel=1)
                               on commit preserve rows
                               AS
                (SELECT * FROM (
                    SELECT {columns_str}
                    FROM {source_table}
                    WHERE {filters_str}
                    GROUP BY {group_by_columns_str}) as a
                    ) with data DISTRIBUTED RANDOMLY;
                    analyze {temp_name};
                    select * from {temp_name};
            """

            self.sql_queries += drop_query + ";"
            self.sql_queries += create_query + "\n"
            self.prev_temp_table = temp_name

            return self.sql_queries

    def close_connection(self):
        """
        Close the database connection.
        """
        if self.cursor:
            self.cursor.close()
            print("Database connection closed.")

    def execute_query_to_df(self, query):
        """
        Execute SQL query and return result as DataFrame.
        """
        try:
            print(f"Executing query: {query}")
            self.cursor.execute(query)
            df = pd.DataFrame(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])
            return df
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Exception: {e}")
            return pd.DataFrame()

    def aggregate_and_plot(self, df):
      ## if need to change the type 
        df['your_column'] = pd.to_numeric(df['your_column'], errors='coerce')
        aggregated_df = df.agg({
            'example_column' : 'sum',
        })
        print("Aggregated Data:")
        print(aggregated_df)

        df['your_date'] = pd.to_datetime(df['your_date'])
        df['year'] = df['your_date'].dt.year

        columns_to_plot = ['example_column1', 'example_column2','example_column3']


        for column in columns_to_plot:
            if column in df.colunms:
                yearly_data = df.groupby('year')[column].sum()
                plt.figure()
                yearly_data.plot(kind='bar')
                plt.title(f'{column} by Year')
                plt.xlabel('Year')
                plt.ylabel(column)
                plt.grid(axis='y')
                plt.show()

                quarterly_data = df.groupby('quarter')[column].sum()
                plt.figure()
                quarterly_data.plot(kind='bar')
                plt.title(f'{column} by Quarter')
                plt.xlabel('Quarter')
                plt.ylabel(column)
                plt.grid(axis='y')
                plt.show()

        @staticmethod
        def plot_custom_field(df, field_name):
            df['your_date'] = pd.to_datetime(df['your_date'])
            df['year'] = df['your_date'].dt.year

            if field_name in df.columns:
                yearly_data = df.groupby('year')[field_name].sum()
                plt.figure()
                yearly_data.plot(kind='bar')
                plt.title(f'{field_name} by Year')
                plt.xlabel('Year')
                plt.ylabel(field_name)
                plt.grid(axis='y')
                plt.show()
            else:
                print(f"Field '{field_name}' not found in the DataFrame.")
