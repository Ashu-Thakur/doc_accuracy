from sqlalchemy import create_engine
import psycopg2
from psycopg2 import Error

class Database:
    def __init__(self, db_params):
        self.db_params = db_params
        self.connection = None
        self.cursor = None
        self.engine = self.create_engine()

    def create_engine(self):
        """Create a SQLAlchemy engine for connecting to the PostgreSQL database."""
        connection_string = f"postgresql+psycopg2://{self.db_params['user']}:" \
                            f"{self.db_params['password']}@" \
                            f"{self.db_params['host']}:" \
                            f"{self.db_params['port']}/{self.db_params['dbname']}"
        return create_engine(connection_string)

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor()
            print("Database connection successful.")
            return True
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("PostgreSQL connection is closed.")
        print("Database connection closed.")

    def execute_query(self, query, params=None):
        """Execute a query and return results for SELECT queries."""
        with self.engine.connect() as connection:
            result = connection.execute(query, params)
            if query.strip().upper().startswith("SELECT"):
                return result.fetchall()
            else:
                print("Query executed successfully.")

    def store_dataframe_to_db(self, records_df, table_name):
        """Insert a DataFrame into the specified table."""
        if not records_df.empty: 
            try:
                records_df.to_sql(table_name, self.engine, if_exists = 'append', index=False)
                print("Data successfully added to the database.")
            except Exception as e:
                print(f"Error executing insert query: {e}")
        else:
            print("No records to insert.")


            



