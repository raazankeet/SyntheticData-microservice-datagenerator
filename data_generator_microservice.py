from flask import Flask, request, jsonify
from flask_cors import CORS
from faker_data_generators import *  # Assuming all your generator functions are in this file
from data_records_inserts import insert_records_method

from loguru import logger
import os
import pyodbc
import yaml

app = Flask(__name__)
CORS(app)
app.json.sort_keys = False

# Ensure the logs directory exists
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure the logger to log to 'dbscanner.log'
logger.add(
    os.path.join(log_directory, "datagenerator.log"),  # Log file path
    rotation="1 week",  # Log rotation based on time
    level="INFO",  # Log level
    compression="zip"  # Optional: compress log files when rotated
)

# Example of logging a test message
logger.info("Data generator microservice started!")

app.json.sort_keys = False

# Load config from YAML
def load_config():
    try:
        with open("appconfig.yml", "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        raise ValueError("Failed to load config file.")

config = load_config()
db_config = config.get("sql-server-database", {})

# Construct the connection string dynamically
connection_string = (
    f"Driver={db_config.get('driver', '')};"
    f"Server={db_config.get('server', '')};"
    f"Database={db_config.get('database', '')};"
    f"UID={db_config.get('uid', '')};"
    f"PWD={db_config.get('pwd', '')};"
)

# Switch for enabling/disabling terminal logging
DEV_MODE = False  # Set to False for production



def connect_to_db():
    try:
        connection =pyodbc.connect(connection_string)
        
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

# Function to fetch the appropriate generator dynamically
def get_generator_function(generator_name):
    return globals().get(generator_name)

app.json.sort_keys = False

@app.route('/submit', methods=['POST'])
def parse_json():
    # Check if JSON is provided in the request body
    if request.is_json:
        data = request.get_json()  # Parse the JSON data

        # Extract and process relevant data from the JSON
        central_table_metadata = data.get('central_table_metadata', {})
        parent_tables_metadata = data.get('parent_tables_metadata', {})
        child_tables_metadata = data.get('child_tables_metadata', {})
        constraints = data.get('constraints', [])

        # Generate synthetic data and return it in the response
        generated_data = generate_synthetic_data(central_table_metadata, parent_tables_metadata, child_tables_metadata, constraints)
        logger.info("Synthetic data is generated...")
        
        try:
            output=(insert_records_method(generated_data))

            # logger.info(output)

            return jsonify({
            "message": "Synthetic data generated succesfully.",
            "response_code": 0,
            "details" :output
        })
       
        # Example response to confirm receipt and parsing
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            return jsonify({"error": "Data insertion failed"}), 400

    else:
        return jsonify({"error": "Invalid JSON"}), 400
    
# Function to fetch primary key data from the database
def fetch_parent_primary_keys_from_db(parent_table_name, pk_column_name, limit):
    """Fetch primary keys from the parent table in the database using the actual PK column name."""
    # Example: You might use an actual database connection here.
    # This is a simple SQLite example, adjust as per your actual DB connection.
    # conn = sqlite3.connect('your_database.db')
    # cursor = conn.cursor()

    try:
        connection = connect_to_db()

        with connection.cursor() as cursor:
            query = f"SELECT top {limit} {pk_column_name} FROM {parent_table_name};"
            print(query)
            cursor.execute(query)
            parent_keys = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"Trying to fetch {limit} Parent keys were {parent_table_name}.")
    except Exception as e:
        logger.error(f"Error accessing table {parent_table_name}: {e}")
        connection.rollback()  # Rollback in case of error
        raise



    # Query to fetch only the primary key column (dynamically using pk_column_name)
    # query = f"SELECT {pk_column_name} FROM {parent_table_name} LIMIT {limit};"
    # cursor.execute(query)
    # parent_keys = cursor.fetchall()  # Fetch the primary keys
    # conn.close()
    #parent_keys=[x for x in range (1,limit+1)]
    #logger.info(f"in fetch for table:{parent_table_name} to find pk_column_name:{pk_column_name} with limit:{limit}")
    #logger.info(f"parent_keys:{parent_keys}")
    
    # Flatten the list of tuples to just a list of primary keys
    
    #return [key[0] for key in parent_keys]
    return parent_keys

def generate_synthetic_data(central_table_metadata, parent_tables_metadata, child_tables_metadata, constraints):
    logger.info("Generating Synthetic data")

    dict_parent_primary_keys = {}  # To store generated primary keys
    dict_pk_fk_relationships = {}  # To store relationships

    # Initialize parent_column_dict with the keys for parent tables
    for constraint in constraints:
        parent_table = constraint.get('parent_table').lower()
        parent_column = constraint.get('parent_column').lower()
        child_table = constraint.get('child_table').lower()
        child_column = constraint.get('child_column').lower()

        parent_key = f"{parent_table}.{parent_column}"
        child_key = f"{child_table}.{child_column}"

        dict_pk_fk_relationships[child_key] = parent_key

        # Initialize an empty list if this parent column hasn't been encountered yet
        if parent_key not in dict_parent_primary_keys:
            dict_parent_primary_keys[parent_key] = []

    # Generate data for parent tables based on the constraints
    parent_table_data = generate_parent_table_data(central_table_metadata,  dict_parent_primary_keys)

    # Generate data for child tables based on the constraints
    child_table_data = generate_child_table_data(child_tables_metadata, dict_parent_primary_keys,dict_pk_fk_relationships)

    logger.debug(f"Parent Column Dict with Generated Keys: {dict_parent_primary_keys}")
    logger.debug(f"PK-FK Relationships: {dict_pk_fk_relationships}")

    return {
        "parent_tables": parent_table_data,
        "child_tables": child_table_data,
    }

def generate_parent_table_data(central_table_metadata, dict_parent_primary_keys):
  
    generated_data = []  # List to hold table data with metadata

    # Generate synthetic data for each table
    for table_name, table_metadata in central_table_metadata.items():
        generate_data = table_metadata.get("generate_data")
        truncate_table = table_metadata.get("truncate_table")
        existing_record_count = table_metadata.get("existing_record_count")
        records_to_generate = table_metadata.get("records_to_generate")
        
        logger.info(f"Processing parent table {table_name}")
        logger.debug(f"Metadata details --> Generate data: {generate_data}, Truncate table:{truncate_table}, Existing records counts:{existing_record_count}, Records to generate:{records_to_generate} ")

        table_rows = []  # Hold rows for the current table

        if (generate_data):

            for _ in range(records_to_generate):
                table_row = {}

                for column in table_metadata['columns']:
                    column_name = column['COLUMN_NAME']
                    generator_name = column['selected_generator']
                    generator_func = get_generator_function(generator_name)

                    if generator_func:
                        generated_value = generator_func()  # Call the generator function
                    else:
                        generated_value = None  # No generator defined, set as None

                    table_row[column_name] = generated_value

                    current_node = f"{table_name}.{column_name}".lower()
                    if current_node in dict_parent_primary_keys:
                        dict_parent_primary_keys[current_node].append(generated_value)  # Append to the list

                table_rows.append(table_row)

            # Add table metadata and rows to the generated data
        generated_data.append({
            "table_type": "central",  # Central table type
            "table_name": table_name,
            "truncate_table": truncate_table,
            "columns": table_rows
            })

    return generated_data

def generate_child_table_data(child_tables_metadata, dict_parent_primary_keys, dict_pk_fk_relationships):

    generated_data = []  # List to hold child table data with metadata

    for table_name, table_metadata in child_tables_metadata.items():
        generate_data = table_metadata.get("generate_data")
        records_to_generate = table_metadata.get("records_to_generate")
        truncate_table = table_metadata.get("truncate_table")
        existing_record_count = table_metadata.get("existing_record_count")
        reusability_pct = table_metadata.get("reusability_pct",0)

        logger.info(f"Processing child table {table_name}")
        logger.debug(f"Metadata details --> Generate data: {generate_data}, Truncate table:{truncate_table}, Existing records counts:{existing_record_count}, Records to generate:{records_to_generate}, Reusability Percentage:{reusability_pct} ")

        reusable_records = []

        if (generate_data):
            # Initialize a list to hold all reusable records
           

            # Loop through each column in the child table and find its parent table/column
            for column in table_metadata['columns']:
                column_name = column['COLUMN_NAME']
                parent_table_name = None
                pk_column_name = None  # We'll dynamically fetch the primary key column name

                # Look for the parent table and its primary key column associated with the foreign key column
                child_column = f"{table_name.lower()}.{column_name.lower()}"
                if child_column in dict_pk_fk_relationships:
                    parent_column = dict_pk_fk_relationships[child_column]
                    parent_table_name, pk_column_name = parent_column.split('.')

                    # Calculate reusable records based on the reusability percentage
                    parent_keys_generated_in_session = dict_parent_primary_keys.get(f"{parent_table_name.lower()}.{pk_column_name.lower()}", [])
                    reusable_records_count = int(len(parent_keys_generated_in_session) * (reusability_pct / 100))

                    # Use the parent keys generated in the current session if available
                    for _ in range(reusable_records_count):
                        table_row = {}

                        for col in table_metadata['columns']:
                            col_name = col['COLUMN_NAME']

                            if col_name.lower() == column_name.lower():
                                # Reuse parent keys
                                parent_value = random.choice(parent_keys_generated_in_session)
                                table_row[col_name] = parent_value
                            else:
                                # Generate other values for the child table
                                generator_name = col['selected_generator']
                                generator_func = get_generator_function(generator_name)
                                table_row[col_name] = generator_func() if generator_func else None

                        reusable_records.append(table_row)

            # Calculate how many new records need to be generated
            new_records_count = records_to_generate - len(reusable_records)

            # Fetch remaining records from the parent table if necessary
            if new_records_count > 0:
                logger.debug(f"Fetching {new_records_count} more parent keys from the database")
                
                # For each column, determine which parent table to query for additional keys
                for column in table_metadata['columns']:
                    column_name = column['COLUMN_NAME']
                    child_column = f"{table_name.lower()}.{column_name.lower()}"
                    if child_column in dict_pk_fk_relationships:
                        parent_column = dict_pk_fk_relationships[child_column]
                        parent_table_name, pk_column_name = parent_column.split('.')

                        parent_keys = fetch_parent_primary_keys_from_db(parent_table_name, pk_column_name, new_records_count)

                        for _ in range(new_records_count):
                            table_row = {}

                            for col in table_metadata['columns']:
                                col_name = col['COLUMN_NAME']

                                if col_name.lower() == column_name.lower():
                                    # Reuse keys from the fetched parent records
                                    parent_value = random.choice(parent_keys)
                                    table_row[col_name] = parent_value
                                else:
                                    # Generate other values for the child table
                                    generator_name = col['selected_generator']
                                    generator_func = get_generator_function(generator_name)
                                    table_row[col_name] = generator_func() if generator_func else None

                            reusable_records.append(table_row)


        generated_data.append({
                "table_type": "child",  # Child table type
                "table_name": table_name,
                "truncate_table": truncate_table,
                "columns": reusable_records
            })

    return generated_data




if __name__ == '__main__':
    app.run(debug=True, port=5001)
