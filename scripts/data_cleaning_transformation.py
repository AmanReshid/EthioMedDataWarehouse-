# Task 2: Data Cleaning and Transformation

import os
import pandas as pd
import logging
import emoji
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables for database credentials
load_dotenv('.env')

# Set up logging
logging.basicConfig(filename='data_cleaning.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    """
    Load the scraped data into a pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data loaded successfully from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None

def missing_values_table(df):
    # Total missing values
    mis_val = df.isnull().sum()

    # Percentage of missing values
    mis_val_percent = 100 * df.isnull().sum() / len(df)

    # dtype of missing values
    mis_val_dtype = df.dtypes

    # Make a table with the results
    mis_val_table = pd.concat([mis_val, mis_val_percent, mis_val_dtype], axis=1)

    # Rename the columns
    mis_val_table_ren_columns = mis_val_table.rename(
        columns={0: 'Missing Values', 1: '% of Total Values', 2: 'Dtype'})

    # Sort the table by percentage of missing descending
    mis_val_table_ren_columns = mis_val_table_ren_columns[
        mis_val_table_ren_columns['% of Total Values'] != 0].sort_values(
        '% of Total Values', ascending=False).round(1)

    # Print some summary information
    print("Your selected dataframe has " + str(df.shape[1]) + " columns.\n"
          "There are " + str(mis_val_table_ren_columns.shape[0]) +
          " columns that have missing values.")

    # Return the dataframe with missing information
    return mis_val_table_ren_columns



def store_cleaned_data(df, table_name):
    """
    Store the cleaned data into a PostgreSQL database.
    """
    try:
        db_url = os.getenv('DATABASE_URL')  # Make sure to define this in the .env file
        engine = create_engine(db_url)
        
        # Store the cleaned data into PostgreSQL
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Cleaned data stored in the {table_name} table in PostgreSQL")
    except Exception as e:
        logging.error(f"Error storing cleaned data: {e}")

def setup_dbt_project(project_name):
    """
    Set up the DBT project for data transformation.
    """
    try:
        os.system(f'dbt init {project_name}')
        logging.info(f"DBT project {project_name} initialized successfully")
    except Exception as e:
        logging.error(f"Error initializing DBT project: {e}")

def run_dbt_models():
    """
    Run the DBT models to perform transformations.
    """
    try:
        os.system('dbt run')
        logging.info("DBT models ran successfully")
    except Exception as e:
        logging.error(f"Error running DBT models: {e}")

def dbt_testing_and_docs():
    """
    Run DBT tests and generate documentation.
    """
    try:
        os.system('dbt test')
        logging.info("DBT testing completed")

        os.system('dbt docs generate')
        os.system('dbt docs serve')
        logging.info("DBT documentation generated and served")
    except Exception as e:
        logging.error(f"Error in DBT testing or documentation: {e}")

def remove_emojis(text):
    return emoji.replace_emoji(text, replace='')

def summary(df):
    """Generate a summary of columns in the DataFrame."""
    summary_data = []
    
    for col_name in df.columns:
        col_dtype = df[col_name].dtype
        num_of_nulls = df[col_name].isnull().sum()
        num_of_non_nulls = df[col_name].notnull().sum()
        num_of_distinct_values = df[col_name].nunique()
        
        if num_of_distinct_values <= 10:
            distinct_values_counts = df[col_name].value_counts().to_dict()
        else:
            top_10_values_counts = df[col_name].value_counts().head(10).to_dict()
            distinct_values_counts = {k: v for k, v in sorted(top_10_values_counts.items(), key=lambda item: item[1], reverse=True)}

        summary_data.append({
            'col_name': col_name,
            'col_dtype': col_dtype,
            'num_of_nulls': num_of_nulls,
            'num_of_non_nulls': num_of_non_nulls,
            'num_of_distinct_values': num_of_distinct_values,
            'distinct_values_counts': distinct_values_counts
        })
    
    df = pd.DataFrame(summary_data)
    return df