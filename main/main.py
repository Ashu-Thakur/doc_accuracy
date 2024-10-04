import boto3
import json
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import database as db

def get_list_of_files(s3, bucket_name, key):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
    files_data = {}
    
    for item in response.get('Contents', []):
        if item["Key"].endswith('.json'):
            file_name = item["Key"].split("/")[-1].split(".")[0]
            if file_name in files_data:
                print(f"Duplicate file detected: {file_name}, updating with latest value.")
            files_data[file_name] = {
                "processed_date": item["LastModified"],
                "path": item["Key"]
            }
            print(f"{file_name} ---> {files_data[file_name]}")
    
    return files_data

def get_data_of_file(s3, bucket_name, key, file_name):
    try:
        print(f"Fetching data from ---> {bucket_name}/{key}/{file_name}")
        response = s3.get_object(Bucket=bucket_name, Key=f"{key}/{file_name}.json")
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        total_attributes = [item['value'] for item in data['data']['labels'] if item['value'] != "!:Attention"]
        actions = []
        total_extracted_attrib = []

        for item in data.get("predictions", []):
            if item["model_version"] == "user_review":
                actions = item["action"]
            elif item["model_version"] == "old_parser":
                total_extracted_attrib = item["result"][1:]

        # Prepare data for DataFrame in a single pass
        rows = []
        action_performed_attributes_ids = set()

        for action in actions:
            attrib_name = action['value']['labels'][0]
            action_performed_attributes_ids.add(action['id'])
            action_type = action["action"]
            old_value = None
            new_value = None
            
            if action_type == "delete":
                old_value = action['value']['text']
            elif action_type == "add":
                new_value = action['value']['text']
            elif action_type == "edit":
                old_value = action['value']['text']
                new_value = action['value']['editedText']

            # Create a row for each action
            rows.append({
                "document_id": int(file_name),  
                "Attribute_name": attrib_name,
                "user_action": action_type,  
                "old_value": old_value,
                "new_value": new_value,
                "is_active": True  
            })

        # Store unchanged attributes
        for attrib in total_extracted_attrib:
            if attrib['id'] not in action_performed_attributes_ids:
                attrib_name = attrib['value']['labels'][0]
                old_value = attrib['value']['text']
                action_type = "no changes"
                rows.append({
                    "document_id": int(file_name),  
                    "Attribute_name": attrib_name,
                    "user_action": action_type,  
                    "old_value": old_value,
                    "new_value": None,
                    "is_active": True
                })
        
        # Add missing attributes
        missing_attributes = set(total_attributes) - {row['Attribute_name'] for row in rows}
        for attr in missing_attributes:
            empty_row = {
                "document_id": int(file_name), 
                "Attribute_name": attr,
                "user_action": "not extracted",
                "old_value": None,
                "new_value": None,
                "is_active": True 
            }
            rows.append(empty_row)

        # Create DataFrame at once
        df = pd.DataFrame(rows)

        # Optionally sort the DataFrame
        df.sort_values(by='Attribute_name', inplace=True)

        print(f"Actions performed on file {file_name}:\n{df}")
        return df, len(total_extracted_attrib)
    
    except s3.exceptions.NoSuchKey:
        print(f"Key does not exist: {key}/{file_name}.json")
    except Exception as e:
        print(f"Error fetching data: {e}")
    
    return None, None

def calculate_accuracy(total, sum_of_actions):
    return (1 - (sum_of_actions / total)) * 100 if total > 0 else 0

def processing_for_doc(s3, file, bucket_name, file_path_key, system_datetime):
    doc_id = file['document_id']
    print(f"\nProcessing file: {doc_id}")
    processed_date = file['processed_date']
    path = file["path"]

    df, attrib_counts = get_data_of_file(s3, bucket_name, file_path_key, doc_id)
    
    if df and attrib_counts is not None:
        added = len(df["add"])
        edited = len(user_action["edit"])
        deleted = len(user_action["delete"])
        
        record = {
            'document_id': doc_id,
            'attributes_extracted': attrib_counts,
            'attributes_added': added,
            'attributes_edited': edited,
            'attributes_deleted': deleted,
            's3_doc_reference': path,
            'processed_date': processed_date,
            'accuracy': calculate_accuracy(attrib_counts, sum([added, edited, deleted])),
            'user_actions': user_action.get('action_details', ''),
            'system_date': system_datetime
        }

        record_logs = {
            'document_id': doc_id,
            'user_action': ,
            'atribute_name' ,
            'old_value' ,
            'new_value' ,
            'is_active' ,
            'created_at' ,
            'updated_at' ,
            'action_performed_',
        }
        # Return as DataFrame
        return pd.DataFrame([record])  
    else:
        print("No data found for the file.")
        # Return an empty DataFrame
        return None  

def store_files_into_db(records_df, table_name, db_instance):
    """Insert a DataFrame into the specified table using SQLAlchemy."""
    if not records_df.empty: 
        try:
            # Use to_sql to insert the DataFrame into the database
            records_df.to_sql(table_name, db_instance.engine, if_exists='append', index=False)
            print("Data successfully added to the database.")
        except Exception as e:
            print(f"Error executing insert query: {e}")
    else:
        print("No records to insert.")

def main():
    load_dotenv()
    db_params = {
        'dbname': os.getenv("POSTGRE_NAME"),
        'user': os.getenv("POSTGRE_USER"),
        'password': os.getenv("POSTGRE_PASSWORD"),
        'host': os.getenv("POSTGRE_HOST"), 
        'port': os.getenv("POSTGRE_PORT")    
    }
    search_path_key = os.getenv("SEARCH_PATH")
    file_path_key = os.getenv("FILE_PATH")
    Bucket_name = os.getenv("BUCKET_NAME")
    table_name = os.getenv("TABLE_NAME")
    
    s3 = boto3.client("s3")
    db_instance = db.Database(db_params)
    
    try:
        if db_instance.connect():
            files_data = get_list_of_files(s3, bucket_name = Bucket_name, key = search_path_key)
            s3_df = pd.DataFrame.from_dict(files_data, orient='index').reset_index()
            s3_df.columns = ['document_id', 'processed_date', 'path']

            select_query = f"SELECT document_id FROM {os.getenv('TABLE_NAME')};"
            processed_files = db_instance.execute_query(query=select_query)
            processed_ids_set = set(doc[0] for doc in processed_files)

            files_to_process_df = s3_df[~s3_df['document_id'].isin(processed_ids_set)].reset_index(drop=True)
            print(f"Total files to process: {len(files_to_process_df)}\n")

            processing_for_system_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Process each file and accumulate results
            final_df = pd.DataFrame(columns=[
                                            'document_id', 
                                            'attributes_extracted',
                                            'attributes_added',
                                            'attributes_edited', 
                                            'attributes_deleted', 
                                            's3_doc_reference',
                                            'processed_date', 
                                            'accuracy',
                                            'user_actions',
                                            'system_date'
                                        ])

            for _, row in files_to_process_df.iterrows():
                result_df = processing_for_doc(s3, row, Bucket_name, file_path_key, processing_for_system_time)
                
                # Only add to final_df if result_df is not empty
                if result_df:
                    final_df = final_df.append(result_df, ignore_index=True)

            # Insert into database if final_df has records
            if not final_df.empty:
                print("Final DataFrame ready for insertion:\n", final_df)
                store_files_into_db(final_df.reset_index(drop=True), table_name, db_instance)
            else:
                print("Nothing to add into the database!")
                
        else:
            print("No valid records to add to the database.")

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        db_instance.close()  # Ensure the database connection is closed

if __name__ == "__main__":
    main()
