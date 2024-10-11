import csv
from elasticsearch import Elasticsearch, helpers

# Initialize Elasticsearch connection
def connect_elasticsearch():
    try:
        es = Elasticsearch(["http://localhost:9200"], sniff_on_start=True, sniff_on_node_failure=True, min_delay_between_sniffing=60)
        
        if es.ping():
            print("Successfully connected to Elasticsearch!")
            return es
        else:
            print("Failed to connect to Elasticsearch.")
            return None
    except Exception as e:
        print(f"An error occurred during connection: {e}")
        return None

es = connect_elasticsearch()

def createCollection(p_collection_name):
    if es and not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Index '{p_collection_name}' created.")
    else:
        print(f"Index '{p_collection_name}' already exists or connection failed.")

def indexDataFromCSV(p_collection_name, csv_file_path, p_exclude_column):
    if es:
        actions = []
        with open(csv_file_path, mode='r', encoding='ISO-8859-1') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                # Normalize the row keys by stripping whitespace
                row = {key.strip(): value for key, value in row.items()}
                doc = {key: value for key, value in row.items() if key != p_exclude_column}
                action = {
                    "_index": p_collection_name,
                    "_id": row.get("Employee ID"),  
                    "_source": doc
                }
                actions.append(action)
        try:
            helpers.bulk(es, actions)
            print(f"Data indexed into '{p_collection_name}', excluding column '{p_exclude_column}'.")
        except helpers.BulkIndexError as bulk_error:
            print(f"Bulk indexing failed: {bulk_error.errors}")
            for error in bulk_error.errors:
                print(f"Failed document ID: {error['index']['_id'] if 'index' in error else 'Unknown ID'}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    if es:
        body = {
            "query": {
                "match": {
                    p_column_name: p_column_value
                }
            }
        }
        res = es.search(index=p_collection_name, body=body)
        return res['hits']['hits']
    return []

def getEmpCount(p_collection_name):
    if es:
        res = es.count(index=p_collection_name)
        print(f"Employee count in '{p_collection_name}': {res['count']}")
        return res['count']
    return 0

def delEmpById(p_collection_name, p_employee_id):
    if es:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Deleted employee with ID '{p_employee_id}' from collection '{p_collection_name}'.")

def getDepFacet(p_collection_name):
    if es:
        body = {
            "size": 0,
            "aggs": {
                "by_department": {
                    "terms": {
                        "field": "Department.keyword"
                    }
                }
            }
        }
        res = es.search(index=p_collection_name, body=body)
        buckets = res['aggregations']['by_department']['buckets']
        for bucket in buckets:
            print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")
        return buckets
    return []

# Function Executions
v_nameCollection = 'hash_shiran'  
v_phoneCollection = 'hash_2279'  

# CSV file path
csv_file_path = '/Users/shiren/Documents/Placement_Drives/Hash Agile/Employee_Sample_Data_1.csv'

# Ensure Elasticsearch connection is successful
if es:
    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)

    # Get employee count before indexing
    getEmpCount(v_nameCollection)

    # Index data from CSV excluding specific columns
    indexDataFromCSV(v_nameCollection, csv_file_path, 'Department')
    indexDataFromCSV(v_phoneCollection, csv_file_path, 'Gender')

    # Delete an employee by ID
    delEmpById(v_nameCollection, 'E02003')

    # Get employee count after deletion
    getEmpCount(v_nameCollection)

    # Search by column
    print("Search results by Department (IT):")
    print(searchByColumn(v_nameCollection, 'Department', 'IT'))

    print("Search results by Gender (Male):")
    print(searchByColumn(v_nameCollection, 'Gender', 'Male'))

    print("Search results by Department (IT) in phone collection:")
    print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

    # Department facets (aggregations)
    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)
