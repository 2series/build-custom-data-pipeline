import dlt

data = [
    {"id": 1, "name": "Alice", "surname": "Smith", "age": 25, "city": "New York", "country": "USA", "email": "alice@example.com"}, 
    {"id": 2, "name": "Bob", "surname": "Johnson", "age": 30, "city": "Los Angeles", "country": "USA", "email": "bob@example.com"},
    {"id": 3, "name": "Charlie", "surname": "Williams", "age": 35, "city": "Chicago", "country": "USA", "email": "charlie@example.com"},
    {"id": 4, "name": "David", "surname": "Brown", "age": 40, "city": "Houston", "country": "USA", "email": "david@example.com"},
    {"id": 5, "name": "Eve", "surname": "Jones", "age": 45, "city": "Phoenix", "country": "USA", "email": "eve@example.com"},
    {"id": 6, "name": "Frank", "surname": "Garcia", "age": 50, "city": "Philadelphia", "country": "USA", "email": "frank@example.com"},
    {"id": 7, "name": "Grace", "surname": "Martinez", "age": 55, "city": "San Antonio", "country": "USA", "email": "grace@example.com"},
    {"id": 8, "name": "Hank", "surname": "Lopez", "age": 60, "city": "San Francisco", "country": "USA", "email": "hank@example.com"},
    {"id": 9, "name": "Ivy", "surname": "Hernandez", "age": 65, "city": "San Diego", "country": "USA", "email": "ivy@example.com"},
    {"id": 10, "name": "Jack", "surname": "Gonzalez", "age": 70, "city": "Dallas", "country": "USA", "email": "jack@example.com"}
    ]

pipeline = dlt.pipeline(
    pipeline_name="quick_start", 
    destination="duckdb", 
    dataset_name="mydata"
)
load_info = pipeline.run(data, table_name="users")

print(load_info)