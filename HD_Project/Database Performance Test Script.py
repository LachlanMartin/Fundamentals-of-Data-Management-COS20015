import time
import statistics
import pymongo
import psycopg2
from psycopg2.extras import execute_batch

# MongoDB connection
def connect_mongodb():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client['testdb']

# PostgreSQL connection
def connect_postgresql():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return execution_time, result
    return wrapper

# 3.1 Data Insertion Tests
@measure_time
def test_single_insertion(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.insert_one({"test_field": "test_value"})
    else:
        cursor = db.cursor()
        cursor.execute("INSERT INTO test_table (test_field) VALUES (%s)", ("test_value",))
        db.commit()

@measure_time
def test_bulk_insertion(db, is_mongodb, num_records=10000):
    data = [{"test_field": f"test_value_{i}"} for i in range(num_records)]
    if is_mongodb:
        collection = db['testcollection']
        collection.insert_many(data)
    else:
        cursor = db.cursor()
        execute_batch(cursor, "INSERT INTO test_table (test_field) VALUES (%s)",
                      [(d['test_field'],) for d in data])
        db.commit()

# 3.2 Data Retrieval Tests
@measure_time
def test_single_retrieval(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        result = collection.find_one({"test_field": "test_value_1000"})
    else:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE test_field = %s", ("test_value_1000",))
        result = cursor.fetchone()

@measure_time
def test_multi_retrieval(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        result = list(collection.find({"test_field": {"$regex": "^test_value_"}}))
    else:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE test_field LIKE 'test_value_%'")
        result = cursor.fetchall()

# 3.3 Data Update Tests
@measure_time
def test_single_update(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.update_one({"test_field": "test_value_1000"}, {"$set": {"test_field": "updated_value"}})
    else:
        cursor = db.cursor()
        cursor.execute("UPDATE test_table SET test_field = %s WHERE test_field = %s",
                       ("updated_value", "test_value_1000"))
        db.commit()

@measure_time
def test_multi_update(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.update_many(
            {"test_field": {"$regex": "^test_value_"}},
            {"$set": {"updated": True}}
        )
    else:
        cursor = db.cursor()
        cursor.execute("UPDATE test_table SET updated = TRUE WHERE test_field LIKE 'test_value_%'")
        db.commit()

# 3.4 Data Deletion Tests
@measure_time
def test_single_deletion(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.delete_one({"test_field": "test_value_1000"})
    else:
        cursor = db.cursor()
        cursor.execute("DELETE FROM test_table WHERE test_field = %s", ("test_value_1000",))
        db.commit()

@measure_time
def test_multi_deletion(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.delete_many({"test_field": {"$regex": "^test_value_"}})
    else:
        cursor = db.cursor()
        cursor.execute("DELETE FROM test_table WHERE test_field LIKE 'test_value_%'")
        db.commit()

# 3.5 Indexing Performance Tests
@measure_time
def test_index_creation(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        collection.create_index("test_field")
    else:
        cursor = db.cursor()
        cursor.execute("""
            SELECT to_regclass('idx_test_field')
        """)
        if cursor.fetchone()[0] is None:  # Index does not exist
            cursor.execute("CREATE INDEX idx_test_field ON test_table (test_field)")
        db.commit()

@measure_time
def test_query_with_index(db, is_mongodb):
    if is_mongodb:
        collection = db['testcollection']
        result = list(collection.find({"test_field": "test_value_1000"}).hint("test_field_1"))
    else:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM test_table WHERE test_field = 'test_value_1000'")
        result = cursor.fetchall()

# New setup functions for join tests
def setup_mongodb_join_data(db):
    db['users'].drop()
    db['orders'].drop()
    
    users = [{"_id": i, "name": f"User{i}"} for i in range(1000)]
    orders = [{"user_id": i % 1000, "total": i * 10} for i in range(10000)]
    
    db['users'].insert_many(users)
    db['orders'].insert_many(orders)
    
    db['users'].create_index("_id")
    db['orders'].create_index("user_id")

def setup_postgresql_join_data(db):
    cursor = db.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS orders")
    cursor.execute("DROP TABLE IF EXISTS users")
    
    cursor.execute(""" 
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
    """)
    
    users = [(f"User{i}",) for i in range(1000)]
    execute_batch(cursor, "INSERT INTO users (name) VALUES (%s)", users)  # Insert users first

    cursor.execute(""" 
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            total NUMERIC(10, 2)
        )
    """)
    
    # Adjust user_id to start from 1 to match the users table
    orders = [(i % 1000 + 1, i * 10) for i in range(10000)]  # Change (i % 1000) to (i % 1000 + 1)
    execute_batch(cursor, "INSERT INTO orders (user_id, total) VALUES (%s, %s)", orders)
    
    cursor.execute("CREATE INDEX idx_orders_user_id ON orders (user_id)")
    
    db.commit()

# New join test functions
@measure_time
def test_mongodb_join(db):
    result = list(db['orders'].aggregate([
        {
            '$lookup': {
                'from': 'users',
                'localField': 'user_id',
                'foreignField': '_id',
                'as': 'user'
            }
        },
        {'$unwind': '$user'},
        {'$limit': 1000}
    ]))

@measure_time
def test_postgresql_join(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT o.id, o.total, u.name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        LIMIT 1000
    """)
    result = cursor.fetchall()

def run_test_suite(db, is_mongodb, num_records, num_runs):
    tests = [
        ("Single Insertion", lambda: test_single_insertion(db, is_mongodb)),
        ("Bulk Insertion", lambda: test_bulk_insertion(db, is_mongodb, num_records)),
        ("Single Retrieval", lambda: test_single_retrieval(db, is_mongodb)),
        ("Multi Retrieval", lambda: test_multi_retrieval(db, is_mongodb)),
        ("Single Update", lambda: test_single_update(db, is_mongodb)),
        ("Multi Update", lambda: test_multi_update(db, is_mongodb)),
        ("Single Deletion", lambda: test_single_deletion(db, is_mongodb)),
        ("Multi Deletion", lambda: test_multi_deletion(db, is_mongodb)),
        ("Index Creation", lambda: test_index_creation(db, is_mongodb)),
        ("Query with Index", lambda: test_query_with_index(db, is_mongodb)),
        ("Join Operation", lambda: test_mongodb_join(db) if is_mongodb else test_postgresql_join(db)),
    ]

    results = {}
    for test_name, test_func in tests:
        run_times = []
        for run in range(num_runs):
            time_taken, _ = test_func()
            run_times.append(time_taken)
            print(f"{test_name} - Run {run + 1}: {time_taken:.4f} seconds")

        avg_time = statistics.mean(run_times)
        std_dev = statistics.stdev(run_times) if len(run_times) > 1 else 0
        results[test_name] = (avg_time, std_dev)

        print(f"{test_name} - Average: {avg_time:.4f} seconds, StdDev: {std_dev:.4f} seconds\n")

    return results

def setup_postgresql(db):
    cursor = db.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id serial PRIMARY KEY,
            test_field varchar,
            updated boolean
        )
    """)
    db.commit()

def clear_data(db, is_mongodb):
    if is_mongodb:
        db['testcollection'].delete_many({})
        db['users'].delete_many({})
        db['orders'].delete_many({})
    else:
        cursor = db.cursor()
        # Delete from orders first to avoid foreign key violation
        cursor.execute("DELETE FROM orders")
        cursor.execute("DELETE FROM users")
        cursor.execute("DELETE FROM test_table")
        db.commit()

def main():
    NUM_RECORDS = 10000
    NUM_RUNS = 30

    print(f"Running tests with {NUM_RECORDS} records, {NUM_RUNS} times each.\n")

    mongo_db = connect_mongodb()
    postgres_db = connect_postgresql()

    setup_postgresql(postgres_db)
    setup_mongodb_join_data(mongo_db)
    setup_postgresql_join_data(postgres_db)

    clear_data(mongo_db, True)

    print("MongoDB Tests:")
    mongo_results = run_test_suite(mongo_db, True, NUM_RECORDS, NUM_RUNS)

    clear_data(postgres_db, False)

    print("\nPostgreSQL Tests:")
    postgres_results = run_test_suite(postgres_db, False, NUM_RECORDS, NUM_RUNS)

    print("\nComparison:")
    for test_name in mongo_results.keys():
        mongo_avg, mongo_std = mongo_results[test_name]
        postgres_avg, postgres_std = postgres_results[test_name]
        print(f"{test_name}:")
        print(f"  MongoDB:    Avg: {mongo_avg:.4f}s, StdDev: {mongo_std:.4f}s")
        print(f"  PostgreSQL: Avg: {postgres_avg:.4f}s, StdDev: {postgres_std:.4f}s")
        print()

    mongo_db.client.close()
    postgres_db.close()

if __name__ == "__main__":
    main()