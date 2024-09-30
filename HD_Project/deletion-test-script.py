import time
import statistics
import pymongo
import psycopg2

def test_mongodb_deletion(client):
    db = client["testdb"]
    collection = db["testcollection"]
    
    start_time = time.time()
    collection.delete_many({"test_field": {"$regex": "^test_value_"}})
    end_time = time.time()
    
    return end_time - start_time

def test_postgresql_deletion(conn):
    cur = conn.cursor()
    
    start_time = time.time()
    cur.execute("DELETE FROM test_table WHERE test_field LIKE 'test_value_%'")
    conn.commit()
    end_time = time.time()
    
    cur.close()
    return end_time - start_time

def run_deletion_tests(num_records, num_runs):
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    postgres_conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )

    mongo_times = []
    postgres_times = []

    for _ in range(num_runs):
        # Insert test data
        mongo_client["testdb"]["testcollection"].insert_many([{"test_field": f"test_value_{i}"} for i in range(num_records)])
        cur = postgres_conn.cursor()
        cur.executemany("INSERT INTO test_table (test_field) VALUES (%s)", [(f"test_value_{i}",) for i in range(num_records)])
        postgres_conn.commit()
        cur.close()

        # Run deletion tests
        mongo_times.append(test_mongodb_deletion(mongo_client))
        postgres_times.append(test_postgresql_deletion(postgres_conn))

    mongo_client.close()
    postgres_conn.close()

    return {
        "MongoDB": {
            "avg": statistics.mean(mongo_times),
            "std": statistics.stdev(mongo_times) if len(mongo_times) > 1 else 0
        },
        "PostgreSQL": {
            "avg": statistics.mean(postgres_times),
            "std": statistics.stdev(postgres_times) if len(postgres_times) > 1 else 0
        }
    }

if __name__ == "__main__":
    NUM_RECORDS = 10000
    NUM_RUNS = 5
    
    print(f"Running deletion tests with {NUM_RECORDS} records, {NUM_RUNS} runs.\n")
    
    results = run_deletion_tests(NUM_RECORDS, NUM_RUNS)
    
    print("Deletion Test Results:")
    print(f"MongoDB:    Avg: {results['MongoDB']['avg']:.4f}s, StdDev: {results['MongoDB']['std']:.4f}s")
    print(f"PostgreSQL: Avg: {results['PostgreSQL']['avg']:.4f}s, StdDev: {results['PostgreSQL']['std']:.4f}s")
