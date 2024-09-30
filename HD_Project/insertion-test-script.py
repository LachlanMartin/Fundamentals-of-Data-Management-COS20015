import time
import statistics
import pymongo
import psycopg2

def test_mongodb_insertion(client, num_records):
    db = client["testdb"]
    collection = db["testcollection"]
    
    start_time = time.time()
    for i in range(num_records):
        collection.insert_one({"test_field": f"test_value_{i}"})
    end_time = time.time()
    
    return end_time - start_time

def test_postgresql_insertion(conn, num_records):
    cur = conn.cursor()
    
    start_time = time.time()
    for i in range(num_records):
        cur.execute("INSERT INTO test_table (test_field) VALUES (%s)", (f"test_value_{i}",))
    conn.commit()
    end_time = time.time()
    
    cur.close()
    return end_time - start_time

def run_insertion_tests(num_records, num_runs):
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    postgres_conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )

    # Ensure the PostgreSQL table exists
    cur = postgres_conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, test_field VARCHAR(255))")
    postgres_conn.commit()
    cur.close()

    mongo_times = []
    postgres_times = []

    for _ in range(num_runs):
        mongo_times.append(test_mongodb_insertion(mongo_client, num_records))
        postgres_times.append(test_postgresql_insertion(postgres_conn, num_records))

        # Clear data after each run
        mongo_client["testdb"]["testcollection"].delete_many({})
        cur = postgres_conn.cursor()
        cur.execute("DELETE FROM test_table")
        postgres_conn.commit()
        cur.close()

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
    
    print(f"Running insertion tests with {NUM_RECORDS} records, {NUM_RUNS} runs.\n")
    
    results = run_insertion_tests(NUM_RECORDS, NUM_RUNS)
    
    print("Insertion Test Results:")
    print(f"MongoDB:    Avg: {results['MongoDB']['avg']:.4f}s, StdDev: {results['MongoDB']['std']:.4f}s")
    print(f"PostgreSQL: Avg: {results['PostgreSQL']['avg']:.4f}s, StdDev: {results['PostgreSQL']['std']:.4f}s")
