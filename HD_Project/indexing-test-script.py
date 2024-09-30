import time
import statistics
import pymongo
import psycopg2

def test_mongodb_indexing(client):
    db = client["testdb"]
    collection = db["testcollection"]
    
    start_time = time.time()
    collection.create_index("test_field")
    end_time = time.time()
    
    return end_time - start_time

def test_postgresql_indexing(conn):
    cur = conn.cursor()
    
    start_time = time.time()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test_field ON test_table (test_field)")
    conn.commit()
    end_time = time.time()
    
    cur.close()
    return end_time - start_time

def run_indexing_tests(num_runs):
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
        # Drop existing indexes
        mongo_client["testdb"]["testcollection"].drop_indexes()
        cur = postgres_conn.cursor()
        cur.execute("DROP INDEX IF EXISTS idx_test_field")
        postgres_conn.commit()
        cur.close()

        # Run indexing tests
        mongo_times.append(test_mongodb_indexing(mongo_client))
        postgres_times.append(test_postgresql_indexing(postgres_conn))

    mongo_client.close()
    postgres_conn.close()

    return {
        "MongoDB": {
            "avg": statistics.mean(mongo_times),
            "std": statistics.stdev(mongo_times) if len(mongo_times) > 1 else 0
        },
        "PostgreSQL": {
            "avg": statistics.mean(postgres_times),
            "std": statistics.stdev(postgres_times) if len(mongo_times) > 1 else 0
        }
    }

if __name__ == "__main__":
    NUM_RUNS = 5
    
    print(f"Running indexing tests with {NUM_RUNS} runs.\n")
    
    results = run_indexing_tests(NUM_RUNS)
    
    print("Indexing Test Results:")
    print(f"MongoDB:    Avg: {results['MongoDB']['avg']:.4f}s, StdDev: {results['MongoDB']['std']:.4f}s")
    print(f"PostgreSQL: Avg: {results['PostgreSQL']['avg']:.4f}s, StdDev: {results['PostgreSQL']['std']:.4f}s")
