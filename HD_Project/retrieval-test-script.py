import time
import statistics
import pymongo
import psycopg2

def test_mongodb_retrieval(client):
    db = client["testdb"]
    collection = db["testcollection"]
    
    start_time = time.time()
    result = collection.find_one({"test_field": "test_value_1000"})
    end_time = time.time()
    
    return end_time - start_time

def test_postgresql_retrieval(conn):
    cur = conn.cursor()
    
    start_time = time.time()
    cur.execute("SELECT * FROM test_table WHERE test_field = %s", ("test_value_1000",))
    result = cur.fetchone()
    end_time = time.time()
    
    cur.close()
    return end_time - start_time

def run_retrieval_tests(num_runs):
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    postgres_conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )

    # Ensure test data exists
    mongo_client["testdb"]["testcollection"].insert_one({"test_field": "test_value_1000"})
    cur = postgres_conn.cursor()
    cur.execute("INSERT INTO test_table (test_field) VALUES (%s) ON CONFLICT DO NOTHING", ("test_value_1000",))
    postgres_conn.commit()
    cur.close()

    mongo_times = []
    postgres_times = []

    for _ in range(num_runs):
        mongo_times.append(test_mongodb_retrieval(mongo_client))
        postgres_times.append(test_postgresql_retrieval(postgres_conn))

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
    NUM_RUNS = 1000
    
    print(f"Running retrieval tests with {NUM_RUNS} runs.\n")
    
    results = run_retrieval_tests(NUM_RUNS)
    
    print("Retrieval Test Results:")
    print(f"MongoDB:    Avg: {results['MongoDB']['avg']:.4f}s, StdDev: {results['MongoDB']['std']:.4f}s")
    print(f"PostgreSQL: Avg: {results['PostgreSQL']['avg']:.4f}s, StdDev: {results['PostgreSQL']['std']:.4f}s")
