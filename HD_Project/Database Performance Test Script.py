import time
import statistics
import pymongo
import psycopg2
from psycopg2 import sql

def test_mongodb(num_records, num_runs):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["testdb"]
    collection = db["testcollection"]
    
    run_times = []
    for run in range(num_runs):
        start_time = time.time()
        for i in range(num_records):
            collection.insert_one({"test_field": f"test_value_{i}"})
        end_time = time.time()
        run_time = end_time - start_time
        run_times.append(run_time)
        print(f"MongoDB Run {run + 1} insert time: {run_time:.4f} seconds")
        
        # Clear the collection for the next run
        collection.delete_many({})
    
    avg_time = statistics.mean(run_times)
    std_dev = statistics.stdev(run_times) if len(run_times) > 1 else 0
    
    print(f"\nMongoDB Average insert time: {avg_time:.4f} seconds")
    print(f"MongoDB Standard Deviation: {std_dev:.4f} seconds")
    
    client.close()
    return avg_time, std_dev

def test_postgresql(num_records, num_runs):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, test_field varchar);")
    
    run_times = []
    for run in range(num_runs):
        start_time = time.time()
        for i in range(num_records):
            cur.execute(sql.SQL("INSERT INTO test_table (test_field) VALUES (%s)"), [f"test_value_{i}"])
        conn.commit()
        end_time = time.time()
        run_time = end_time - start_time
        run_times.append(run_time)
        print(f"PostgreSQL Run {run + 1} insert time: {run_time:.4f} seconds")
        
        # Clear the table for the next run
        cur.execute("DELETE FROM test_table")
        conn.commit()
    
    avg_time = statistics.mean(run_times)
    std_dev = statistics.stdev(run_times) if len(run_times) > 1 else 0
    
    print(f"\nPostgreSQL Average insert time: {avg_time:.4f} seconds")
    print(f"PostgreSQL Standard Deviation: {std_dev:.4f} seconds")
    
    cur.close()
    conn.close()
    return avg_time, std_dev

if __name__ == "__main__":
    NUM_RECORDS = 10000
    NUM_RUNS = 5
    
    print(f"Running tests with {NUM_RECORDS} records, {NUM_RUNS} times each.\n")
    
    mongo_avg, mongo_std = test_mongodb(NUM_RECORDS, NUM_RUNS)
    print("\n" + "="*50 + "\n")
    postgres_avg, postgres_std = test_postgresql(NUM_RECORDS, NUM_RUNS)
    
    print("\nComparison:")
    print(f"MongoDB:    Avg: {mongo_avg:.4f}s, StdDev: {mongo_std:.4f}s")
    print(f"PostgreSQL: Avg: {postgres_avg:.4f}s, StdDev: {postgres_std:.4f}s")