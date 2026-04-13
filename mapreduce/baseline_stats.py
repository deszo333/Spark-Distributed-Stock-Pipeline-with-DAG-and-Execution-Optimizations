# mapreduce/baseline_stats.py
import csv
import sys
from collections import defaultdict
import multiprocessing

def mapper(chunk):
    """
    MAP STAGE: Takes a chunk of rows and extracts (Symbol, Volume).
    Ignores the header row.
    """
    mapped_data = []
    for row in chunk:
        try:
            symbol = row[0]
            # Ensure it's not the header and volume exists
            if symbol != "Ticker" and row[7]:
                volume = int(row[7])
                mapped_data.append((symbol, volume))
        except (ValueError, IndexError):
            continue
    return mapped_data

def reducer(mapped_data):
    """
    REDUCE STAGE: Groups by Symbol and calculates Average Volume.
    """
    # The 'Shuffle' phase is handled by grouping the data into a dictionary
    grouped_data = defaultdict(list)
    for symbol, volume in mapped_data:
        grouped_data[symbol].append(volume)

    # The 'Reduce' phase aggregates the grouped data
    results = {}
    for symbol, volumes in grouped_data.items():
        avg_vol = sum(volumes) / len(volumes)
        results[symbol] = round(avg_vol, 2)
    
    return results

def run_mapreduce():
    file_path = "data/raw/SP500_Historical_Data.csv"
    print(" Starting MapReduce Baseline Job...")

    # 1. Read the data
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        data = list(reader)

    # 2. Split data into chunks for parallel processing (simulating HDFS blocks)
    num_cores = multiprocessing.cpu_count()
    chunk_size = len(data) // num_cores
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    print(f" Splitting data across {num_cores} CPU cores for Map phase...")

    # 3. Execute MAP in parallel
    with multiprocessing.Pool(processes=num_cores) as pool:
        mapped_chunks = pool.map(mapper, chunks)

    # Flatten the mapped results
    flat_mapped_data = [item for sublist in mapped_chunks for item in sublist]

    print("Executing Reduce phase...")

    # 4. Execute REDUCE
    final_results = reducer(flat_mapped_data)

    # 5. Output top 5 results to prove it worked
    print("MapReduce Complete! Here are the Baseline Average Volumes:")
    top_5 = list(final_results.items())[:5]
    for symbol, avg_vol in top_5:
        print(f"   Symbol: {symbol} | Average Volume: {avg_vol:,}")

if __name__ == "__main__":
    run_mapreduce()