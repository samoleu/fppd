import random
import numpy as np
import time
import csv
from multiprocessing import Pool

def read_csv_elements(file_path):
    """
    Reads elements from a CSV file and returns a flat list.
    """
    elements = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            elements.extend(row)
    return elements

def convert_to_integers(elements):
    """
    Converts a list of string elements in scientific notation to integers.
    """
    return [int(float(element)) for element in elements]

# Merge Sort - Serial version
def merge_sort_sequential(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort_sequential(arr[:mid])
    right = merge_sort_sequential(arr[mid:])
    return merge(left, right)

def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result

# Merge Sort - Parallel version
def merge_sort_parallel(arr, num_processes):
    length = len(arr)
    if length <= 1:
        return arr
    chunk_size = (length + num_processes - 1) // num_processes  # Ensure all elements are covered
    chunks = [
        arr[i * chunk_size: min((i + 1) * chunk_size, length)]
        for i in range(num_processes)
    ]
    
    # Step 1: Sort each chunk in parallel
    with Pool(num_processes) as pool:
        sorted_chunks = pool.map(merge_sort_sequential, chunks)
    
    # Step 2: Merge the sorted chunks
    while len(sorted_chunks) > 1:
        new_chunks = []
        for i in range(0, len(sorted_chunks), 2):
            if i + 1 < len(sorted_chunks):
                new_chunks.append(merge(sorted_chunks[i], sorted_chunks[i+1]))
            else:
                new_chunks.append(sorted_chunks[i])
        sorted_chunks = new_chunks
    
    return sorted_chunks[0]

if __name__ == "__main__":
    NUM_PROCESSES = 12  # Utilize múltiplos processos
    FILE_PATH = './dados/D.csv'

    # Read and preprocess data
    array = np.array(convert_to_integers(read_csv_elements(FILE_PATH)), dtype=np.int32)

    # Serial version
    start_time = time.perf_counter()
    sorted_sequential = merge_sort_sequential(array.copy())
    sequential_time = time.perf_counter() - start_time
    print(f"Serial execution time: {sequential_time:.6f} seconds")

    # Parallel version
    start_time = time.perf_counter()
    sorted_parallel = merge_sort_parallel(array.copy(), NUM_PROCESSES)
    parallel_time = time.perf_counter() - start_time
    print(f"Parallel execution time: {parallel_time:.6f} seconds")
    print(f"Speedup: {sequential_time / parallel_time:.2f}")

    # Consistency check
    assert np.array_equal(sorted_sequential, sorted_parallel), "Error: Parallel Merge Sort failed!"
    print("Consistency check passed: Both versions sorted correctly.")
