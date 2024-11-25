import csv
import time
import numpy as np
import random
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.shared_memory import SharedMemory

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

def search(array, value):
    """
    Performs a serial search for a value in the array.
    """
    result = None
    for i in range(len(array)):
        if array[i] == value:
            result = i
    return result

def search_chunk(shared_name, length, value, start, chunk_size):
    """
    Searches for a value in a chunk of the shared array.
    Returns the global index if found, else None.
    """
    shm = SharedMemory(name=shared_name)
    array = np.ndarray((length,), dtype=np.int32, buffer=shm.buf)

    end = min(start + chunk_size, length)
    result = None
    for i in range(start, end):
        if array[i] == value:
            result = i  # Global index
    return result


def search_mp(array, value, num_processes):
    """
    Distributes the search across multiple processes using shared memory.
    """
    length = len(array)
    chunk_size = (length + num_processes - 1) // num_processes  # Ensure all elements are covered

    # Create shared memory
    shm = SharedMemory(create=True, size=array.nbytes)
    shared_array = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
    shared_array[:] = array[:]

    result = None
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [
            executor.submit(search_chunk, shm.name, length, value, i * chunk_size, chunk_size)
            for i in range(num_processes)
        ]

        for future in futures:
            result = future.result()
            if result is not None:
                break

    # Clean up shared memory
    shm.close()
    shm.unlink()

    return result


if __name__ == "__main__":
    # Parameters
    NUM_PROCESSES = 12  # Utilize múltiplos processos
    FILE_PATH = './dados/C.csv'

    # Read and preprocess data
    array = np.array(convert_to_integers(read_csv_elements(FILE_PATH)), dtype=np.int32)
    SEARCHED_VALUE = array[random.randint(0, len(array)-1)]
    print(SEARCHED_VALUE)

    # Serial search
    start_time = time.perf_counter()
    index_serial = search(array, SEARCHED_VALUE)
    serial_time = time.perf_counter() - start_time
    print(f"Serial result: \n\tindex: {index_serial}\n\tvalue: {array[index_serial]}\n\ttime: {serial_time:.6f} seconds")

    # Parallel search
    start_time = time.perf_counter()
    index_parallel = search_mp(array, SEARCHED_VALUE, NUM_PROCESSES)
    parallel_time = time.perf_counter() - start_time
    print(f"Parallel result: \n\tindex: {index_parallel}\n\tvalue: {array[index_parallel]}\n\ttime: {parallel_time:.6f} seconds")

    # Speedup
    if index_parallel is not None:
        print(f"Speedup: {serial_time / parallel_time:.2f}")
    else:
        print("Value not found in both methods.")
