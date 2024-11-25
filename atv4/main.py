import concurrent.futures
import time
import random

def sequential_prefix_sum(input_list):
    length = len(input_list)
    prefix_sum = [0] * length
    prefix_sum[0] = input_list[0]
    for i in range(1, length):
        prefix_sum[i] = input_list[i - 1] + input_list[i]
    return prefix_sum

#===============================================================================

def parallel_prefix_sum(input_list):
    length = len(input_list)
    prefix_sum = [0] * length
    prefix_sum[0] = input_list[0]
    
    def compute_sum_segment(start_index, end_index):
        for i in range(start_index, end_index):
            if i == 0:
                prefix_sum[i] = input_list[i]
            else:
                prefix_sum[i] = input_list[i - 1] + input_list[i]

    num_threads = 4
    chunk_size = length // num_threads
    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(num_threads):
            start_index = i * chunk_size
            end_index = (i + 1) * chunk_size if i < num_threads - 1 else length
            futures.append(executor.submit(compute_sum_segment, start_index, end_index))
        concurrent.futures.wait(futures)
    return prefix_sum

def main():
    
    list_size = 50000

    input_list = [random.randint(1, 100) for _ in range(list_size)]

    start_time = time.perf_counter()
    sequential_result = sequential_prefix_sum(input_list)
    sequential_time = time.perf_counter() - start_time

    start_time = time.perf_counter()
    parallel_result = parallel_prefix_sum(input_list)
    parallel_time = time.perf_counter() - start_time

    if sequential_result != parallel_result:
        print("Error: The sequential and parallel results are different.")
    else:
        print("The sequential and parallel results are the same.")

    speedup = sequential_time / parallel_time

    print(f"Sequential time: {sequential_time:.6f} seconds")
    print(f"Parallel time: {parallel_time:.6f} seconds")
    print(f"Speedup: {speedup:.2f}")

if __name__ == "__main__":
    main()
