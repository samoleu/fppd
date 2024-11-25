from multiprocessing import Pool, cpu_count
import random
import time

def matrix_multiplication_serial(matrix_A, matrix_B):
    rows_A, cols_A = len(matrix_A), len(matrix_A[0])
    cols_B = len(matrix_B[0])
    result_matrix = [[0] * cols_B for _ in range(rows_A)]
    for i in range(rows_A):
        for j in range(cols_B):
            for k in range(cols_A):
                result_matrix[i][j] += matrix_A[i][k] * matrix_B[k][j]
    return result_matrix

def process_matrix_chunk(chunk):
    matrix_A_chunk, matrix_B_T, cols_B = chunk  # Use the transpose of B to avoid recomputation
    chunk_result = []
    for row in matrix_A_chunk:
        row_result = []
        for j in range(cols_B):
            row_result.append(sum(row[k] * matrix_B_T[j][k] for k in range(len(row))))
        chunk_result.append(row_result)
    return chunk_result

def matrix_multiplication_parallel(matrix_A, matrix_B, num_processes=None):
    rows_A = len(matrix_A)
    cols_B = len(matrix_B[0])

    # Transpose matrix B to avoid recomputation
    matrix_B_T = list(zip(*matrix_B))

    # Determine the number of processes to use (based on available cores)
    if num_processes is None:
        num_processes = cpu_count()

    chunk_size = rows_A // num_processes
    chunks = [(matrix_A[i * chunk_size:(i + 1) * chunk_size], matrix_B_T, cols_B)
              for i in range(num_processes)]

    with Pool(num_processes) as pool:
        chunk_results = pool.map(process_matrix_chunk, chunks)

    # Combine the results from the chunks
    result_matrix = [row for chunk in chunk_results for row in chunk]
    return result_matrix

if __name__ == "__main__":
    # Random matrix generation
    matrix_A = [[random.randint(1, 10) for _ in range(400)] for _ in range(200)]
    matrix_B = [[random.randint(1, 10) for _ in range(100)] for _ in range(400)]

    # Serial matrix multiplication
    start_time = time.perf_counter()
    serial_result = matrix_multiplication_serial(matrix_A, matrix_B)
    serial_time = time.perf_counter() - start_time
    print(f"Serial Time: {serial_time:.6f} seconds")

    # Parallel matrix multiplication
    num_processes = 4
    start_time = time.perf_counter()
    parallel_result = matrix_multiplication_parallel(matrix_A, matrix_B, num_processes)
    parallel_time = time.perf_counter() - start_time
    print(f"Parallel Time: {parallel_time:.6f} seconds")
    print(f"Speedup: {serial_time / parallel_time:.2f}")

    # Consistency check
    assert serial_result == parallel_result, "Error: Results do not match!"
