import multiprocessing
import csv
import time

def load_numbers_from_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        numbers = [int(float(row[0])) for row in reader]
    return numbers

def calculate_prefix_sum_chunk(numbers, start_index, end_index, result_queue):
    prefix_sum_chunk = []
    for i in range(start_index, end_index):
        if i == 0:
            prefix_sum_chunk.append(numbers[0])
        else:
            prefix_sum_chunk.append(numbers[i-1] + numbers[i])
    result_queue.put((start_index, prefix_sum_chunk))

def compute_parallel_prefix_sum(numbers, num_processes):
    chunk_size = len(numbers) // num_processes
    processes = []
    result_queue = multiprocessing.Queue()

    for i in range(num_processes):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i < num_processes - 1 else len(numbers)
        process = multiprocessing.Process(
            target=calculate_prefix_sum_chunk,
            args=(numbers, start_index, end_index, result_queue)
        )
        processes.append(process)
        process.start()

    result = [0] * len(numbers)
    for _ in processes:
        start_index, prefix_sum_chunk = result_queue.get()
        for i, value in enumerate(prefix_sum_chunk):
            result[start_index + i] = value

    for process in processes:
        process.join()

    return result

if __name__ == "__main__":
    csv_file = "./dados/C.csv"

    num_processes = 12

    numbers = load_numbers_from_csv(csv_file)
    print(f"Total de números carregados: {len(numbers)}")

    print("Executando soma de prefixos serial...")
    start_time = time.time()
    serial_result = compute_parallel_prefix_sum(numbers, 1)
    end_time = time.time()

    serial_time = end_time - start_time
    print(f"Soma de Prefixos Serial completa. Tempo: {serial_time:.4f} segundos")

    print("Executando soma de prefixos paralela...")
    start_time = time.time()
    parallel_result = compute_parallel_prefix_sum(numbers, num_processes)
    end_time = time.time()

    parallel_time = end_time - start_time
    print(f"Soma de Prefixos Paralela completa. Tempo: {parallel_time:.4f} segundos")

    # Calcular o speedup
    speedup = serial_time / parallel_time
    print(f"Speedup: {speedup:.2f}")

    # Verificar resultados
    if serial_result == parallel_result:
        print("Os resultados coincidem!")
    else:
        print("Os resultados NÃO coincidem! Verifique a implementação.")
