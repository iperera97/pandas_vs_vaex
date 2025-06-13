import time
import psutil
import functools
import os

def profile_resources(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())

        # Capture initial CPU times and memory info
        cpu_start = process.cpu_times()
        mem_start = process.memory_info()

        time_start = time.perf_counter()
        result = func(*args, **kwargs)
        time_end = time.perf_counter()

        # Capture CPU and memory after function execution
        cpu_end = process.cpu_times()
        mem_end = process.memory_info()

        # Calculate CPU time used by this function call (user + system time)
        cpu_time_used = (cpu_end.user - cpu_start.user) + (cpu_end.system - cpu_start.system)
        # Calculate memory used in bytes (rss difference)
        mem_used = mem_end.rss - mem_start.rss

        print(f"Function `{func.__name__}` execution time: {time_end - time_start:.4f} seconds")
        print(f"Function `{func.__name__}` CPU time used: {cpu_time_used:.4f} seconds")
        print(f"Function `{func.__name__}` Memory usage increase: {mem_used / (1024 ** 2):.4f} MB")

        return result
    return wrapper
