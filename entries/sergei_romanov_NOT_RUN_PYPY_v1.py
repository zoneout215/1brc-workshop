#!/usr/bin/python3

# This script uses
#  - Kahan summation for accurate floating-point addition
#  - Multiprocessing for parallelism
#  - Garbage collection control for performance
#  - Optimal blocksize calculation
#  - Array with double data type for memory efficiency 
import sys
import os
import multiprocessing as mp
from array import array
from functools import partial
from gc import disable as gc_disable, enable as gc_enable


def get_optimal_blocksize(file_name: str, cpu_count: int, file_size: int) -> int:
    """Calculate optimal blocksize using only built-in Python"""
    
    # Get filesystem block size (Unix-like systems)
    try:
        stat_result = os.statvfs(file_name)
        fs_blocksize = stat_result.f_bsize  # Typically 4KB
    except (AttributeError, OSError):
        fs_blocksize = 4096  # Default to 4KB
    
    chunk_size = file_size // cpu_count
    
    # Heuristic: blocksize should be ~1-2% of chunk size
    # But bounded between reasonable limits
    calculated_blocksize = chunk_size // 100
    
    blocksize = max(
        fs_blocksize * 256,      # Minimum: 1MB (256 * 4KB)
        min(
            calculated_blocksize,
            fs_blocksize * 16384  # Maximum: 64MB (16384 * 4KB)
        )
    )
    
    blocksize = (blocksize // (1024 * 1024)) * (1024 * 1024)
    return max(blocksize, 1 * 1024 * 1024)  # Ensure minimum 1MB


def get_file_chunks(
    file_name: str,
    max_cpu: int = 8,
) -> list[int, list[tuple[str, int, int]]]:
    """Split file into chunks"""
    cpu_count = min(max_cpu, mp.cpu_count())

    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = list()
    with open(file_name, mode="r+b") as f:

        def is_new_line(position: int) -> bool:
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == b"\n"

        def next_line(position: int):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while not is_new_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end)

            start_end.append(
                (
                    file_name,
                    chunk_start,
                    chunk_end,
                )
            )

            chunk_start = chunk_end

    return (
        cpu_count,
        start_end,
    )


def _process_file_chunk(
    file_name: str,
    chunk_start: int,
    chunk_end: int,
    blocksize: int = 1024 * 1024 * 1,
) -> dict:
    """Process each file chunk in a different process"""
    result = dict()

    with open(file_name, mode="r+b") as fh:
        fh.seek(chunk_start)
        gc_disable()

        tail = b""
        location = None
        byte_count = chunk_end - chunk_start
    
        while byte_count > 0:
            if blocksize > byte_count:
                blocksize = byte_count
            byte_count -= blocksize

            index = 0
            data = tail + fh.read(blocksize)
            while data:
                if location is None:
                    try:
                        semicolon_indx: int = data.index(b";", index)
                    except ValueError:
                        tail = data[index:]
                        break

                    location = data[index:semicolon_indx]
                    index = semicolon_indx + 1

                try:
                    newline = data.index(b"\n", index)
                except ValueError:
                    tail = data[index:]
                    break

                temperature = float(data[index:newline])
                index = newline + 1
                
                try:
                    stats = result[location]
                    if temperature < stats[0]:
                        stats[0] = temperature
                    if temperature > stats[1]:
                        stats[1] = temperature
                    
                    # Kahan summation
                    y = temperature - stats[4]
                    t = stats[2] + y
                    stats[4] = (t - stats[2]) - y
                    stats[2] = t
                    stats[3] += 1
                except KeyError:
                    # array('d', [min, max, sum, count, compensation])
                    result[location] = array('d', [temperature, temperature, temperature, 1, 0.0])

                location = None
        gc_enable()
    return result


def process_file(
    cpu_count: int,
    start_end: list,
    file: str
) -> dict:
    """Process data file"""

    opt_size = get_optimal_blocksize(file, cpu_count, os.path.getsize(file))
    partial_chunk_process = partial(_process_file_chunk, blocksize = opt_size)
    with mp.Pool(cpu_count) as p:
        # Run chunks in parallel
        chunk_results = p.starmap(
            partial_chunk_process,
            start_end,
        )

    # Combine all results from all chunks with Kahan summation
    result = dict()
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            if location not in result:
                result[location] = measurements
            else:
                stats = result[location]
                if measurements[0] < stats[0]:
                    stats[0] = measurements[0]
                if measurements[1] > stats[1]:
                    stats[1] = measurements[1]
                
                # Kahan summation when merging
                y = measurements[2] - stats[4]
                t = stats[2] + y
                stats[4] = (t - stats[2]) - y
                stats[2] = t
                stats[3] += measurements[3]

    # Print final results
    for location, measurements in sorted(result.items()):
        if measurements[3] > 0:
            avg_temp = round(measurements[2] / measurements[3], 1)
        else:
            avg_temp = 0.0
        print(
            f"{location.decode('utf-8')}={measurements[0]:.1f}/{avg_temp:.1f}/{measurements[1]:.1f}",
            end="\n",
        )

if __name__ == "__main__":
    cpu_count, *start_end = get_file_chunks(sys.argv[1])
    process_file(cpu_count, start_end[0], sys.argv[1])
    # cProfile.run('process_file(cpu_count, start_end[0], sys.argv[1])')