#!/usr/bin/python3

# This script uses
#  - MapReduce,
#  - Welford for AVG computation,
#  - Multiprocessing for pararlleslism
#  - rstrip for line parsing 

import sys
import os
import multiprocessing as mp
from functools import reduce

def get_file_chunks(file_name: str, max_cpu: int = 8) -> tuple[int, list[tuple[str, int, int]]]:
    """
    Split file into chunks for parallel processing, we need as many number of chunks as cpu available
    """
    cpu_count = min(max_cpu, mp.cpu_count())
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count
    
    chunks = []
    with open(file_name, mode="rb") as f:
        def is_new_line(position: int) -> bool:
            if position == 0:
                return True
            f.seek(position - 1)
            return f.read(1) == b"\n"
        
        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            
            # Adjust to line boundary
            while not is_new_line(chunk_end) and chunk_end < file_size:
                chunk_end += 1
            
            if chunk_start < chunk_end:
                chunks.append((file_name, chunk_start, chunk_end))
            
            chunk_start = chunk_end
    
    return cpu_count, chunks


def map_chunk(args: tuple[str, int, int]) -> dict:
    """MAP: Process a chunk and return local statistics"""
    file_name, start, end = args
    result = {}
    
    with open(file_name, mode="rb") as f:
        f.seek(start)
        
        while f.tell() < end:
            line = f.readline()
            if not line:
                break
            
            location, temp = line.rstrip(b"\n").split(b";")
            temperature = int(float(temp) * 10)
            
            if location in result:
                stats = result[location]
                if stats[0] > temperature:
                    stats[0] = temperature
                if stats[1] < temperature:
                    stats[1] = temperature
                
                stats[2] += temperature  # sum
                stats[3] += 1  # count
            else:
                result[location] = [temperature, temperature, temperature, 1]
    
    return result


def reduce_results(result1: dict, result2: dict) -> dict:
    """REDUCE: Combine two result dictionaries"""
    for location, stats2 in result2.items():
        if location in result1:
            stats1 = result1[location]
            if stats1[0] > stats2[0]:
                stats1[0] = stats2[0]
            if stats1[1] < stats2[1]:
                stats1[1] = stats2[1]
            stats1[2] += stats2[2]  # sum
            stats1[3] += stats2[3]  # count
        else:
            result1[location] = stats2
    
    return result1

 
def main(file_name: str):
    """Main processing function using map-reduce"""
    cpu_count, chunks = get_file_chunks(file_name)
    
    # MAP phase: Process chunks in parallel
    with mp.Pool(cpu_count) as pool:
        chunk_results = pool.map(map_chunk, chunks)
    
    # REDUCE phase: Combine all results
    final_result = reduce(reduce_results, chunk_results, {})
    
    # Format and print output
    for location, stats in sorted(final_result.items()):
        if stats[3] > 0:
            avg_temp = (stats[2]/stats[3])
        else:
            avg_temp = 0
        print(f"{location.decode('utf-8')}={stats[0]/10:.1f}/{avg_temp/10:.1f}/{stats[1]/10:.1f}", end="\n")

if __name__ == "__main__":
    main(sys.argv[1])