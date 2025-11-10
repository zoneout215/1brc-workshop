import itertools as it
import pathlib
import subprocess
from collections import defaultdict
from timeit import default_timer as timer
import polars as pl
import sys


WITH_VERIFICATION = True

def get_entries(sys_args) -> tuple[str, list[pathlib.Path]]:
    def print_entries(path: pathlib.Path):
        for file in sorted(path.glob("*.py")):
            print(f" - {file}")

    if len(sys_args) == 1:
        print("No measurement file specified")
        print("Usage:  leaderboard.sh <measurement file> [<entry1.py> <entry2.py> ...] or <directory>")
        exit()

    measurement_file = sys_args[1]

    if len(sys_args) == 2:
        path = pathlib.Path("entries/")
        print(f"No entries specified, running all entries in {path} folder:")
        print_entries(path)
        return measurement_file, sorted(path.glob("*.py"))

    path = pathlib.Path(sys_args[2])
    if path.is_dir():
        print(f"Directory {path} specified, running all entries in that folder:")
        print_entries(path)
        return measurement_file, sorted(path.glob("*.py"))

    print(f"Specific entries specified: {sys_args[2:]}")
    return measurement_file, [pathlib.Path(entry) for entry in sys_args[2:]]

def make_ground_truth(txt_file):
    print("\nGenerating ground truths for verification...")
    df = pl.scan_csv(
        txt_file,
        separator=";",
        has_header=False,
        with_column_names=lambda _: ["station_name", "measurement"],
    )

    grouped = (
        df.with_columns((pl.col("measurement") * 10).cast(pl.Int32).alias("measurement"))
        .group_by("station_name")
        .agg(
            pl.min("measurement").alias("min_measurement"),
            pl.mean("measurement").alias("mean_measurement"),
            pl.max("measurement").alias("max_measurement"),
        )
        .sort("station_name")
        .collect(streaming=True)
    )
    result = []
    for data in grouped.iter_rows():
        result.append(f"{data[0]}={data[1]/10:.1f}/{data[2]/10:.1f}/{data[3]/10:.1f}")
    return result

def main(measurement_file, entries, ground_truth, python_executable="python") -> dict[pathlib.Path, list[float]]:
    def compare(ground_truth, result: list[str]):
        if len(ground_truth) != len(result):
            yield f"Length mismatch: expected {len(ground_truth)} lines, got {len(result)} lines"

        for l, r in zip(ground_truth, result):
            if l != r:
                yield f"{l}  !=  {r}"

    times = defaultdict(list)

    print(f"\nRunning entries with python: {python_executable}")
    for entry in entries:
        print(f"\n - {entry}")

        # run each entry 3 times and take the median runtime
        for i in range(3):
            try:
                tic = timer()
                res = subprocess.run(
                    [python_executable, entry, measurement_file],
                    encoding="utf-8",
                    capture_output=True,
                    text=True,
                )
                toc = timer()
                res.check_returncode()
            except Exception as e:
                print(f"entry {entry} failed to run succesfully: {e}")
            else:
                diff = list(it.islice(compare(ground_truth, res.stdout.splitlines()), 10))
                if len(diff) != 0:
                    print(f"entry {entry} produced incorrect results:")
                    for diff_entry in diff:
                        print(diff_entry)
                    break
                else:
                    print(f"entry {entry} run {i+1} successful in {toc - tic:.4f} seconds")
                    times[entry].append(toc - tic)
    
    return times

def print_leaderboard(times: dict[pathlib.Path, list[float]]):
    print(f"\n========== leaderboard ==========")
    best_times_per_entry = []
    for entry_name, entry_times in times.items():
        picked_time = sorted(entry_times)[len(entry_times) // 2]
        best_times_per_entry.append((picked_time, entry_name))

    idx = 1
    for entry_time, entry_name in sorted(best_times_per_entry):
        print(f"#{idx}: {entry_name} with {entry_time}")
        idx += 1

if __name__ == "__main__":
    measurement_file, entries = get_entries(sys.argv)

    python_executable = "python3.12"
    if WITH_VERIFICATION:
        choice = input("\n1 for cpython (default), 2 for pypy: ").strip()
        if choice == "2":
            print("Using pypy for verification runs")
            python_executable = "python3.10" # assuming pypy is installed as python3.10

    ground_truth = make_ground_truth(measurement_file)

    entry_runtimes = main(measurement_file, entries, ground_truth, python_executable)
    print_leaderboard(entry_runtimes)
