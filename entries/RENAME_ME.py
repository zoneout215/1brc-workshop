# TODO: Rename this file to firstname_lastname.py
import sys


def print_measurements(cities: dict) -> None:
    """
    Print temperature measurements for cities in a formatted output.
    This function takes a dictionary of cities and their temperature measurements,
    then outputs the minimum, mean, and maximum temperatures for each city in 
    alphabetical order.

    NOTE: This function MUST print to stdout to be compatible with leaderboard.py
    which checks the results and measures runtime performance.

    Args:
        cities (dict): A dictionary where keys are city names (str) and values
                      contain temperature measurement data. The exact structure
                      of the values depends on how measurements are stored
                      (e.g., list of floats, measurement objects, etc.).
    Returns:
        None: This function prints directly to stdout and does not return a value.
    Output Format:
        Each city is printed on a separate line with the format:
        {city_name}={min_temp}/{mean_temp}/{max_temp}
        - All temperatures are formatted to exactly one decimal place
        - Temperature values range from -99.9 to 99.9 degrees
        - Cities are sorted alphabetically by name
        - Mean temperature is calculated as the arithmetic average
    Example:
        >>> cities_data = {
        ...     "Berlin": [10.2, -5.8, 23.1, 8.7],
        ...     "Amsterdam": [12.4, 8.9, 16.7, 11.2]
        ... }
        >>> print_measurements(cities_data)
        Amsterdam=8.9/12.3/16.7
        Berlin=-5.8/9.1/23.1
    """
    ...

def main(measurements_file_path: str) -> dict:
    """
    Process temperature measurements from a file and return aggregated statistics.
    Args:
        measurements_file_path (str): Path to the file containing temperature measurements.
    Returns:
        dict: Dictionary with station names and temperature measurements/ statistics.
    """
    ... 

if __name__ == '__main__':
    cities = main(sys.argv[1])
    print_measurements(cities)
