from pathlib import Path
import pandas as pd

class DataOutput:

    def __init__(self):
        pass

    def output_to_csv(self, data, filename):
        """ Method used to create the output CSV files of every performed calculation.
        
        Parameters
        ----------
        data: pandas.DataFrame
            Data we want to output to the CSV
        filename: str
            Indicates the filename you want to give to output CSV (without extension)
        """

        # We set the path for the output to be written to
        save_dir = Path(__file__).parent.parent.parent
        output_dir = (save_dir / "../output").resolve()
        filename = filename + ".csv"
        file_path = (output_dir / filename).resolve()

        # If the output doesn't exist, create the csv
        if not file_path.is_file():
            data.to_csv(path_or_buf=file_path)