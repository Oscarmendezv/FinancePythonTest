from pathlib import Path
import pandas as pd

class DataOutput:

    def __init__(self):
        pass

    def output_to_csv(self, data, name, source=None, consolidated=False):
        # We set the path for the output to be written to
        save_dir = Path(__file__).parent.parent.parent
        output_dir = (save_dir / "../output").resolve()

        if not source == None:
            if not consolidated:
                filename = source + "__" + name + ".csv"
            
            else:
                filename = "consolidated_" + source + "__comparison.csv"
                  
        else:
            if consolidated:
                filename = "consolidated__" + name + ".csv"
            else:
                filename = name +".csv"

        file_path = (output_dir / filename).resolve()

        # If the output doesn't exist, create the csv
        if not file_path.is_file():
            data.to_csv(path_or_buf=file_path)