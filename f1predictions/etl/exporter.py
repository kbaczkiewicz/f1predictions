import pandas as pd
import os.path

_DATADIR = os.path.dirname(__file__) + '/../../data'


class Exporter:
    def __init__(self, file: str, keys_to_export: list):
        self.file = file
        self.keys_to_export = keys_to_export

    def export(self) -> pd.DataFrame:
        global _DATADIR
        df = pd.read_csv(_DATADIR + '/' + self.file)

        return df[self.keys_to_export]