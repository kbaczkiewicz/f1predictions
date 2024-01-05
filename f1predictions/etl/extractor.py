import pandas as pd
import os.path

_DATADIR = os.path.dirname(__file__) + '/../../data'


class Extractor:
    def __init__(self, file: str, keys_to_export: list):
        self.file = file
        self.keys_to_export = keys_to_export

    def extract(self) -> pd.DataFrame:
        global _DATADIR
        df = pd.read_csv(_DATADIR + '/' + self.file)

        return df[self.keys_to_export]


class RelatedModelsExtractor(Extractor):
    def __init__(self, file: str, keys_to_export: list[str], related_file: str, key_to_join: str):
        super().__init__(file, keys_to_export)
        self.related_file = related_file
        self.key_to_join = key_to_join

    def extract(self) -> pd.DataFrame:
        global _DATADIR
        df = pd.read_csv(_DATADIR + '/' + self.file)
        related_df = pd.read_csv(_DATADIR + '/' + self.related_file)

        return df.join(related_df.set_index(self.key_to_join), on=self.key_to_join, lsuffix='_')[self.keys_to_export]


class DirectDataFrameExtractor(Extractor):
    def __init__(self, df: pd.DataFrame):
        self.dataframe = df

    def extract(self) -> pd.DataFrame:
        return self.dataframe
