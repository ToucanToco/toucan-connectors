# Spreadsheets doc:
# https://drive.google.com/drive/folders/0B56th7Lb-9vScy0tMlpIeGNxQ0E

from .add_missing_row import add_missing_row
from .combine_columns_aggregation import combine_columns_aggregation
from .clean import clean_dataframe
from .compute_cumsum import compute_cumsum
from .compute_evolution import (
    compute_evolution,
    compute_evolution_by_criteria,
    compute_evolution_by_frequency
)
from .compute_ffill_by_group import compute_ffill_by_group
from .roll_up import roll_up
from .two_values_melt import two_values_melt
from .date_requester import date_requester_generator
