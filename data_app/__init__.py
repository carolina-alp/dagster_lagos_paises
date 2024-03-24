# from .repository import data_app
from dagster import Definitions, load_assets_from_modules

from .assets import feriados

all_assets = load_assets_from_modules([feriados])

defs = Definitions(
    assets=all_assets,
)
