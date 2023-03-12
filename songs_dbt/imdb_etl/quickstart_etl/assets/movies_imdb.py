import json

from dagster import build_init_resource_context, io_manager, with_resources, file_relative_path, \
    load_assets_from_modules, Definitions
from dagster_aws.s3 import s3_pickle_io_manager
import base64
from io import BytesIO
from typing import List

import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import MetadataValue, OpExecutionContext, asset

from wordcloud import STOPWORDS, WordCloud
from dagster_airbyte import airbyte_resource, airbyte_sync_op, build_airbyte_assets
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition
from dagster_airbyte import load_assets_from_airbyte_instance
from quickstart_etl.external_api.imdb import ImdbApi
from dagster import AssetIn, MetadataValue, AssetOut
from dagster_dbt import load_assets_from_dbt_project, dbt_cli_resource, load_assets_from_dbt_manifest

DBT_PROJECT_PATH = file_relative_path(__file__, "../imdb_dbt")
DBT_PROFILES = file_relative_path(__file__,     "../imdb_dbt/config")

# if larger project use load_assets_from_dbt_manifest



@asset(group_name="imbd", compute_kind="IMDB API", key_prefix=["imdb_api"])
def imdb_movies_and_cast():
    """
    Get movies and cast from the IMDB API.
    """
    imdb_api = ImdbApi()
    imdb_api.upload_movies_to_s3('tv_movie', '7.5,10', '1990-01-01,2023-01-01', '10000', '2')

my_airbyte_resource=airbyte_resource.configured(
    {
        "host": "localhost",
        "port": "8000",
        # If using basic auth, include username and password:
        "username": "airbyte",
        "password": "password",
    }
    )

ab_assets_movies = with_resources(
    build_airbyte_assets(
        connection_id="13fb7a92-dbf0-44ad-a5f5-2f4286ac84d0",
        destination_tables=["movies"],
        asset_key_prefix=["company_db"]
    ),
    {"airbyte": my_airbyte_resource},
)

ab_assets_actors = with_resources(
    build_airbyte_assets(
        connection_id="b89d861d-eea5-4aa7-a217-98fe6401da7c",
        destination_tables=["actors"],
        asset_key_prefix=["company_db"]
    ),
    {"airbyte": my_airbyte_resource},
)

ab_assets_writers = with_resources(
    build_airbyte_assets(
        connection_id="e9bb0f62-1ba6-4388-b942-17a94e4eb94b",
        destination_tables=["writers"],
        asset_key_prefix=["company_db"]
    ),
    {"airbyte": my_airbyte_resource},
)

dbt_assets = load_assets_from_dbt_manifest(manifest_json=json.load(open(DBT_PROJECT_PATH + "/target/manifest.json")),
                                           key_prefix=["imdb_dbt"])
# dbt_assets = load_assets_from_dbt_project(
#     project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["imdb_dbt"]
# )
dbt_assets = with_resources(
    dbt_assets,
    {"dbt": dbt_cli_resource},
)
# airbyte_assets = load_assets_from_airbyte_instance(my_airbyte_resource)

# module_assets = load_assets_from_modules([assets])
# @job(resource_defs={"airbyte": my_airbyte_resource})
# def my_simple_airbyte_job():
#     sync_fake()


# defs = Definitions(assets=list(module_assets), resources=resources)


