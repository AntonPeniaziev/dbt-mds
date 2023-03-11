from dagster import build_init_resource_context, io_manager, with_resources, file_relative_path
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
from dagster_dbt import load_assets_from_dbt_project, dbt_cli_resource


DBT_PROJECT_PATH = file_relative_path(__file__, "../imdb_dbt")
DBT_PROFILES = file_relative_path(__file__,     "../imdb_dbt/config")

# if larger project use load_assets_from_dbt_manifest
# dbt_assets = load_assets_from_dbt_manifest(json.load(DBT_PROJECT_PATH + "manifest.json", encoding="utf8"))


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

ab_assets = with_resources(
    build_airbyte_assets(
        connection_id="13fb7a92-dbf0-44ad-a5f5-2f4286ac84d0",
        destination_tables=["movies"],
        asset_key_prefix=["movies"]
    ),
    {"airbyte": my_airbyte_resource},
)

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["imdb_dbt"], source_key_prefix=["company_db"]
)
dbt_assets = with_resources(
    dbt_assets,
    {"dbt": dbt_cli_resource},
)
# airbyte_assets = load_assets_from_airbyte_instance(my_airbyte_resource)





#
#
# @asset(group_name="hackernews", compute_kind="HackerNews API")
# def hackernews_topstory_ids() -> List[int]:
#     """
#     Get up to 500 top stories from the HackerNews topstories endpoint.
#
#     API Docs: https://github.com/HackerNews/API#new-top-and-best-stories
#     """
#     newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
#     top_500_newstories = requests.get(newstories_url).json()
#     return top_500_newstories
#
#
# @asset(group_name="hackernews", compute_kind="HackerNews API")
# def hackernews_topstories(
#     context: OpExecutionContext, hackernews_topstory_ids: List[int]
# ) -> pd.DataFrame:
#     """
#     Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.
#
#     API Docs: https://github.com/HackerNews/API#items
#     """
#     results = []
#     for item_id in hackernews_topstory_ids:
#         item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
#         results.append(item)
#         if len(results) % 20 == 0:
#             context.log.info(f"Got {len(results)} items so far.")
#
#     df = pd.DataFrame(results)
#
#     # Dagster supports attaching arbitrary metadata to asset materializations. This metadata will be
#     # shown in the run logs and also be displayed on the "Activity" tab of the "Asset Details" page in the UI.
#     # This metadata would be useful for monitoring and maintaining the asset as you iterate.
#     # Read more about in asset metadata in https://docs.dagster.io/concepts/assets/software-defined-assets#recording-materialization-metadata
#     context.add_output_metadata(
#         {
#             "num_records": len(df),
#             "preview": MetadataValue.md(df.head().to_markdown()),
#         }
#     )
#     return df
#
#
# @asset(group_name="hackernews", compute_kind="Plot")
# def hackernews_topstories_word_cloud(
#     context: OpExecutionContext, hackernews_topstories: pd.DataFrame
# ) -> bytes:
#     """
#     Exploratory analysis: Generate a word cloud from the current top 500 HackerNews top stories.
#     Embed the plot into a Markdown metadata for quick view.
#
#     Read more about how to create word clouds in http://amueller.github.io/word_cloud/.
#     """
#     stopwords = set(STOPWORDS)
#     stopwords.update(["Ask", "Show", "HN"])
#     titles_text = " ".join([str(item) for item in hackernews_topstories["title"]])
#     titles_cloud = WordCloud(stopwords=stopwords, background_color="white").generate(titles_text)
#
#     # Generate the word cloud image
#     plt.figure(figsize=(8, 8), facecolor=None)
#     plt.imshow(titles_cloud, interpolation="bilinear")
#     plt.axis("off")
#     plt.tight_layout(pad=0)
#
#     # Save the image to a buffer and embed the image into Markdown content for quick view
#     buffer = BytesIO()
#     plt.savefig(buffer, format="png")
#     image_data = base64.b64encode(buffer.getvalue())
#     md_content = f"![img](data:image/png;base64,{image_data.decode()})"
#
#     # Attach the Markdown content as metadata to the asset
#     # Read about more metadata types in https://docs.dagster.io/_apidocs/ops#metadata-types
#     context.add_output_metadata({"plot": MetadataValue.md(md_content)})
#
#     return image_data