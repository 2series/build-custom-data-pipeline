# import dlt
# from dlt.sources.helpers import requests

# # Specify the URL of the API endpoint
# url = "https://api.github.com/repos/dlt-hub/dlt/issues"
# # Make a request and check if it was successful
# response = requests.get(url)
# response.raise_for_status()

# pipeline = dlt.pipeline(
#     pipeline_name="github_issues",
#     destination="duckdb",
#     dataset_name="github_data",
# )
# # The response contains a list of issues
# load_info = pipeline.run(response.json(), table_name="issues")

# print(load_info)

## Append OR replace our data with the write_disposition parameter
# import dlt
# from dlt.sources.helpers import requests

# # Specify the URL of the API endpoint
# url = "https://api.github.com/repos/dlt-hub/dlt/issues"
# # Make a request and check if it was successful
# response = requests.get(url)
# response.raise_for_status()

# pipeline = dlt.pipeline(
#     pipeline_name="github_issues",
#     destination="duckdb",
#     dataset_name="github_data",
# )

# # The response contains a list of issues
# load_info = pipeline.run(
#     response.json(), 
#     table_name="issues",
#     write_disposition="replace"
# )

# print(load_info)


## Load only new data (incremental loading)
# import dlt
# from dlt.sources.helpers import requests

# @dlt.resource(table_name="issues", write_disposition="append")
# def get_issues(
#     created_at=dlt.sources.incremental(
#         "created_at", 
#         initial_value="1970-01-01T00:00:00Z"
#         )
# ):
#     # NOTE: we read only open issues to minimize number of calls to the API.
#     # There's a limit of ~50 calls for not authenticated Github users.
#     url = (
#         "https://api.github.com/repos/dlt-hub/dlt/issues"
#         "?per_page=100&sort=created&directions=desc&state=open"
#     )

#     while True:
#         response = requests.get(url)
#         response.raise_for_status()
#         yield response.json()

#         # Stop requesting pages if the last element was already
#         # older than initial value
#         # Note: incremental will skip those items anyway, we just
#         # do not want to use the api limits
#         if created_at.start_out_of_range:
#             break

#         # get next page
#         if "next" not in response.links:
#             break
#         url = response.links["next"]["url"]

# pipeline = dlt.pipeline(
#     pipeline_name="github_issues_incremental",
#     destination="duckdb",
#     dataset_name="github_data_append",
# )

# load_info = pipeline.run(get_issues)
# row_counts = pipeline.last_trace.last_normalize_info

# print(row_counts)
# print("------")
# print(load_info)


## Update and deduplicate our data
import dlt
from dlt.sources.helpers import requests

@dlt.resource(
    table_name="issues",
    write_disposition="merge",
    primary_key="id",
)
def get_issues(
    updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
):
    # NOTE: we read only open issues to minimize number of calls to
    # the API. There's a limit of ~50 calls for not authenticated
    # Github users
    url = (
        "https://api.github.com/repos/dlt-hub/dlt/issues"
        f"?since={updated_at.last_value}&per_page=100&sort=updated"
        "&directions=desc&state=open"
    )

    while True:
        response = requests.get(url)
        response.raise_for_status()
        yield response.json()

        # Get next page
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]

pipeline = dlt.pipeline(
    pipeline_name="github_issues_merge",
    destination="duckdb",
    dataset_name="github_data_merge",
)
load_info = pipeline.run(get_issues)
row_counts = pipeline.last_trace.last_normalize_info

print(row_counts)
print("------")
print(load_info)