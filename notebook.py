# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "boto3",
#     "marimo",
#     "pandas",
#     "plotly",
#     "pyarrow",
# ]
# ///

import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # CDPS Dashboard

    ### This notebook reports statistics about MIT Libraries' Comprehensive Digital Preservation Services (CDPS) storage.
    """)
    return


@app.cell
def _():
    # IDs for bags that have been digitized outside of the Archivematica workflow
    digitized_bag_ids = [
        "6ba766c3-c778-4904-81c6-ebec0cb83f80",
        "0855323e-fab7-4b4d-acbf-0c4e460a1122",
        "253b2da4-03a7-4cc7-b0ff-60564ed21e27",
        "4c347b0a-46b1-4e0b-8316-04e9bc076301",
        "c6f6cbea-1afc-4cbc-bd36-7f0ea72efa79",
        "d29b5d7b-536d-4cec-b954-ba845408eaf0",
        "77d372d4-8dfa-4338-890c-341e45b856f8",
        "c842a632-9b1b-431a-969a-7013e945d157",
        "44b13df4-5892-424e-9db4-dc1d029df3f3",
        "e4c2b563-ecdc-4c19-8d4b-e6e5fa3ef00d",
        "75bbd422-b028-4697-9519-1eda0616bdf0",
        "1d336b68-4a09-4ced-84ff-bfd819207f45",
        "cf88c667-2841-42e0-8bce-dbf215c79dac",
        "a5a873ad-8cbe-467a-9163-e8e00183689f",
        "ed4bbf43-69d0-40cc-9a1b-5351226d2f05",
        "9f2cfdd8-17bc-4967-b275-604adf4cbd4f",
        "20d70ede-f395-4dc2-9886-e20911ad66e1",
        "3d0c5d43-b951-43cf-a8c5-7092d96658c8",
        "12246ea1-fa82-47e1-afc3-5146d37811f9",
        "f9e267e5-9e30-4c25-a6c2-3a37abf35732",
        "4419c268-d09d-42af-9aea-912c2d5fd722",
        "9231dc94-01fc-42bf-a5fc-1f5f010bd111",
        "4ef4b2aa-24ae-4579-a29f-0f696874b09e",
        "3e161569-43d6-4c6f-aafd-f39723f5c0f8",
        "b3719fa0-8b57-4c90-a40a-9e083e79858e",
        "438964ce-6b7a-47c0-9c6a-f3995ed9842a",
        "8f6e852d-1619-49c6-9279-c69f2fbf1125",
        "f8bcf269-2869-49a9-aaa9-0f40837ac214",
    ]
    return (digitized_bag_ids,)


@app.cell
def _(digitized_bag_ids, mo):
    # Functions

    import datetime
    import io
    import logging
    import math
    import mimetypes
    import os
    import re
    from collections.abc import Callable
    from pathlib import Path
    from urllib.parse import urlparse

    import boto3
    import numpy as np
    import pandas as pd
    import plotly.graph_objects as go
    from botocore.exceptions import ClientError

    logger = logging.getLogger(__name__)
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s")
    logger.setLevel(logging.INFO)

    def create_dataframe_for_date(
        s3_client, selected_date: str, symlink_uris: list, uri_cache: dict | None = None
    ) -> tuple[pd.DataFrame, dict]:
        """Orchestrate creation of processed inventory dataframe for a given date."""
        if uri_cache is None:
            uri_cache = {}

        # Check if parquet URIs are already cached for this date
        with mo.status.spinner(title=f"Loading inventory data for {selected_date}"):
            if selected_date in uri_cache:
                logger.info(f"Using cached parquet URIs for {selected_date}")
                parquet_uris = uri_cache[selected_date]
            else:
                # Get parquet URIs from symlink files and cache them
                logger.info(f"Fetching and caching parquet URIs for {selected_date}")
                parquet_uris = get_parquet_uris_from_symlinks(s3_client, symlink_uris)
                uri_cache[selected_date] = parquet_uris

            # Create dataframes from parquet files
            parquet_dfs = create_parquet_dataframes(s3_client, parquet_uris)

        with mo.status.spinner(title=f"Processing inventory data for {selected_date}"):
            # Process and return current inventory data
            current_df = process_inventory_data(parquet_dfs)

            # Transform current inventory dataframe
            cdps_df = transform_cdps_data(current_df)

        return cdps_df, uri_cache

    def get_parquet_uris_from_symlinks(s3_client, symlink_uris: list) -> list:
        """Retrieve parquet file URIs from symlink.txt files."""
        parquet_uris = []
        for symlink_uri in symlink_uris:
            parsed_symlink = urlparse(symlink_uri)
            symlink_bucket = parsed_symlink.netloc
            symlink_key = parsed_symlink.path.lstrip("/")
            try:
                logger.info(
                    f"Retrieving symlink file: s3://{symlink_bucket}/{symlink_key}"
                )
                response = s3_client.get_object(Bucket=symlink_bucket, Key=symlink_key)
            except ClientError:
                logger.exception("Client error while retrieving symlink.txt file:")
                raise
            parquet_uris.append(response["Body"].read().decode("utf-8"))
        return parquet_uris

    def create_parquet_dataframes(s3_client, parquet_uris: list) -> list:
        """Retrieve parquet files from S3 and convert to dataframes."""
        parquet_dfs = []
        for parquet_uri in parquet_uris:
            parsed_uri = urlparse(parquet_uri)
            parquet_bucket = parsed_uri.netloc
            parquet_key = parsed_uri.path.lstrip("/")
            try:
                logger.info(
                    f"Retrieving parquet file: s3://{parquet_bucket}/{parquet_key}"
                )
                s3_object = s3_client.get_object(Bucket=parquet_bucket, Key=parquet_key)
            except ClientError:
                logger.exception("Client error while retrieving parquet file:")
                raise
            parquet_df = pd.read_parquet(io.BytesIO(s3_object["Body"].read()))
            parquet_df.loc[:, "parquet_file"] = parquet_key.split("/")[-1]
            parquet_dfs.append(parquet_df)
        return parquet_dfs

    def process_inventory_data(
        dataframes: list,
    ) -> pd.DataFrame:
        """Process inventory dataframe to extract current objects.

        Args:
            dataframes: List of inventory DataFrames
            logger: Logger instance

        Returns:
            DataFrame containing only current objects
        """
        # Concatenate and deduplicate
        inventory_df = (
            pd.concat(dataframes, ignore_index=True)
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Filter for current objects (not deleted, latest version)
        inventory_df.loc[:, "is_current"] = (
            inventory_df["is_latest"] & ~inventory_df["is_delete_marker"]
        )
        current_df = (
            inventory_df.loc[inventory_df["is_current"]].copy().reset_index(drop=True)
        )
        logger.info(f"Current CDPS dataframe built with {len(current_df)} records.")
        return current_df

    def transform_cdps_data(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Apply all CDPS data transformations to the inventory dataframe."""
        return (
            dataframe.pipe(rename_bucket)
            .pipe(parse_s3_keys)
            .pipe(is_metadata)
            .pipe(preservation_level)
            .pipe(mime_types)
            .pipe(is_digitized_aip)
            .pipe(is_replica)
            .pipe(is_normalized_file)
            .pipe(is_aip)
            .pipe(set_status)
            .pipe(is_av_image)
        )

    def rename_bucket(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Extract label from bucket field (e.g., 'aipstore1b', 'dissemination')."""
        dataframe.loc[:, "bucket"] = dataframe["bucket"].str.extract(
            r"(aipstore\d+[a-z]?|dissemination|submission)", expand=False
        )
        return dataframe

    def parse_s3_keys(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Parse S3 keys to extract additional metadata."""
        key_parts = dataframe["key"].str.split("/", expand=True)

        dataframe.loc[:, "bagname"] = key_parts[8] if key_parts.shape[1] > 8 else ""
        dataframe["uuid"] = dataframe["key"].str.extract(
            r"(\S{8}-\S{4}-\S{4}-\S{4}-\S{12})"
        )
        dataframe["accession_name"] = dataframe["bagname"].str.split("-").str[0]
        dataframe.loc[:, "file"] = dataframe["key"].str.split("/").str[-1]
        dataframe.loc[:, "filepath"] = (
            dataframe["key"].str.split("/").str[9:].apply("/".join)
        )

        dataframe.loc[:, "extension"] = dataframe["key"].apply(
            lambda x: Path(x).suffix.lower()
        )
        return dataframe

    def is_metadata(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies metadata files in the DataFrame."""
        metadata_files = [
            "data/logs",
            "data/METS",
            "data/README.html",
            "data/objects/metadata",
            "data/objects/submissionDocumentation",
            "bag-info.txt",
            "bagit.txt",
            "manifest-sha256.txt",
            "tagmanifest-sha256.txt",
        ]
        dataframe.loc[:, "is_metadata"] = dataframe["key"].apply(
            lambda x: any(metadata_file in x for metadata_file in metadata_files)
        )

        return dataframe

    def preservation_level(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Add preservation level based on S3 bucket."""
        dataframe.loc[:, "preservation_level"] = np.where(
            dataframe["bucket"].str.contains("1b"),
            "Level 1",
            np.where(
                dataframe["bucket"].str.contains("2b"),
                "Level 2",
                np.where(
                    dataframe["bucket"].str.contains("3b"),
                    "Level 3",
                    np.where(
                        dataframe["bucket"].str.contains("4b")
                        | dataframe["bucket"].str.contains("4a"),
                        "Level 4",
                        np.where(
                            dataframe["bucket"].str.contains("5b")
                            | dataframe["bucket"].str.contains("5a"),
                            "Level 5",
                            np.where(
                                dataframe["bucket"].str.contains("dissemination"),
                                "Level 0",
                                np.where(
                                    dataframe["bucket"].str.contains("submission"),
                                    "Backlog",
                                    "ERROR",
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        return dataframe

    def mime_types(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Add mime type based on file extension."""
        mimetypes.add_type("application/vnd.ms-outlook", ".msg")
        mimetypes.add_type("text/plain", ".md5")
        mimetypes.add_type("text/x-server-parsed-html", ".shtml")
        mimetypes.add_type("application/warc", ".warc")
        mimetypes.add_type("video/mxf", ".mxf")
        dataframe.loc[:, "mimetype"] = dataframe["extension"].apply(
            lambda extension: (
                "unknown"
                if pd.isna(extension) or not extension
                else mimetypes.types_map.get(extension, "unknown")
            )
        )
        return dataframe

    def is_digitized_aip(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies digitized AIPS based on UUID."""
        digitized_aip_regex = r"\d{4}_\d{3}[r|R]{2}_\d{3}"
        dataframe.loc[:, "is_digitized_AIP"] = np.where(
            dataframe.accession_name.str.contains(digitized_aip_regex, regex=True),
            "Digitized",
            np.where(
                dataframe.uuid.isin(digitized_bag_ids),
                "Digitized",
                "Born Digital",
            ),
        )
        return dataframe

    def is_replica(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies replicas based on S3 bucket."""
        dataframe.loc[:, "is_replica"] = np.where(
            dataframe["bucket"].str.contains("4b"),
            True,
            np.where(dataframe["bucket"].str.contains("5b"), True, False),
        )
        return dataframe

    def is_normalized_file(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies normalized files based on several criteria."""
        am_uuid_regex = (
            r"-\S{8}-\S{4}-\S{4}-\S{4}-\S{12}."  # regex for archivematica file UUID
        )
        dataframe.loc[:, "is_normalized_file"] = np.where(
            dataframe.file.str.contains(am_uuid_regex, regex=True),
            True,
            np.where(
                dataframe.file.str.contains("data/thumbnails"),
                True,
                np.where(
                    (dataframe["is_digitized_AIP"] == "Digitized")
                    & (dataframe.file.str.contains(".pdf")),
                    True,
                    np.where(
                        dataframe["bucket"].str.contains("dissemination"), True, False
                    ),
                ),
            ),
        )
        return dataframe

    def is_aip(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies files in AIP packages."""
        dataframe.loc[:, "is_aip"] = np.where(
            dataframe["bucket"].str.contains("aipstore"), True, False
        )
        return dataframe

    def set_status(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Adds a status based on CDPS content categories."""
        dataframe.loc[:, "status"] = np.where(
            dataframe["is_replica"],
            "replica",
            np.where(
                dataframe["is_metadata"],
                "metadata",
                np.where(
                    dataframe["is_normalized_file"],
                    "normalized/access",
                    "original content",
                ),
            ),
        )
        return dataframe

    def is_av_image(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Identifies AV or Image content based on mimetype."""
        dataframe.loc[:, "is_av_image"] = np.where(
            dataframe["mimetype"].str.contains("audio")
            | dataframe["mimetype"].str.contains("video"),
            "AV",
            np.where(
                dataframe["mimetype"].str.contains("image"),
                "Still Image",
                "Everything else",
            ),
        )
        return dataframe

    def convert_size(size_bytes):
        """Convert byte counts into a human readable format."""
        if size_bytes == 0:
            return "0B"

        # Detect if the value is negative before converting to absolute value
        is_negative = size_bytes < 0
        size_bytes = abs(size_bytes)

        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = math.floor(math.log(size_bytes, 1024))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        result = f"{s} {size_name[i]}"

        if is_negative:
            result = f"-{result}"
        return result

    def file_count_growth_by_field(
        start_df: pd.DataFrame, end_df: pd.DataFrame, field: str
    ) -> pd.DataFrame:
        """Calculate file count growth grouped by a specified field."""
        file_count_start = start_df.groupby(field).size()
        file_count_end = end_df.groupby(field).size()
        return _calculate_growth_by_field(
            file_count_start, file_count_end, field, "start file count", "end file count"
        )

    def storage_growth_by_field(
        start_df: pd.DataFrame, end_df: pd.DataFrame, group_field: str
    ) -> pd.DataFrame:
        """Calculate storage growth grouped by a specified field."""
        end_storage = end_df.groupby(group_field)["size"].sum()
        start_storage = start_df.groupby(group_field)["size"].sum()
        return _calculate_growth_by_field(
            start_storage,
            end_storage,
            group_field,
            "start storage",
            "end storage",
            formatter=convert_size,
        )

    def _calculate_growth_by_field(
        start_series: pd.Series,
        end_series: pd.Series,
        field_name: str,
        start_label: str,
        end_label: str,
        formatter: Callable | None = None,
    ) -> pd.Series:
        """Calculate growth between two series."""
        # Reindex both series to ensure all values are present
        all_values = start_series.index.union(end_series.index)
        start_series = start_series.reindex(all_values, fill_value=0)
        end_series = end_series.reindex(all_values, fill_value=0)

        # Calculate growth and create dataframe
        growth_data = (
            (end_series - start_series).sort_values(ascending=False).reset_index()
        )
        growth_data.columns = [field_name, "growth"]

        # Add start and end values as columns
        start_values = start_series.reindex(
            growth_data[field_name], fill_value=0
        ).to_numpy()
        end_values = end_series.reindex(growth_data[field_name], fill_value=0).to_numpy()
        growth_data.insert(1, start_label, start_values)
        growth_data.insert(2, end_label, end_values)

        # Calculate percent growth
        growth_data["percent growth"] = (
            (growth_data["growth"].to_numpy() / start_values) * 100
        ).round(2).astype(str) + "%"

        # Apply formatter if provided
        if formatter:
            for field in [start_label, end_label, "growth"]:
                growth_data[field] = growth_data[field].apply(formatter)

        return growth_data

    return (
        ClientError,
        boto3,
        convert_size,
        create_dataframe_for_date,
        datetime,
        file_count_growth_by_field,
        go,
        logger,
        os,
        pd,
        re,
        storage_growth_by_field,
        urlparse,
    )


@app.cell
def _(ClientError, boto3, logger, mo, os, re, urlparse):
    # Get symlink files and dates as dict
    s3 = boto3.client("s3")

    logger.info("Building symlink dict from inventory locations")
    symlink_dict = {}

    # Iterate through the S3 inventory locations and build symlink dict
    with mo.status.spinner(title="Collecting inventory data..."):
        for s3_inventory_location in os.environ["S3_INVENTORY_LOCATIONS"].split(","):
            logger.info(f"Retrieving symlink.txt files from: {s3_inventory_location}")
            parsed_location = urlparse(s3_inventory_location)
            inventory_bucket = parsed_location.netloc
            inventory_prefix = parsed_location.path.lstrip("/")

            paginator = s3.get_paginator("list_objects_v2")
            try:
                for page in paginator.paginate(
                    Bucket=inventory_bucket, Prefix=inventory_prefix
                ):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if key.lower().endswith("symlink.txt"):
                            if match := re.search(r"dt=\d{4}-\d{2}-\d{2}", key):
                                date_string = match.group(0)[3:]
                            else:
                                raise ValueError(
                                    f"Could not parse datetime partition from uri: {key}"
                                )

                            if date_string not in symlink_dict:
                                symlink_dict[date_string] = []
                            symlink_dict[date_string].append(
                                f"s3://{inventory_bucket}/{key}"
                            )
            except ClientError:
                logger.exception("Client error while retrieving symlink.txt files:")
                raise
        logger.info(f"Symlink dict built with {len(symlink_dict)} dates.")
    return s3, symlink_dict


@app.cell
def _(datetime, mo):
    # Select date from calendar element

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    date_selector = mo.ui.date(value=str(yesterday), label="Select inventory Date")
    date_selector
    return date_selector, yesterday


@app.cell
def _(date_selector, mo, pd, symlink_dict):
    # Verify data exists for the selected date

    selected_date = pd.to_datetime(date_selector.value).strftime("%Y-%m-%d")

    mo.stop(
        selected_date not in symlink_dict,
        mo.md(f"No inventory data found for {selected_date}, select a different date"),
    )
    return (selected_date,)


@app.cell
def _():
    # Initialize cache for parquet file URIs to minimize AWS calls
    parquet_file_uri_cache = {}
    return (parquet_file_uri_cache,)


@app.cell
def _(
    create_dataframe_for_date,
    parquet_file_uri_cache,
    s3,
    selected_date,
    symlink_dict,
):
    # Create processed dataframe for selected date (cached for efficiency)
    cdps_df, updated_uri_cache = create_dataframe_for_date(
        s3, selected_date, symlink_dict[selected_date], parquet_file_uri_cache
    )
    # Update the cache in-place so it persists for future date selections
    parquet_file_uri_cache.update(updated_uri_cache)
    return (cdps_df,)


@app.cell(hide_code=True)
def _(cdps_df, go, mo):
    # File counts

    # Data views generated from filtered dataframes
    file_bucket_data = (
        cdps_df.groupby("bucket")
        .size()
        .to_frame("file count")
        .sort_values(by="bucket", ascending=False)
    )
    file_status_data = (
        cdps_df.groupby("status")
        .size()
        .to_frame("file count")
        .sort_values(by="status", ascending=False)
    )
    file_bucket_status_data = (
        cdps_df.groupby(["bucket", "status"])
        .size()
        .to_frame("file count")
        .sort_values(by="bucket", ascending=False)
    )
    file_preservation_data = (
        cdps_df.groupby("preservation_level")
        .size()
        .to_frame("file count")
        .sort_values(by="preservation_level", ascending=False)
    )

    # Create pie chart for presevation level
    file_preservation_data = file_preservation_data.reset_index()
    file_preservation_chart = go.Figure(
        data=[
            go.Pie(
                labels=file_preservation_data["preservation_level"],
                values=file_preservation_data["file count"],
                title="File count by preservation level",
            )
        ]
    )

    # Organizes the data views into tables vertically with labels
    file_counts_display = mo.vstack(
        [
            mo.md(
                "This section reports the total count of files by storage location, status category, and preservation level."
            ),
            mo.md("#### File count by bucket"),
            mo.ui.table(file_bucket_data, selection=None, page_size=25),
            mo.md("#### File count by status"),
            mo.ui.table(file_status_data, selection=None, page_size=25),
            mo.md("#### File count by bucket and status"),
            mo.ui.table(file_bucket_status_data, selection=None, page_size=25),
            mo.md("#### File count by preservation level"),
            mo.ui.plotly(file_preservation_chart),
            mo.ui.table(file_preservation_data, selection=None, page_size=25),
        ],
        gap=1,
    )
    return (file_counts_display,)


@app.cell(hide_code=True)
def _(cdps_df, convert_size, go, mo):
    # File type data

    # Data views generated from filtered dataframes
    file_extensions_file_count_data = (
        cdps_df.groupby("extension")
        .size()
        .to_frame("file count")
        .sort_values(by="file count", ascending=False)
    )

    mimetype_file_count_data = (
        cdps_df.groupby("mimetype")
        .size()
        .to_frame("file count")
        .sort_values(by="file count", ascending=False)
    )

    mimetype_size_data = (
        cdps_df.groupby("mimetype")["size"]
        .sum()
        .to_frame("bytes")
        .sort_values(by="bytes", ascending=False)
    )
    mimetype_size_data["size"] = mimetype_size_data["bytes"].apply(
        lambda x: convert_size(x)
    )

    top10_mimetype_size_data = mimetype_size_data.head(10)
    top10_mimetype_size_data = top10_mimetype_size_data.reset_index()

    # Create pie chart for top 10 mimetypes
    top10_mimetype_size_chart = go.Figure(
        data=[
            go.Pie(
                labels=top10_mimetype_size_data["mimetype"],
                values=top10_mimetype_size_data["bytes"],
                title="Total storage size for top 10 mimetypes",
            )
        ]
    )
    mimetype_size_data = mimetype_size_data.drop("bytes", axis=1)
    top10_mimetype_size_data = top10_mimetype_size_data.drop("bytes", axis=1)

    # Organizes the data views into tables vertically with labels
    file_type_display = mo.vstack(
        [
            mo.md(
                "This section groups files by their formats and mimetypes and reports the total counts and storage size. A file's format is extrapolated from the file's extension - file formats have not been validated in these datasets. These data points tell us what kinds of files are most prevalent and take up the most storage."
            ),
            mo.md("#### File count by file extension"),
            mo.ui.table(file_extensions_file_count_data, selection=None, page_size=25),
            mo.md("#### File count by mimetype"),
            mo.ui.table(mimetype_file_count_data, selection=None, page_size=25),
            mo.md("#### Storage size by mimetype"),
            mo.ui.table(mimetype_size_data, selection=None, page_size=25),
            mo.md("#### Storage size for top 10 mimetypes"),
            mo.ui.plotly(top10_mimetype_size_chart),
            mo.ui.table(top10_mimetype_size_data, selection=None, page_size=25),
        ],
        gap=1,
    )
    return (file_type_display,)


@app.cell
def _(cdps_df, convert_size, go, mo):
    # Storage data

    # Data views generated from filtered dataframes
    storage_bucket = cdps_df.groupby("bucket")["size"].sum().sort_values(ascending=True)
    storage_bucket_data = storage_bucket.reset_index()
    storage_bucket_chart = go.Figure(
        data=[
            go.Pie(
                labels=storage_bucket_data["bucket"],
                values=storage_bucket_data["size"],
                title="Storage size by bucket",
            )
        ]
    )
    storage_bucket_data = storage_bucket_data.assign(
        size=lambda x: x["size"].apply(convert_size)
    )

    storage_status = cdps_df.groupby("status")["size"].sum().sort_values(ascending=True)
    storage_status_data = storage_status.reset_index()
    storage_status_chart = go.Figure(
        data=[
            go.Pie(
                labels=storage_status_data["status"],
                values=storage_status_data["size"],
                title="Storage size by status",
            )
        ]
    )
    storage_status_data = storage_status_data.assign(
        size=lambda x: x["size"].apply(convert_size)
    )

    storage_status_bucket = (
        cdps_df.groupby(["status", "bucket"])["size"].sum().sort_values(ascending=True)
    )
    storage_status_bucket_data = storage_status_bucket.reset_index()
    storage_status_bucket_data = storage_status_bucket_data.assign(
        size=lambda x: x["size"].apply(convert_size)
    )

    storage_preservation = (
        cdps_df.groupby("preservation_level")["size"].sum().sort_values(ascending=True)
    )
    storage_preservation_data = storage_preservation.reset_index()
    storage_preservation_chart = go.Figure(
        data=[
            go.Pie(
                labels=storage_preservation_data["preservation_level"],
                values=storage_preservation_data["size"],
                title="Size by preservation level",
            )
        ]
    )
    storage_preservation_data = storage_preservation_data.assign(
        size=lambda x: x["size"].apply(convert_size)
    )

    largest_file = cdps_df.loc[cdps_df["size"].idxmax()]
    largest_file_data = {
        "File extension": largest_file["extension"],
        "Storage size": convert_size(largest_file["size"]),
        "Bag": largest_file["bagname"],
        "Parquet file": largest_file["parquet_file"],
    }

    metadata_files_data = cdps_df[cdps_df["status"] == "metadata"]
    largest_metadata_file = metadata_files_data.loc[metadata_files_data["size"].idxmax()]
    largest_metadata_file_data = {
        "File extension": largest_metadata_file["extension"],
        "Storage size": convert_size(largest_metadata_file["size"]),
        "Bag": largest_metadata_file["bagname"],
        "Parquet file": largest_metadata_file["parquet_file"],
    }

    top10_largest_files_data = (
        cdps_df.sort_values(by="size", ascending=False)
        .loc[:, ["extension", "bagname", "size"]]
        .assign(size=lambda x: x["size"].apply(convert_size))
        .reset_index(drop=True)[:10]
    )

    mean_file_size = {"Mean file storage size": convert_size(cdps_df["size"].mean())}

    mean_file_size_by_status = (
        cdps_df.groupby("status")["size"].mean().apply(convert_size).to_dict()
    )

    mean_file_size_by_preservation_level = (
        cdps_df.groupby("preservation_level")["size"].mean().apply(convert_size).to_dict()
    )

    # Organizes the data views into tables vertically with labels
    storage_display = mo.vstack(
        [
            mo.md(
                "This section sums file storage size by storage location, file status category, and file preservation level. It also reports the largest content and metadata files in storage and the mathematical mean file storage sizes for each file status. These data points help us understand how workflows and collecting trends impact preservation storage."
            ),
            mo.md("#### Storage size by bucket"),
            mo.ui.plotly(storage_bucket_chart),
            mo.ui.table(storage_bucket_data, selection=None, page_size=25),
            mo.md("#### Storage size by status"),
            mo.ui.plotly(storage_status_chart),
            mo.ui.table(storage_status_data, selection=None, page_size=25),
            mo.md("#### Storage size by status and bucket"),
            mo.ui.table(storage_status_bucket_data, selection=None, page_size=25),
            mo.md("#### Storage size by preservation level"),
            mo.ui.plotly(storage_preservation_chart),
            mo.ui.table(storage_preservation_data, selection=None, page_size=25),
            mo.md("#### Largest file"),
            mo.ui.table(largest_file_data, selection=None, page_size=25),
            mo.md("#### Largest metadata file"),
            mo.ui.table(largest_metadata_file_data, selection=None, page_size=25),
            mo.md("#### Top 10 largest files"),
            mo.ui.table(top10_largest_files_data, selection=None, page_size=25),
            mo.md("#### Mean file storage size"),
            mo.ui.table(mean_file_size, selection=None, page_size=25),
            mo.md("#### Mean file storage size by status"),
            mo.ui.table(mean_file_size_by_status, selection=None, page_size=25),
            mo.md("#### Mean file storage size by preservation level"),
            mo.ui.table(
                mean_file_size_by_preservation_level, selection=None, page_size=25
            ),
        ],
        gap=1,
    )
    return (storage_display,)


@app.cell
def _(cdps_df, convert_size, mo):
    # AIPs

    # Data views generated from filtered dataframes
    aip_df = cdps_df[cdps_df["is_aip"]]

    total_aip_count = {"Total AIP count": aip_df["uuid"].nunique()}

    aip_count_by_bucket_data = (
        aip_df.groupby(["bucket"])["uuid"]
        .nunique()
        .sort_values(ascending=False)
        .reset_index()
    )

    aips_by_size_data = (
        aip_df.groupby("bagname")["size"].sum().sort_values(ascending=False).reset_index()
    )
    largest_aip_by_size_data = {
        "Largest AIP by storage size": aips_by_size_data.loc[0].bagname,
        "Storage size": convert_size(aips_by_size_data.iloc[0]["size"]),
    }

    aips_by_file_count_data = (
        aip_df.groupby("bagname")["size"]
        .count()
        .sort_values(ascending=False)
        .to_frame("file_count")
        .reset_index()
    )
    largest_aip_by_file_count_data = {
        "Largest AIP by file count": aips_by_file_count_data.loc[0].bagname,
        "File count": aips_by_file_count_data.iloc[0]["file_count"],
    }

    mean_aip_statistics = {
        "Mean AIP file storage size": convert_size(aips_by_size_data["size"].mean()),
        "Mean AIP file count": aips_by_file_count_data["file_count"].mean().round(0),
    }

    # Organizes the data views into tables vertically with labels
    aip_display = mo.vstack(
        [
            mo.md(
                "This section counts archival information packages (AIPs) and reports their storage size by storage location. It also reports the largest and mathematical mean AIPs by storage size and file count. AIPs are the packages that contain preservation files, which largely correspond to archival collections and digitization requests. These data points are used to inform CDPS system requirements."
            ),
            mo.md("#### Total AIP count"),
            mo.ui.table(total_aip_count, selection=None, page_size=25),
            mo.md("#### AIP count by bucket"),
            mo.ui.table(aip_count_by_bucket_data, selection=None, page_size=25),
            mo.md("#### Largest AIP by storage size"),
            mo.ui.table(largest_aip_by_size_data, selection=None, page_size=25),
            mo.md("#### Largest AIP by file count"),
            mo.ui.table(largest_aip_by_file_count_data, selection=None, page_size=25),
            mo.md("#### Mean AIP statistics"),
            mo.ui.table(mean_aip_statistics, selection=None, page_size=25),
        ],
        gap=1,
    )
    return aip_df, aip_display


@app.cell(hide_code=True)
def _(cdps_df, convert_size, go, mo):
    # Born-digital vs. digitized content

    # Data views generated from filtered dataframes
    born_digital_digitized_size_data = (
        cdps_df.groupby("is_digitized_AIP")["size"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    born_digital_digitized_size_chart = go.Figure(
        data=[
            go.Pie(
                labels=born_digital_digitized_size_data["is_digitized_AIP"],
                values=born_digital_digitized_size_data["size"],
                title="Storage size by born-digital vs. digitized",
            )
        ]
    )
    born_digital_digitized_size_data = born_digital_digitized_size_data.assign(
        size=lambda x: x["size"].apply(convert_size)
    )

    born_digital_digitized_bucket_size_data = (
        cdps_df.groupby(["is_digitized_AIP", "bucket"])["size"]
        .sum()
        .sort_values(ascending=True)
    )
    born_digital_digitized_bucket_size_data = (
        born_digital_digitized_bucket_size_data.reset_index()
    )
    born_digital_digitized_bucket_size_data = (
        born_digital_digitized_bucket_size_data.assign(
            size=lambda x: x["size"].apply(convert_size)
        )
    )

    born_digital_digitized_file_count_data = (
        cdps_df.groupby("is_digitized_AIP")
        .size()
        .sort_values(ascending=False)
        .to_frame("file count")
        .reset_index()
    )

    # Organizes the data views into tables vertically with labels
    born_digital_digitized_display = mo.vstack(
        [
            mo.md(
                "This section compares born-digital files and digitized files by storage size, storage location, and file count. These data points help us understand how much of the preservation program is dedicated to digitization workflows vs born-digital collecting."
            ),
            mo.md("#### Storage size by born-digital vs. digitized"),
            mo.ui.plotly(born_digital_digitized_size_chart),
            mo.ui.table(born_digital_digitized_size_data, selection=None, page_size=25),
            mo.md("#### Storage size by born-digital vs. digitized and bucket"),
            mo.ui.table(
                born_digital_digitized_bucket_size_data, selection=None, page_size=25
            ),
            mo.md("#### File count by born-digital vs. digitized"),
            mo.ui.table(
                born_digital_digitized_file_count_data, selection=None, page_size=25
            ),
        ],
        gap=1,
    )
    return (born_digital_digitized_display,)


@app.cell(hide_code=True)
def _(cdps_df, convert_size, go, mo):
    # AV vs. Image

    # Data views generated from filtered dataframes
    av_file_count_data = (
        cdps_df.groupby("is_av_image")["size"]
        .count()
        .to_frame("file count")
        .reset_index()
    )
    av_file_count_data.sort_values(by="file count", ascending=False)

    av_storage_size_data = (
        cdps_df.groupby("is_av_image")["size"].sum().to_frame("bytes").reset_index()
    )
    av_storage_size_data.sort_values(by="bytes", ascending=False)
    av_storage_size_data["size"] = av_storage_size_data["bytes"].apply(
        lambda x: convert_size(x)
    )
    av_storage_size_chart = go.Figure(
        data=[
            go.Pie(
                labels=av_storage_size_data["is_av_image"],
                values=av_storage_size_data["bytes"],
                title="Still image, audiovisual, and everything else by storage size",
            )
        ]
    )
    av_storage_size_data = av_storage_size_data.drop("bytes", axis=1)

    # Organizes the data views into tables vertically with labels
    image_av_display = mo.vstack(
        [
            mo.md(
                "This section compares audiovisual files, still image files, and everything else. It groups mimetypes into the three categories. AV and still image files are large. These data points demonstrate the impact AV and still image format projects and collections have on digital preservation."
            ),
            mo.md("#### Still image, audiovisual, and everything else by file count"),
            mo.ui.table(av_file_count_data, selection=None, page_size=25),
            mo.md("#### Still image, audiovisual, and everything else by storage size"),
            mo.ui.plotly(av_storage_size_chart),
            mo.ui.table(av_storage_size_data, selection=None, page_size=25),
        ],
        gap=1,
    )
    return (image_av_display,)


@app.cell(hide_code=True)
def _(cdps_df, convert_size, go, mo):
    # Original files

    # Data views generated from filtered dataframes
    original_files = cdps_df[cdps_df["status"] == "original content"].copy()

    original_files_extension_file_count_data = (
        original_files.groupby("extension")
        .size()
        .to_frame("file count")
        .sort_values(by="file count", ascending=False)
        .reset_index()
    )

    original_files_mimetype_file_count_data = (
        original_files.groupby("mimetype")
        .size()
        .to_frame("file count")
        .sort_values(by="file count", ascending=False)
        .reset_index()
    )

    original_files_preservation_level_file_count_data = (
        original_files.groupby("preservation_level")
        .size()
        .to_frame("file count")
        .sort_values(by="file count", ascending=False)
        .reset_index()
    )

    original_files_mimetype_size_data = (
        original_files.groupby("mimetype")["size"]
        .sum()
        .to_frame("bytes")
        .sort_values(by="bytes", ascending=False)
    )
    original_files_mimetype_size_data["size"] = original_files_mimetype_size_data[
        "bytes"
    ].apply(lambda x: convert_size(x))

    # Create pie chart for top 10 mimetypes
    top10_original_files_mimetype_size_data = original_files_mimetype_size_data.head(10)
    top10_original_files_mimetype_size_data = (
        top10_original_files_mimetype_size_data.reset_index()
    )
    top10_original_files_mimetype_chart = go.Figure(
        data=[
            go.Pie(
                labels=top10_original_files_mimetype_size_data["mimetype"],
                values=top10_original_files_mimetype_size_data["bytes"],
                title="Total storage size for top 10 original file mimetypes",
            )
        ]
    )
    original_files_mimetype_size_data = original_files_mimetype_size_data.drop(
        "bytes", axis=1
    )
    top10_original_files_mimetype_size_data = (
        top10_original_files_mimetype_size_data.drop("bytes", axis=1)
    )

    original_files_born_digital_digitized_size_data = (
        original_files.groupby("is_digitized_AIP")["size"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    original_files_born_digital_digitized_size_chart = go.Figure(
        data=[
            go.Pie(
                labels=original_files_born_digital_digitized_size_data[
                    "is_digitized_AIP"
                ],
                values=original_files_born_digital_digitized_size_data["size"],
                title="Original files storage size by born-digital vs. digitized",
            )
        ]
    )
    original_files_born_digital_digitized_size_data = (
        original_files_born_digital_digitized_size_data.assign(
            size=lambda x: x["size"].apply(convert_size)
        )
    )

    # Organizes the data views into tables vertically with labels
    original_files_display = mo.vstack(
        [
            mo.md(
                "This section presets data points about 'original files' which, for the purposes of this notebook, are files that are not duplicate copies, normalizations, access derivatives, or metadata. The data points filter for original files and repeat some of the statistics presented in other sections. These data points help us dig slightly deeper into collection content analysis."
            ),
            mo.md("#### Original files by file extension"),
            mo.ui.table(
                original_files_extension_file_count_data, selection=None, page_size=25
            ),
            mo.md("#### Original files by mimetype"),
            mo.ui.table(
                original_files_mimetype_file_count_data, selection=None, page_size=25
            ),
            mo.md("#### Original files by mimetype and storage size"),
            mo.ui.table(original_files_mimetype_size_data, selection=None, page_size=25),
            mo.md("#### Storage size for top 10 original file mimetypes"),
            mo.ui.plotly(top10_original_files_mimetype_chart),
            mo.ui.table(
                top10_original_files_mimetype_size_data, selection=None, page_size=25
            ),
            mo.md("#### Original files storage size by born-digital vs. digitized"),
            mo.ui.plotly(original_files_born_digital_digitized_size_chart),
            mo.ui.table(
                original_files_preservation_level_file_count_data,
                selection=None,
                page_size=25,
            ),
            mo.ui.table(
                original_files_born_digital_digitized_size_data,
                selection=None,
                page_size=25,
            ),
        ],
        gap=1,
    )
    return (original_files_display,)


@app.cell
def _(aip_df, cdps_df, convert_size, mo):
    # Summary stats

    total_files = mo.stat(
        label="Total files",
        value=f"{len(cdps_df):,}",
    )

    total_storage = mo.stat(
        label="Total storage size",
        value=f"{convert_size(cdps_df["size"].sum())}",
    )

    preserved_files = mo.stat(label="Preserved files", value=f"{len(aip_df):,}")

    preserved_storage = mo.stat(
        label="Total preserved size",
        value=f"{convert_size(aip_df["size"].sum())}",
    )

    # Organizes the summary stats horizontally
    current_summary = mo.hstack(
        [total_files, total_storage, preserved_files, preserved_storage],
        widths="equal",
        gap=1,
    )
    return (current_summary,)


@app.cell(hide_code=True)
def _(mo):
    # About this notebook

    about_display = mo.md(
        """ The notebook's data comes from the CDPS buckets' AWS S3 inventories. The notebook can display data from any existing set of inventories. Use the calendar to select a date. Inventories are updated daily. The notebook can also present a basic comparison of two dates, see the lower sections for more information.

    The data includes the preservation storage buckets (AIPStores 1 through 5), the Submission bucket (for ingest and backlog storage), and the Dissemination bucket (for sharing access copies). The statistics often analyze all buckets but some, with notice and where appropriate, have been filtered to only analyze the preservation AIPStores.

    The notebook's data is intended for MIT Libraries staff use. It has minor redactions that protect data security and archive restrictions. The full AWS inventories remain restricted.

    The notebook categorizes files in ways that facilitate analysis. Here's a summary of the logic used to categorize the files:
    - ***Metadata:*** If a file has specific file names or is stored in specific directories that indicate it is descriptive or preservation metadata, its status is categorized metadata.
    - ***Normalized/access derivative:*** If a file has an Archivematica file UUID appended to the filename, is a PDF in a digitized AIP, is in a thumbnails directory, or is stored in the Dissemination bucket, its status is categorized normalized/access derivative.
    - ***Replica copy:*** If an AIP is a backup copy stored in redundant storage (4b or 5b), the files within it are given the status category replica copy.
    - ***Original content:*** Any file that is not a replica copy, a normalized/access derivative, or metadata is given the status category original content.
    - ***Mimetypes:*** Mimetypes are estimated using the file's extension and the Python mimetypes library. File formats have not been validated in these datasets.
    - ***AIP:*** Any file that has gone through routine preservation workflows and is in an AIPStore bucket is marked as being part of an Archival Information Package (AIP).
    - ***Digitized:*** If the AIP containing a file has a name indicating it came from MIT Libraries digitization workflows, the file is marked digitized.
    - ***Born-digital:*** Any files that are not in AIPs marked digitized are marked born-digital.

    For more information about the Libraries' preservation infrastructure see [Repository and Digital Content Storage Systems and Services](https://mitlibraries.atlassian.net/wiki/x/AQDsEQE).

    The notebook's code is maintained in the MIT Libraries [GitHub marimo-cdps-dashboard repo](https://github.com/MITLibraries/marimo-cdps-dashboard).

    Have questions or comments? Contact the Digital Preservation Coordinator (digitalpreservation@mit.edu)."""
    )
    return (about_display,)


@app.cell
def _(
    about_display,
    aip_display,
    born_digital_digitized_display,
    current_summary,
    file_counts_display,
    file_type_display,
    image_av_display,
    mo,
    original_files_display,
    storage_display,
):
    # Dashboard

    # Collects all the data displays with labels in an accordion element
    data_category_accordion = mo.accordion(
        lazy=True,
        items={
            "About this notebook:": about_display,
            "Storage data": storage_display,
            "File counts": file_counts_display,
            "File type data": file_type_display,
            "Archival information packages": aip_display,
            "Born-digital vs. digitized content": born_digital_digitized_display,
            "AV, image, and everything else": image_av_display,
            "Original files": original_files_display,
        },
    )

    # Organizes elements on the page vertically
    mo.vstack(
        [mo.md("### CDPS Summary"), current_summary, data_category_accordion],
        gap=1,
    )
    return


@app.cell
def _(datetime, mo, yesterday):
    # Select date range for growth data points

    start_date_default = datetime.date(2025, 10, 18)
    start_date_selector = mo.ui.date(
        value=str(start_date_default), label="Select Start Date"
    )
    end_date_selector = mo.ui.date(value=str(yesterday), label="Select End Date")

    date_selectors = mo.hstack([start_date_selector, end_date_selector])
    mo.vstack(
        [
            mo.md("""<hr class="dotted">
                    <style>
                    hr.dotted {
                        border-top: 8px dotted;
                        border-bottom: none;
                        }
                    </style>"""),
            mo.md("### Compare two dates:"),
            date_selectors,
        ]
    )
    return end_date_selector, start_date_selector


@app.cell
def _(mo):
    compare_button = mo.ui.run_button(label="Create comparison")
    compare_button
    return (compare_button,)


@app.cell
def _(
    compare_button,
    end_date_selector,
    mo,
    pd,
    start_date_selector,
    symlink_dict,
):
    # Verify start date is before end date and that data exists for the selected dates

    mo.stop(not compare_button.value, mo.md("Select two dates and click button to begin"))

    mo.stop(
        start_date_selector.value > end_date_selector.value,
        mo.md("Start date must be before end date, please select a valid date range"),
    )

    mo.stop(
        start_date_selector.value == end_date_selector.value,
        mo.md("Start date and end date must be different"),
    )

    start_date = pd.to_datetime(start_date_selector.value).strftime("%Y-%m-%d")
    end_date = pd.to_datetime(end_date_selector.value).strftime("%Y-%m-%d")

    for series_date in [start_date, end_date]:
        mo.stop(
            series_date not in symlink_dict,
            mo.md(f"No inventory data found for {series_date}, select a different date"),
        )
    return end_date, start_date


@app.cell
def _(
    create_dataframe_for_date,
    end_date,
    parquet_file_uri_cache,
    s3,
    start_date,
    symlink_dict,
):
    # Create dataframes for both dates

    comparison_dfs = {}
    for date_key, date_value in {"start": start_date, "end": end_date}.items():
        dataframe, comparison_cache = create_dataframe_for_date(
            s3,
            date_value,
            symlink_dict[date_value],
            parquet_file_uri_cache,
        )
        # Update the cache in-place so it persists for future date selections
        parquet_file_uri_cache.update(comparison_cache)
        comparison_dfs[date_key] = dataframe

    start_df = comparison_dfs["start"]
    end_df = comparison_dfs["end"]
    start_aip_df = start_df[start_df["is_aip"]]
    end_aip_df = end_df[end_df["is_aip"]]
    return end_aip_df, end_df, start_aip_df, start_df


@app.cell(hide_code=True)
def _(convert_size, end_df, mo, start_df):
    # Date range growth totals summary

    # Total file count statistics
    start_file_count = len(start_df)
    end_file_count = len(end_df)
    start_file_count_stat = mo.stat(label="Start file count", value=start_file_count)
    end_file_count_stat = mo.stat(label="End file count", value=end_file_count)

    total_file_count_growth = mo.stat(
        label="Total file count growth",
        value=f"{end_file_count - start_file_count:,}",
    )

    total_file_count_growth_percent = mo.stat(
        label="Total file count growth percentage",
        value=f"{round((end_file_count - start_file_count ) / start_file_count * 100, 2)} %",
    )

    # Total storage statistics
    start_storage = start_df["size"].sum()
    end_storage = end_df["size"].sum()
    start_storage_stat = mo.stat(label="Start storage", value=convert_size(start_storage))
    end_storage_stat = mo.stat(label="End storage", value=convert_size(end_storage))

    total_storage_growth = mo.stat(
        label="Total storage size growth",
        value=f"{convert_size(end_storage - start_storage)}",
    )
    total_storage_growth_percent = mo.stat(
        label="Total storage size growth percentage",
        value=f"{round((end_storage - start_storage ) / start_storage * 100, 2)} %",
    )

    # displays
    file_compare_display = mo.hstack(
        [
            start_file_count_stat,
            end_file_count_stat,
            total_file_count_growth,
            total_file_count_growth_percent,
        ],
        widths="equal",
        gap=1,
    )

    storage_compare_display = mo.hstack(
        [
            start_storage_stat,
            end_storage_stat,
            total_storage_growth,
            total_storage_growth_percent,
        ],
        widths="equal",
        gap=1,
    )

    comparison_summary = mo.vstack(
        [
            mo.md("_File count growth:_"),
            file_compare_display,
            mo.md("_Storage size growth:_"),
            storage_compare_display,
        ]
    )
    return (comparison_summary,)


@app.cell(hide_code=True)
def _(convert_size, end_aip_df, mo, start_aip_df):
    # AIP and preserved content comparisons

    # Preserved file count statistics
    start_preserved_file_count = len(start_aip_df)
    end_preserved_file_count = len(end_aip_df)
    start_preserved_file_count_stat = mo.stat(
        label="Start preserved files", value=start_preserved_file_count
    )
    end_preserved_file_count_stat = mo.stat(
        label="End preserved file count", value=end_preserved_file_count
    )

    preserved_file_count_growth = mo.stat(
        label="Preserved file count growth",
        value=f"{end_preserved_file_count - start_preserved_file_count:,}",
    )

    preserved_file_count_growth_percent = mo.stat(
        label="Preserved file count growth percentage",
        value=f"{round((end_preserved_file_count - start_preserved_file_count ) / start_preserved_file_count * 100, 2)} %",
    )

    # Preserved total storage statistics
    start_preserved_storage = start_aip_df["size"].sum()
    end_preserved_storage = end_aip_df["size"].sum()
    start_preserved_storage_stat = mo.stat(
        label="Start preserved storage", value=convert_size(start_preserved_storage)
    )
    end_preserved_storage_stat = mo.stat(
        label="End preserved storage", value=convert_size(end_preserved_storage)
    )

    preserved_storage_growth = mo.stat(
        label="Preserved storage size growth",
        value=f"{convert_size(end_preserved_storage - start_preserved_storage)}",
    )
    preserved_storage_growth_percent = mo.stat(
        label="Preserved storage size growth percentage",
        value=f"{round((end_preserved_storage - start_preserved_storage ) / start_preserved_storage * 100, 2)} %",
    )

    # New AIPs
    end_uuids = set(end_aip_df["uuid"].unique())
    start_uuids = set(start_aip_df["uuid"].unique())
    new_aip_uuids = end_uuids - start_uuids
    new_aips = mo.stat(label="New AIPs", value=f"{len(new_aip_uuids)}")

    # Deleted AIPs
    deleted_aip_uuids = start_uuids - end_uuids
    deleted_aips = mo.stat(label="Deleted AIPs", value=f"{len(deleted_aip_uuids)}")

    # growth in AIP counts
    growth_aip_uuids = len(end_aip_df["uuid"].unique()) - len(
        start_aip_df["uuid"].unique()
    )
    growth_aips = mo.stat(label="Growth in total AIPs", value=f"{growth_aip_uuids}")

    # Find largest added AIP by storage size
    aip_sizes_end = end_aip_df.groupby("uuid")["size"].sum()
    new_aip_sizes = aip_sizes_end[aip_sizes_end.index.isin(new_aip_uuids)]
    if len(new_aip_sizes) > 0:
        largest_aip_uuid = new_aip_sizes.idxmax()
        largest_aip_data = str(
            largest_aip_uuid + " (" + convert_size(new_aip_sizes[largest_aip_uuid]) + ")"
        )
    else:
        largest_aip_data = {"NA: No new AIPs during this period"}

    largest_aip_table = mo.stat(
        label="Largest AIP added",
        value=largest_aip_data,
    )

    # Displays

    preserved_file_display = mo.hstack(
        [
            start_preserved_file_count_stat,
            end_preserved_file_count_stat,
            preserved_file_count_growth,
            preserved_file_count_growth_percent,
        ],
        widths="equal",
        gap=1,
    )

    preserved_storage_display = mo.hstack(
        [
            start_preserved_storage_stat,
            end_preserved_storage_stat,
            preserved_storage_growth,
            preserved_storage_growth_percent,
        ],
        widths="equal",
        gap=1,
    )

    aips_display = mo.hstack(
        [new_aips, deleted_aips, growth_aips],
        widths="equal",
        gap=1,
    )

    preserved_growth_display = mo.vstack(
        [
            mo.md("_AIP count growth:_"),
            aips_display,
            mo.md("_AIP file count growth:_"),
            preserved_file_display,
            mo.md("_AIP storage size growths:_"),
            preserved_storage_display,
            largest_aip_table,
        ]
    )
    return (preserved_growth_display,)


@app.cell
def _(end_df, file_count_growth_by_field, mo, start_df):
    # File count growth:

    # File count growth by bucket
    file_count_growth_by_bucket_data = file_count_growth_by_field(
        start_df, end_df, "bucket"
    )
    file_count_growth_by_bucket_table = mo.ui.table(
        file_count_growth_by_bucket_data,
        label="File count growth by bucket",
        selection=None,
        page_size=25,
    )

    # File count growth by status
    file_count_growth_by_status_data = file_count_growth_by_field(
        start_df, end_df, "status"
    )
    file_count_growth_by_status_table = mo.ui.table(
        file_count_growth_by_status_data,
        label="File count growth by status",
        selection=None,
        page_size=25,
    )

    # File count growth by preservation_level
    file_count_growth_by_preservation_level_data = file_count_growth_by_field(
        start_df, end_df, "preservation_level"
    )
    file_count_growth_by_preservation_level_table = mo.ui.table(
        file_count_growth_by_preservation_level_data,
        label="File count growth by preservation level",
        selection=None,
        page_size=25,
    )

    file_counts_growth_display = mo.vstack(
        [
            file_count_growth_by_bucket_table,
            file_count_growth_by_status_table,
            file_count_growth_by_preservation_level_table,
        ]
    )
    return (file_counts_growth_display,)


@app.cell
def _(end_df, mo, start_df, storage_growth_by_field):
    # Storage size growth

    # Storage growth by bucket
    storage_growth_by_bucket_data = storage_growth_by_field(start_df, end_df, "bucket")
    storage_growth_by_bucket_table = mo.ui.table(
        storage_growth_by_bucket_data,
        label="Storage size growth by bucket",
        selection=None,
        page_size=25,
    )

    # Storage growth by status
    storage_growth_by_status_data = storage_growth_by_field(start_df, end_df, "status")
    storage_growth_by_status_table = mo.ui.table(
        storage_growth_by_status_data,
        label="Storage size growth by status",
        selection=None,
        page_size=25,
    )

    # Storage growth by preservation_level
    storage_growth_by_preservation_level_data = storage_growth_by_field(
        start_df, end_df, "preservation_level"
    )
    storage_growth_by_preservation_level_table = mo.ui.table(
        storage_growth_by_preservation_level_data,
        label="Storage size growth by preservation level",
        selection=None,
        page_size=25,
    )

    # display

    storage_growth_display = mo.vstack(
        [
            storage_growth_by_bucket_table,
            storage_growth_by_status_table,
            storage_growth_by_preservation_level_table,
        ]
    )
    return (storage_growth_display,)


@app.cell
def _(
    end_df,
    file_count_growth_by_field,
    mo,
    start_df,
    storage_growth_by_field,
):
    # file type growth

    # File type growth by extension
    file_type_growth_by_extension_data = file_count_growth_by_field(
        start_df, end_df, "extension"
    )
    file_type_growth_by_extension_table = mo.ui.table(
        file_type_growth_by_extension_data,
        label="File type growth by extension",
        selection=None,
        page_size=25,
    )

    # File type growth by mimetype
    file_type_growth_by_mimetype_data = file_count_growth_by_field(
        start_df, end_df, "mimetype"
    )
    file_type_growth_by_mimetype_table = mo.ui.table(
        file_type_growth_by_mimetype_data,
        label="File type growth by mimetype",
        selection=None,
        page_size=25,
    )

    # Storage growth by mimetype
    storage_growth_by_mimetype_data = storage_growth_by_field(
        start_df, end_df, "mimetype"
    )
    storage_growth_by_mimetype_table = mo.ui.table(
        storage_growth_by_mimetype_data,
        label="Storage size growth by mimetype",
        selection=None,
        page_size=25,
    )

    # Storage growth by extension
    storage_growth_by_extension_data = storage_growth_by_field(
        start_df, end_df, "extension"
    )
    storage_growth_by_extension_table = mo.ui.table(
        storage_growth_by_extension_data,
        label="Storage size growth by extension",
        selection=None,
        page_size=25,
    )

    file_type_growth_display = mo.vstack(
        [
            file_type_growth_by_extension_table,
            file_type_growth_by_mimetype_table,
            storage_growth_by_mimetype_table,
            storage_growth_by_extension_table,
        ]
    )
    return (file_type_growth_display,)


@app.cell
def _(mo):
    # about comparison
    about_comparison = mo.md(
        """The notebook creates comparison statistics by comparing the inventories for the selected end date to the inventories for the selected start date. It does not comprehensively compare all data from the selected date range. For example, if an AIP was both added and deleted between the selected dates, the comparison statstics are unaware that it ever existed.\n
    The comparison statistics analyze data from the preservation storage buckets (AIPStores 1 through 5), the Submission bucket, and the Dissemination bucket. The statistics usually analyze all buckets, but stats in the "archival information packages" section have been filtered to only analyze the preservation AIPStores."""
    )
    return (about_comparison,)


@app.cell
def _(
    about_comparison,
    comparison_summary,
    file_counts_growth_display,
    file_type_growth_display,
    mo,
    preserved_growth_display,
    storage_growth_display,
):
    # Comparison Dashboard

    # Collects all the data displays with labels in an accordion element
    data_growth_category_accordion = mo.accordion(
        lazy=True,
        items={
            "About this data comparison:": about_comparison,
            "Storage data compare:": storage_growth_display,
            "File count compare:": file_counts_growth_display,
            "File type compare:": file_type_growth_display,
            "Archival information packages:": preserved_growth_display,
        },
    )

    # Organizes elements on the page vertically
    mo.vstack(
        [
            mo.md("### Comparison Summary"),
            comparison_summary,
            data_growth_category_accordion,
        ],
        gap=1,
    )
    return


if __name__ == "__main__":
    app.run()
