import json
import sys
from contextlib import closing
from pathlib import Path
from typing import List, Optional

import sqlalchemy
import typer

import dbcat.settings
from dbcat.catalog.models import CatForeignKey
from dbcat.api import (
    add_athena_source,
    add_mysql_source,
    add_postgresql_source,
    add_redshift_source,
    add_snowflake_source,
    add_sqlite_source,
    add_bigquery_source,
    import_from_object_stream,
    init_db,
    open_catalog,
    scan_sources, add_external_source, parse_spec, export_format
)
from dbcat.generators import NoMatchesError

schema_help_text = """
Scan only schemas matching schema; When this option is not specified, all
non-system schemas in the target database will be dumped. Multiple schemas can
be selected by writing multiple --include switches. Also, the schema parameter is
interpreted as a regular expression, so multiple schemas can also be selected
by writing wildcard characters in the pattern. When using wildcards, be careful
to quote the pattern if needed to prevent the shell from expanding the wildcards;
"""
exclude_schema_help_text = """
Do not scan any schemas matching the schema pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to exclude
 schemas matching any of several patterns.

When both --include and ---exclude are given, the behavior is to dump just the schemas that
match at least one --include switch but no --exclude switches.
If --exclude appears without --include, then schemas matching --exclude are excluded from what
is otherwise a normal scan.")
"""
table_help_text = """
Scan only tables matching table. Multiple tables can be selected by writing
multiple switches. Also, the table parameter is interpreted as a regular
expression, so multiple tables can also be selected by writing wildcard
characters in the pattern. When using wildcards, be careful to quote the pattern
 if needed to prevent the shell from expanding the wildcards.
"""
exclude_table_help_text = """
Do not scan any tables matching the table pattern. The pattern is interpreted
according to the same rules as for --include. --exclude can be given more than once to
exclude tables matching any of several patterns.

When both switches are given, the behavior is to dump just the tables that
match at least one --include switch but no --exclude switches. If --exclude appears without
--include, then tables matching --exclude are excluded from what is otherwise a normal scan.
"""

app = typer.Typer()


@app.command()
def scan(
        source_name: Optional[List[str]] = typer.Option(
            None, help="List of names of database and data warehouses"
        ),
        include_schema: Optional[List[str]] = typer.Option(None, help=schema_help_text),
        exclude_schema: Optional[List[str]] = typer.Option(
            None, help=exclude_schema_help_text
        ),
        include_table: Optional[List[str]] = typer.Option(None, help=table_help_text),
        exclude_table: Optional[List[str]] = typer.Option(
            None, help=exclude_table_help_text
        ),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        init_db(catalog)
        try:
            scan_sources(
                catalog=catalog,
                source_names=source_name,
                include_schema_regex=include_schema,
                exclude_schema_regex=exclude_schema,
                include_table_regex=include_table,
                exclude_table_regex=exclude_table,
            )
        except NoMatchesError:
            typer.echo(
                "No schema or tables scanned. Ensure include/exclude patterns are correct "
                "and database has tables"
            )


@app.command()
def add_sqlite(
        name: str = typer.Option(..., help="A memorable name for the database"),
        path: Path = typer.Option(..., help="File path to SQLite database"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_sqlite_source(catalog=catalog, name=name, path=path)
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
        typer.echo("Registered SQLite database {}".format(name))


@app.command()
def add_postgresql(
        name: str = typer.Option(..., help="A memorable name for the database"),
        username: str = typer.Option(..., help="Username or role to connect database"),
        password: str = typer.Option(..., help="Password of username or role"),
        database: str = typer.Option(..., help="Database name"),
        uri: str = typer.Option(..., help="Hostname or URI of the database"),
        port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_postgresql_source(
                    catalog=catalog,
                    name=name,
                    username=username,
                    password=password,
                    database=database,
                    uri=uri,
                    port=port,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
    typer.echo("Registered Postgres database {}".format(name))


@app.command()
def add_mysql(
        name: str = typer.Option(..., help="A memorable name for the database"),
        username: str = typer.Option(..., help="Username or role to connect database"),
        password: str = typer.Option(..., help="Password of username or role"),
        database: str = typer.Option(..., help="Database name"),
        uri: str = typer.Option(..., help="Hostname or URI of the database"),
        port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_mysql_source(
                    catalog=catalog,
                    name=name,
                    username=username,
                    password=password,
                    database=database,
                    uri=uri,
                    port=port,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
        typer.echo("Registered MySQL database {}".format(name))


@app.command()
def add_redshift(
        name: str = typer.Option(..., help="A memorable name for the database"),
        username: str = typer.Option(..., help="Username or role to connect database"),
        password: str = typer.Option(..., help="Password of username or role"),
        database: str = typer.Option(..., help="Database name"),
        uri: str = typer.Option(..., help="Hostname or URI of the database"),
        port: Optional[int] = typer.Option(None, help="Port number of the database"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_redshift_source(
                    catalog=catalog,
                    name=name,
                    username=username,
                    password=password,
                    database=database,
                    uri=uri,
                    port=port,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
        typer.echo("Registered Redshift database {}".format(name))


@app.command()
def add_snowflake(
        name: str = typer.Option(..., help="A memorable name for the database"),
        username: str = typer.Option(..., help="Username or role to connect database"),
        password: str = typer.Option(..., help="Password of username or role"),
        database: str = typer.Option(..., help="Database name"),
        account: str = typer.Option(..., help="Snowflake Account Name"),
        warehouse: str = typer.Option(..., help="Snowflake Warehouse Name"),
        role: str = typer.Option(..., help="Snowflake Role Name"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_snowflake_source(
                    catalog=catalog,
                    name=name,
                    username=username,
                    password=password,
                    database=database,
                    account=account,
                    warehouse=warehouse,
                    role=role,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
        typer.echo("Registered Snowflake database {}".format(name))


@app.command()
def add_athena(
        name: str = typer.Option(..., help="A memorable name for the database"),
        aws_access_key_id: str = typer.Option(..., help="AWS Access Key"),
        aws_secret_access_key: str = typer.Option(..., help="AWS Secret Key"),
        region_name: str = typer.Option(..., help="AWS Region Name"),
        s3_staging_dir: str = typer.Option(..., help="S3 Staging Dir"),
        mfa: str = typer.Option(None, help="MFA"),
        aws_session_token: str = typer.Option(None, help="AWS Session Token")
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_athena_source(
                    catalog=catalog,
                    name=name,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name,
                    s3_staging_dir=s3_staging_dir,
                    mfa=mfa,
                    aws_session_token=aws_session_token,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
    typer.echo("Registered AWS Athena {}".format(name))


@app.command()
def add_bigquery(
    name: str = typer.Option(..., help="A memorable name for the database"),
    username: str = typer.Option(..., help="Email to connect to database"),
    project_id: str = typer.Option(..., help="Project id to connect to database"),
    key_path: str = typer.Option(..., help="File Path to BigQuery Private Info (json)"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_bigquery_source(
                    catalog=catalog,
                    name = name,
                    username = username,
                    project_id = project_id,
                    key_path = key_path,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
    typer.echo("Registered Big Query Database {}".format(name))


@app.command()
def add_external(
    name: str = typer.Option(..., help="A memorable name for the database"),
):
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            try:
                add_external_source(
                    catalog=catalog,
                    name=name,
                )
            except sqlalchemy.exc.IntegrityError:
                typer.echo("Catalog with {} name already exist".format(name))
                return
    typer.echo("Registered External Database {}".format(name))


@app.command("import")
def import_from_stdin():
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )

    def json_stream(f):
        for line in f:
            yield json.loads(line.strip())

    with closing(catalog):
        with catalog.managed_session:
            import_from_object_stream(catalog, json_stream(sys.stdin))


@app.command("references-to")
def references_to(
        column: str = typer.Argument(
            ...,
            help="Column spec of src:schema:table:column",
            metavar="SPEC"
        ),
):
    """Find direct references to a column.

    This piece is absolutely prototype code/UI. I think a more useful
    implementation might:

    * Support searches on incomplete information.
    * Put this under a query subcommand.
    * Have a compact human form corresponding to the output form above.
    * Allow the ingestion of the partial import form (maybe?)
    * Optionally report results in the JSON import form.

    """
    spec = parse_spec(column)
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            for fk in catalog.query_references_to(
                spec.column, spec.table, spec.schema, spec.source
            ):
                print("{}:{}:{}:{}".format(
                    fk.source.table.schema.source.name,
                    fk.source.table.schema.name,
                    fk.source.table.name,
                    fk.source.name,
                ))


@app.command("target-of")
def target_of(
        column: str = typer.Argument(
            ...,
            help="Column spec of src:schema:table:column",
            metavar="SPEC"
        ),
):
    """Find what a column refers to.

    This piece is absolutely prototype code/UI. I think a more useful
    implementation might:

    * Support searches on incomplete information.
    * Put this under a query subcommand.
    * Have a compact human form corresponding to the output form above.
    * Allow the ingestion of the partial import form (maybe?)
    * Optionally report results in the JSON import form.

    """
    spec = parse_spec(column)
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            for fk in catalog.query_references_from(
                spec.column, spec.table, spec.schema, spec.source
            ):
                print("{}:{}:{}:{}".format(
                    fk.target.table.schema.source.name,
                    fk.target.table.schema.name,
                    fk.target.table.name,
                    fk.target.name,
                ))


@app.command("columns")
def search_columns(
    query: str = typer.Argument(
        ...,
        help="Source column spec of src:schema:table:column",
        metavar="SPEC"
    ),
):
    """Search for columns matching query

    This piece is absolutely prototype code/UI. I think a more useful
    implementation might:

    * Put this under a query subcommand.
    * Have a compact query for; e.g. t3.t3.*.id to find all links to ID fields.
    * Have a compact human form corresponding to the output form above.
    * Allow the ingestion of the partial import form (maybe?)
    * Optionally report results in the JSON import form.

    """
    spec = parse_spec(query, default='%')
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            for col in sorted(catalog.search_column(
                    spec.column, spec.table, spec.schema, spec.source
            )):
                print("{}:{}:{}:{}".format(
                    col.table.schema.source.name,
                    col.table.schema.name,
                    col.table.name,
                    col.name,
                ))


@app.command("tables")
def search_tables(
    query: str = typer.Argument(
        ...,
        help="Source column spec of src:schema:table:column",
        metavar="SPEC"
    ),
):
    """Search for tables matching query.

    This piece is absolutely prototype code/UI. I think a more useful
    implementation might:

    * Put this under a query subcommand.
    * Have a compact human form corresponding to the output form above.
    * Allow the ingestion of the partial import form (maybe?)
    * Optionally report results in the JSON import form.

    """
    spec = parse_spec(query, default='%')
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            tables = set()
            for col in catalog.search_column(
                spec.column,spec.table, spec.schema, spec.source,
            ):
                tables.add((
                    col.table.schema.source.name,
                    col.table.schema.name,
                    col.table.name,
                ))
            for tbl in sorted(tables):
                print("{}:{}:{}".format(*tbl))


@app.command("add-foreign-key")
def add_foreign_key(
        source: str = typer.Option(
            ...,
            help="Source column spec of src:schema:table:column",
            metavar="SOURCE"
        ),
        target: str = typer.Option(
            ...,
            help="Target column spec of src:schema:table:column",
            metavar="TARGET"
        ),
):
    source_spec = parse_spec(source)
    if source_spec.kind != "column":
        typer.echo("The source must be a column", file=sys.stderr)
        raise typer.Exit(code=126)
    target_spec = parse_spec(target)
    if target_spec.kind != "column":
        typer.echo("The target must be a column", file=sys.stderr)
        raise typer.Exit(code=126)
    catalog = open_catalog(
        app_dir=dbcat.settings.APP_DIR,
        secret=dbcat.settings.CATALOG_SECRET,
        path=dbcat.settings.CATALOG_PATH,
        host=dbcat.settings.CATALOG_HOST,
        port=dbcat.settings.CATALOG_PORT,
        user=dbcat.settings.CATALOG_USER,
        password=dbcat.settings.CATALOG_PASSWORD,
        database=dbcat.settings.CATALOG_DB,
    )
    with closing(catalog):
        with catalog.managed_session:
            source_column = catalog.get_column(
                source_spec.source, source_spec.schema, source_spec.table, source_spec.column
            )
            target_column = catalog.get_column(
                target_spec.source, target_spec.schema, target_spec.table, target_spec.column
            )
            fk = catalog.add_foreign_key(source_column, target_column)
            typer.echo(json.dumps(export_format(fk)))
