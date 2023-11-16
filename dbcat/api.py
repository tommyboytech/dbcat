import itertools
import json
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Sequence

import sqlalchemy
import yaml
from alembic import command
from sqlalchemy.orm.exc import NoResultFound

import dbcat.settings
from dbcat.catalog import Catalog, CatForeignKey, CatSource
from dbcat.catalog.catalog import PGCatalog, SqliteCatalog
from dbcat.catalog.db import DbScanner
from dbcat.generators import NoMatchesError
from dbcat.migrations import get_alembic_config

LOGGER = logging.getLogger(__name__)


class IntegrityError(Exception):
    ...

class Action(str, Enum):
    add = "add"
    remove = "remove"


class OutputFormat(str, Enum):
    tabular = "tabular"
    json = "json"


def init_db(catalog_obj: Catalog) -> None:
    """
    Initialize database
    """

    config = get_alembic_config(catalog_obj.engine)
    command.upgrade(config, "heads")
    LOGGER.info("Initialized the database")


def catalog_connection(
    secret: str,
    path: Optional[Path] = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    dbcat.settings.CATALOG_SECRET = secret

    if (
        host is not None
        and user is not None
        and password is not None
        and database is not None
    ):
        LOGGER.info(f"Open PG Catalog at {host}")
        return PGCatalog(
            host=host, port=port, user=user, password=password, database=database,
        )
    elif path is not None:
        LOGGER.info(f"Open Sqlite Catalog at {path}")
        return SqliteCatalog(path=str(path))

    raise AttributeError("None of Path or Postgres connection parameters are provided")


def catalog_connection_yaml(config: str) -> Catalog:
    config_yaml = yaml.safe_load(config)
    LOGGER.debug("Open Catalog from config")
    if "path" in config_yaml and config_yaml["path"] is not None:
        config_yaml["path"] = Path(config_yaml["path"])
    return catalog_connection(**config_yaml["catalog"])


def open_catalog(
    app_dir: Path,
    secret: str,
    path: Optional[Path] = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
) -> Catalog:
    try:
        catalog = catalog_connection(
            secret=secret,
            path=path,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
    except AttributeError:
        LOGGER.info("No catalog options given as parameters.")
        config_file = app_dir / "catalog.yml"
        if config_file.exists():
            with config_file.open() as f:
                LOGGER.debug("Open Catalog from config file %s", config_file)
                catalog = catalog_connection_yaml(f.read())
        else:
            LOGGER.debug("Open default Sqlite Catalog in %s/catalog.db", app_dir)
            catalog = catalog_connection(
                path=app_dir / "catalog.db",
                secret=dbcat.settings.DEFAULT_CATALOG_SECRET,
            )

    init_db(catalog_obj=catalog)
    return catalog


def scan_sources(
    catalog: Catalog,
    source_names: Optional[List[str]] = None,
    include_schema_regex: Optional[List[str]] = None,
    exclude_schema_regex: Optional[List[str]] = None,
    include_table_regex: Optional[List[str]] = None,
    exclude_table_regex: Optional[List[str]] = None,
):
    with catalog.managed_session:
        if source_names is not None and len(source_names) > 0:
            sources: List[CatSource] = []
            for source_name in source_names:
                try:
                    sources.append(catalog.get_source(source_name))
                except NoResultFound:
                    LOGGER.error("Source '%s' not found", source_name)
        else:
            sources = catalog.get_sources()

        LOGGER.info("%d sources will be scanned", len(sources))
        for source in sources:
            scanner = DbScanner(
                catalog,
                source,
                include_schema_regex_str=include_schema_regex,
                exclude_schema_regex_str=exclude_schema_regex,
                include_table_regex_str=include_table_regex,
                exclude_table_regex_str=exclude_table_regex,
            )
            LOGGER.info("Scanning {}".format(scanner.name))
            try:
                scanner.scan()
            except StopIteration:
                raise NoMatchesError


def add_sqlite_source(
    catalog: Catalog, name: str, path: Path,
):
    with catalog.managed_session:
        catalog.add_source(name=name, uri=str(path), source_type="sqlite")


def add_postgresql_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="postgresql",
        )


def add_mysql_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="mysql",
        )


def add_redshift_source(
    catalog: Catalog,
    name: str,
    username: str,
    password: str,
    database: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            uri=uri,
            port=port,
            source_type="redshift",
        )


def add_snowflake_source(
    catalog: Catalog,
    name: str,
    account: str,
    username: str,
    password: str,
    database: str,
    warehouse: str,
    role: str,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            database=database,
            account=account,
            warehouse=warehouse,
            role=role,
            source_type="snowflake",
        )


def add_athena_source(
    catalog: Catalog,
    name: str,
    region_name: str,
    s3_staging_dir: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    mfa: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            s3_staging_dir=s3_staging_dir,
            mfa = mfa,
            aws_session_token=aws_session_token,
            source_type="athena",
        )


def add_bigquery_source(
    catalog: Catalog,
    name: str,
    username:str,
    project_id: str,
    key_path: str,
) -> CatSource:
    with catalog.commit_context:
            return catalog.add_source(
                name=name,
                username=username,
                project_id = project_id,
                key_path=key_path,
                source_type="bigquery",
            )


def add_oracle_source(
    catalog: Catalog,
    name: str, 
    username: str,
    password: str,
    service_name: str,
    uri: str,
    port: Optional[int] = None,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            username=username,
            password=password,
            service_name=service_name,
            uri=uri,
            port=port,
            source_type="oracle",
        )


def add_external_source(
        catalog: Catalog,
        name: str,
) -> CatSource:
    with catalog.commit_context:
        return catalog.add_source(
            name=name,
            source_type="external",
        )


def import_from_object_stream(catalog: Catalog, stream: Sequence[dict]):
    """Import an object stream.

    Each item in stream is a dictionary. Each dictionary represents an element
    in the catalog. Items must be import after all of their dependencies.

    """
    for i, obj in enumerate(stream):
        errors = validate_import_obj(catalog, obj)
        if errors:
            LOGGER.error("cannot import item %d", i)
            LOGGER.error("contents: %s", json.dumps(obj))
            for e in errors:
                LOGGER.error("validation error: %s", str(e))
            raise IntegrityError("import item {} as errors".format(i))
        consume_import_obj(catalog, obj)


def validate_import_obj(catalog: Catalog, obj: dict) -> Sequence[str]:
    if "type" not in obj:
        return ["no 'type' field in object"]
    if obj["type"] == "foreign_key":
        return _validate_foreign_key(catalog, obj)
    elif obj["type"] == "schema":
        return _validate_schema(catalog, obj)
    elif obj["type"] == "table":
        return _validate_table(catalog, obj)
    elif obj["type"] == "column":
        return _validate_column(catalog, obj)
    else:
        return ["unknown type '{}'".format(obj["type"])]


def _validate_foreign_key(catalog: Catalog, obj: dict) -> Sequence[str]:
    errors = []
    if "source" not in obj:
        errors.append("no 'source' field in foreign_key")
    else:
        errors += _validate_column_stanza(catalog, "source", obj["source"])
    if "target" not in obj:
        errors.append("no 'target' field in foreign_key")
    else:
        errors += _validate_column_stanza(catalog, "target", obj["target"])
    return errors


def _validate_column_stanza(catalog: Catalog, stanza_name: str, stanza: dict) -> Sequence[str]:
    errors = []
    if "database" not in stanza:
        errors.append("no 'database' field in '{}'".format(stanza_name))
    if "schema" not in stanza:
        errors.append("no 'schema' field in '{}'".format(stanza_name))
    if "table" not in stanza:
        errors.append("no 'table' field in '{}'".format(stanza_name))
    if "column" not in stanza:
        errors.append("no 'column' field in '{}'".format(stanza_name))
    if errors:
        return errors
    try:
        catalog.get_column(
            stanza["database"],
            stanza["schema"],
            stanza["table"],
            stanza["column"],
        )
    except sqlalchemy.orm.exc.NoResultFound as e:
        errors.append("'{}' column <{}, {}, {}, {}> does not exist".format(
            stanza_name,
            stanza["database"],
            stanza["schema"],
            stanza["table"],
            stanza["column"],
        ))
    return errors


def _validate_schema(catalog: Catalog, obj: dict) -> Sequence[str]:
    errors = []
    if "database" not in obj:
        errors.append("no 'database' field")
    if "schema" not in obj:
        errors.append("no 'schema' field")
    if errors:
        return errors
    try:
        catalog.get_schema(obj["database"], obj["schema"])
        return ["schema <{}, {}> is already defined".format(obj["database"], obj["schema"])]
    except sqlalchemy.orm.exc.NoResultFound:
        return []


def _validate_table(catalog: Catalog, obj: dict) -> Sequence[str]:
    errors = []
    if "database" not in obj:
        errors.append("no 'database' field")
    if "schema" not in obj:
        errors.append("no 'schema' field")
    if "table" not in obj:
        errors.append("no 'table' field")
    if errors:
        return errors
    try:
        catalog.get_table(obj["database"], obj["schema"], obj["table"])
        return ["table <{}, {}, {}> is already defined".format(obj["database"], obj["schema"], obj["table"])]
    except sqlalchemy.orm.exc.NoResultFound:
        return []


def _validate_column(catalog: Catalog, obj: dict) -> Sequence[str]:
    errors = []
    if "database" not in obj:
        errors.append("no 'database' field")
    if "schema" not in obj:
        errors.append("no 'schema' field")
    if "table" not in obj:
        errors.append("no 'table' field")
    if "column" not in obj:
        errors.append("no 'column' field")
    if "data_type" not in obj:
        errors.append("no 'data_type' field")
    if "sort_order" not in obj:
        errors.append("no 'sort_order' field")
    if errors:
        return errors
    try:
        catalog.get_column(obj["database"], obj["schema"], obj["table"], obj["column"])
        return ["column <{}, {}, {}, {}> is already defined".format(
            obj["database"], obj["schema"], obj["table"], obj["column"]
        )]
    except sqlalchemy.orm.exc.NoResultFound:
        return []


def consume_import_obj(catalog: Catalog, obj: dict):
    """Obj is expected to be validated first."""
    if obj["type"] == "foreign_key":
        source_column = catalog.get_column(
            obj["source"]["database"],
            obj["source"]["schema"],
            obj["source"]["table"],
            obj["source"]["column"],
        )
        target_column = catalog.get_column(
            obj["target"]["database"],
            obj["target"]["schema"],
            obj["target"]["table"],
            obj["target"]["column"],
        )
        action = Action(obj.get("action", str(Action.add)))
        if action == Action.add:
            catalog.add_foreign_key(source_column, target_column)
        elif action == Action.remove:
            catalog.remove_foreign_key(source_column, target_column)
    elif obj["type"] == "schema":
        source = catalog.get_source(obj["database"])
        catalog.add_schema(obj["schema"], source)
    elif obj["type"] == "table":
        schema = catalog.get_schema(obj["database"], obj["schema"])
        catalog.add_table(obj["table"], schema)
    elif obj["type"] == "column":
        table = catalog.get_table(obj["database"], obj["schema"], obj["table"])
        catalog.add_column(obj["column"], obj["data_type"], obj["sort_order"], table)
    else:
        raise ValueError("cannot determine object type")


def export_format(obj, action: Optional[Action] = None):
    """Generates an export dictionary for the represented object.

    Currently, this supports:
        foreign_keys

    """
    def attach_action(obj, action: Optional[Action]):
        if action:
            obj["action"] = action.value
        return obj

    if isinstance(obj, CatForeignKey):
        return attach_action(
            {
                "type": "foreign_key",
                "source": {
                    "database": obj.source.table.schema.source.name,
                    "schema": obj.source.table.schema.name,
                    "table": obj.source.table.name,
                    "column": obj.source.name,
                },
                "target": {
                    "database": obj.target.table.schema.source.name,
                    "schema": obj.target.table.schema.name,
                    "table": obj.target.table.name,
                    "column": obj.target.name,
                },
            },
            action
        )
    else:
        raise NotImplementedError(
            "the object type {} has not been handled yet".format(type(obj))
        )


@dataclass
class Spec:
    kind: str
    source: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None


def parse_spec(query_string: str, default: Optional[str] = None) -> Spec:
    """Reads a component description.

    Component descriptions are used on the command line to replace a
    series of --source, --schema, --table, --column options with a
    single option.

    A component description looks like this:

    `source[:schema[:table[:column]]]`

    The spec returned looks like:
    ```
    Spec(
       kind="source"|"schema"|"table"|"column",
       source=source|None,
       schema=schema|None,
       table=table|None,
       column=column|None,
    )
    ```

    A component description may omit components. So, this description:

    `foo::bar`

    Is a table specification (`spec["kind"] == "table"`) because it has
    three components, and the schema component is `None` (`spec["schema"] is None`).

    The full spec for `foo::bar` would be:
    ```
    Spec(
       kind=table,
       source="foo",
       schema=None,
       table="bar",
       column=None,
    )
    ```

    """
    parts = query_string.split(":")
    if len(parts) > 4:
        raise ValueError("too many entity components")
    entity_names = ["source", "schema", "table", "column"]
    spec = {"kind": entity_names[len(parts)-1]}
    for entity, value in itertools.zip_longest(entity_names, parts, fillvalue=default):
        spec[entity] = value or default
    return Spec(**spec)
