from pathlib import Path
from typing import Optional

import typer

from dbcat import __version__
from dbcat.api import OutputFormat
from dbcat.app_state import app_state
from dbcat.cli import app as catalog_app

app = typer.Typer()


def version_callback(value: bool):
    if value:
        print("{}".format(__version__))
        typer.Exit()


@app.callback()
def cli(
    log_level: str = typer.Option("WARNING", help="Logging Level"),
    output_format: OutputFormat = typer.Option(
        OutputFormat.tabular, case_sensitive=False
    ),
    catalog_path: Path = typer.Option(
        None, help="Path to store catalog state. Use if NOT using a database"
    ),
    catalog_host: str = typer.Option(
        None, help="hostname of Postgres database. Use if catalog is a database."
    ),
    catalog_port: int = typer.Option(
        None, help="port of Postgres database. Use if catalog is a database."
    ),
    catalog_user: str = typer.Option(
        None, help="user of Postgres database. Use if catalog is a database."
    ),
    catalog_password: str = typer.Option(
        None, help="password of Postgres database. Use if catalog is a database."
    ),
    catalog_database: str = typer.Option(
        None, help="database of Postgres database. Use if catalog is a database."
    ),
    version: Optional[bool] = typer.Option(
        None, "--version", callback=version_callback
    ),
):
    #    logging.basicConfig(level=getattr(logging, log_level.upper()))

    if catalog_path is None:
        app_dir = typer.get_app_dir("dbcat")
        app_dir_path = Path(app_dir)
        app_dir_path.mkdir(parents=True, exist_ok=True)
        catalog_path = Path(app_dir) / "catalog.db"

    app_state["catalog_connection"] = {
        "catalog_path": str(catalog_path),
        "catalog_user": catalog_user,
        "catalog_password": catalog_password,
        "catalog_host": catalog_host,
        "catalog_port": catalog_port,
        "catalog_database": catalog_database,
    }
    app_state["output_format"] = output_format


app.add_typer(catalog_app, name="catalog")
