"""Command-line interface for NAM to Zarr conversion."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from nam_to_zarr.noaa.nam_conus.pressure_levels import NamConusPressureLevelsDataset

app = typer.Typer(
    help="NAM to Zarr - NOAA North American Mesoscale pressure level data reformatter",
    no_args_is_help=True,
)
console = Console()

# Dataset registry
DATASETS: dict[str, type] = {
    "noaa-nam-conus-pressure-levels": NamConusPressureLevelsDataset,
}


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, console=console)],
    )


@app.command()
def list_datasets() -> None:
    """List all available datasets."""
    table = Table(title="Available Datasets")
    table.add_column("ID", style="cyan")
    table.add_column("Description", style="green")

    for dataset_id, dataset_class in DATASETS.items():
        instance = dataset_class()
        attrs = instance.template_config.dataset_attributes
        table.add_row(dataset_id, attrs.description)

    console.print(table)


@app.command()
def operational_update(
    dataset_id: Annotated[
        str,
        typer.Option(help="Dataset ID (use 'list-datasets' to see available options)"),
    ] = "noaa-nam-conus-pressure-levels",
    output_dir: Annotated[
        Path,
        typer.Option(help="Output directory for Zarr store"),
    ] = Path("./data"),
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable verbose logging"),
    ] = False,
) -> None:
    """Run operational update for a dataset."""
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    if dataset_id not in DATASETS:
        console.print(f"[red]Error: Unknown dataset ID '{dataset_id}'[/red]")
        console.print("Use 'list-datasets' to see available options")
        raise typer.Exit(1)

    try:
        logger.info(f"Starting operational update for {dataset_id}")
        dataset_class = DATASETS[dataset_id]
        dataset = dataset_class()

        # Run the update
        dataset.operational_update(output_dir=output_dir)

        logger.info("Update completed successfully")

    except Exception as e:
        logger.exception(f"Update failed: {e}")
        raise typer.Exit(1) from e


@app.command()
def info(
    dataset_id: Annotated[
        str,
        typer.Option(help="Dataset ID (use 'list-datasets' to see available options)"),
    ] = "noaa-nam-conus-pressure-levels",
) -> None:
    """Display detailed information about a dataset."""
    if dataset_id not in DATASETS:
        console.print(f"[red]Error: Unknown dataset ID '{dataset_id}'[/red]")
        console.print("Use 'list-datasets' to see available options")
        raise typer.Exit(1)

    dataset_class = DATASETS[dataset_id]
    dataset = dataset_class()
    config = dataset.template_config

    console.print(f"\n[bold cyan]{config.dataset_attributes.title}[/bold cyan]")
    console.print(f"\n{config.dataset_attributes.description}\n")

    # Display metadata
    table = Table(title="Dataset Metadata", show_header=False)
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")

    attrs = config.dataset_attributes
    table.add_row("Provider", attrs.provider)
    table.add_row("Model", attrs.model)
    table.add_row("Variant", attrs.variant)
    table.add_row("Version", attrs.version)

    console.print(table)


if __name__ == "__main__":
    app()
