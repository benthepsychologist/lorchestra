"""
CLI interface for lorch pipeline orchestrator.

Provides commands: run, status, validate, clean.
"""

from pathlib import Path

import click

from lorch import __version__
from lorch.config import load_config
from lorch.pipeline import Pipeline
from lorch.utils import format_duration, print_error, print_info, print_success


@click.group()
@click.version_option(version=__version__, prog_name="lorch")
def main():
    """
    lorch - Local Orchestrator for PHI data pipeline.

    Coordinates extract → canonize → index stages.
    """
    pass


@main.command()
@click.option(
    "--stage",
    type=click.Choice(["extract", "canonize", "index"], case_sensitive=False),
    help="Run specific stage only",
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    help="Custom configuration file (default: config/pipeline.yaml)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate configuration without executing",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable debug logging",
)
def run(stage, config, dry_run, verbose):
    """
    Run pipeline stages.

    Examples:

      # Run full pipeline
      lorch run

      # Run single stage
      lorch run --stage extract

      # Dry run (validation only)
      lorch run --dry-run

      # Verbose logging
      lorch run --verbose

      # Custom config
      lorch run --config /path/to/pipeline.yaml
    """
    try:
        # Load configuration
        pipeline_config = load_config(config) if config else load_config()
        pipeline = Pipeline(pipeline_config)

        # Determine stages to run
        stages_to_run = [stage] if stage else None

        # Run pipeline
        result = pipeline.run(
            stages=stages_to_run,
            dry_run=dry_run,
            verbose=verbose,
        )

        # Exit with appropriate code
        if result.success:
            exit(0)
        else:
            exit(1)

    except Exception as e:
        print_error(f"Pipeline failed: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        exit(1)


@main.command()
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    help="Custom configuration file",
)
def status(config):
    """
    Show pipeline status and last run information.

    Examples:

      # Show status
      lorch status

      # Status with custom config
      lorch status --config /path/to/pipeline.yaml
    """
    try:
        # Load configuration
        pipeline_config = load_config(config) if config else load_config()
        pipeline = Pipeline(pipeline_config)

        # Get last run status
        last_run = pipeline.status()

        if not last_run:
            print_info("No previous pipeline runs found")
            exit(0)

        # Display status
        print_info(f"Pipeline Status: {pipeline_config.name} v{pipeline_config.version}\n")

        status_symbol = "✅" if last_run.success else "❌"
        status_text = "SUCCESS" if last_run.success else "FAILED"

        print(f"Last Run: {last_run.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Status: {status_symbol} {status_text}")
        print(f"Duration: {format_duration(last_run.duration_seconds)}")

        if last_run.error_message:
            print(f"Error: {last_run.error_message}")

        print()

        if last_run.stages:
            print("Stages:")
            for stage_name, stage_result in last_run.stages.items():
                stage_symbol = "✅" if stage_result.success else "❌"
                duration = format_duration(stage_result.duration_seconds)
                records = stage_result.records_processed

                print(f"  {stage_symbol} {stage_name:<12} {duration:>8}  {records:>6} records")

        # Show log location
        log_file = pipeline_config.get_log_file_path()
        if log_file.exists():
            print(f"\nLogs: {log_file}")

    except Exception as e:
        print_error(f"Could not retrieve status: {e}")
        exit(1)


@main.command()
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    help="Custom configuration file",
)
@click.option(
    "--skip-permissions",
    is_flag=True,
    help="Skip PHI directory permission checks",
)
def validate(config, skip_permissions):
    """
    Validate pipeline configuration and dependencies.

    Checks:
    - Configuration file syntax
    - Component paths exist
    - Transform registry accessible
    - Output directories writable
    - PHI directory permissions

    Examples:

      # Validate default config
      lorch validate

      # Validate custom config
      lorch validate --config /path/to/pipeline.yaml

      # Skip permission checks
      lorch validate --skip-permissions
    """
    try:
        # Load configuration
        pipeline_config = load_config(config) if config else load_config()
        pipeline = Pipeline(pipeline_config)

        # Validate
        pipeline.validate(skip_permissions=skip_permissions)

        print_success("✓ Pipeline configuration is valid")
        exit(0)

    except Exception as e:
        print_error(f"Validation failed: {e}")
        exit(1)


@main.command()
@click.option(
    "--stage",
    type=click.Choice(["extract", "canonize", "index"], case_sensitive=False),
    help="Clean specific stage output only",
)
@click.option(
    "--all",
    "clean_all",
    is_flag=True,
    help="Clean all stage outputs (use with caution)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be deleted without deleting",
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    help="Custom configuration file",
)
@click.confirmation_option(
    prompt="Are you sure you want to delete pipeline outputs? This cannot be undone.",
)
def clean(stage, clean_all, dry_run, config):
    """
    Clean stage outputs.

    WARNING: This permanently deletes output files.

    Examples:

      # Clean specific stage
      lorch clean --stage extract

      # Clean all stages (prompts for confirmation)
      lorch clean --all

      # Dry run (show what would be deleted)
      lorch clean --all --dry-run
    """
    try:
        import shutil

        # Load configuration
        pipeline_config = load_config(config) if config else load_config()

        # Determine stages to clean
        if stage:
            stages_to_clean = [stage]
        elif clean_all:
            stages_to_clean = ["extract", "canonize", "index"]
        else:
            print_error("Must specify --stage or --all")
            exit(1)

        # Clean each stage
        for stage_name in stages_to_clean:
            stage_config = pipeline_config.get_stage(stage_name)

            if not stage_config:
                print_error(f"Stage not found: {stage_name}")
                continue

            output_dir = stage_config.output_dir

            if not output_dir.exists():
                print_info(f"{stage_name}: output directory does not exist ({output_dir})")
                continue

            # Count files
            files = list(output_dir.glob("*"))

            if not files:
                print_info(f"{stage_name}: no files to clean")
                continue

            if dry_run:
                print_info(f"{stage_name}: would delete {len(files)} files from {output_dir}")
                for f in files[:10]:  # Show first 10
                    print(f"  - {f.name}")
                if len(files) > 10:
                    print(f"  ... and {len(files) - 10} more")
            else:
                print_info(f"{stage_name}: deleting {len(files)} files from {output_dir}")

                for f in files:
                    try:
                        if f.is_dir():
                            shutil.rmtree(f)
                        else:
                            f.unlink()
                    except Exception as e:
                        print_error(f"Could not delete {f.name}: {e}")

                print_success(f"{stage_name}: cleaned {len(files)} files")

        exit(0)

    except Exception as e:
        print_error(f"Clean failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
