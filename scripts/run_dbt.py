#!/usr/bin/env python3
"""
Script to run dbt commands for the Telegram Analytics project.
This script manages the data transformation pipeline.
"""

import os
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DbtRunner:
    def __init__(self):
        self.dbt_project_dir = Path("dbt")
        self.profiles_dir = Path("dbt")

    def run_command(self, command: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a dbt command."""
        cmd = f"cd {self.dbt_project_dir} && dbt {command}"
        logger.info(f"Running: {cmd}")

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True
        )

        if result.returncode != 0 and check:
            logger.error(f"Command failed: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, cmd)

        return result

    def deps(self):
        """Install dbt dependencies."""
        logger.info("Installing dbt dependencies...")
        result = self.run_command("deps")
        logger.info("Dependencies installed successfully")
        return result

    def debug(self):
        """Run dbt debug to check configuration."""
        logger.info("Running dbt debug...")
        result = self.run_command("debug")
        logger.info("Debug completed")
        return result

    def run(self, models: str = None, full_refresh: bool = False):
        """Run dbt models."""
        cmd = "run"
        if models:
            cmd += f" --models {models}"
        if full_refresh:
            cmd += " --full-refresh"

        logger.info(f"Running dbt models: {cmd}")
        result = self.run_command(cmd)
        logger.info("Models run successfully")
        return result

    def test(self, models: str = None):
        """Run dbt tests."""
        cmd = "test"
        if models:
            cmd += f" --models {models}"

        logger.info(f"Running dbt tests: {cmd}")
        result = self.run_command(cmd)
        logger.info("Tests completed successfully")
        return result

    def docs_generate(self):
        """Generate dbt documentation."""
        logger.info("Generating dbt documentation...")
        result = self.run_command("docs generate")
        logger.info("Documentation generated successfully")
        return result

    def docs_serve(self):
        """Serve dbt documentation."""
        logger.info("Starting dbt docs server...")
        result = self.run_command("docs serve", check=False)
        return result

    def seed(self):
        """Run dbt seeds."""
        logger.info("Running dbt seeds...")
        result = self.run_command("seed")
        logger.info("Seeds completed successfully")
        return result

    def snapshot(self):
        """Run dbt snapshots."""
        logger.info("Running dbt snapshots...")
        result = self.run_command("snapshot")
        logger.info("Snapshots completed successfully")
        return result

    def clean(self):
        """Clean dbt artifacts."""
        logger.info("Cleaning dbt artifacts...")
        result = self.run_command("clean")
        logger.info("Clean completed")
        return result

    def run_full_pipeline(self):
        """Run the complete dbt pipeline."""
        logger.info("Starting full dbt pipeline...")

        try:
            # Install dependencies
            self.deps()

            # Debug configuration
            self.debug()

            # Run models
            self.run()

            # Run tests
            self.test()

            # Generate documentation
            self.docs_generate()

            logger.info("Full dbt pipeline completed successfully!")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run dbt commands for Telegram Analytics")
    parser.add_argument("command", choices=[
        "deps", "debug", "run", "test", "docs-generate", "docs-serve",
        "seed", "snapshot", "clean", "full-pipeline"
    ], help="dbt command to run")
    parser.add_argument("--models", help="Specific models to run/test")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Full refresh for run command")

    args = parser.parse_args()

    runner = DbtRunner()

    if args.command == "deps":
        runner.deps()
    elif args.command == "debug":
        runner.debug()
    elif args.command == "run":
        runner.run(models=args.models, full_refresh=args.full_refresh)
    elif args.command == "test":
        runner.test(models=args.models)
    elif args.command == "docs-generate":
        runner.docs_generate()
    elif args.command == "docs-serve":
        runner.docs_serve()
    elif args.command == "seed":
        runner.seed()
    elif args.command == "snapshot":
        runner.snapshot()
    elif args.command == "clean":
        runner.clean()
    elif args.command == "full-pipeline":
        runner.run_full_pipeline()


if __name__ == "__main__":
    main()
