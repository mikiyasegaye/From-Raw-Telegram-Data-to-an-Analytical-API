#!/usr/bin/env python3
"""
Test script to validate dbt project setup and configuration.
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


class DbtSetupTester:
    def __init__(self):
        self.dbt_project_dir = Path("dbt")
        self.scripts_dir = Path("scripts")

    def test_project_structure(self):
        """Test that the dbt project structure is correct."""
        logger.info("Testing dbt project structure...")

        required_files = [
            "dbt_project.yml",
            "profiles.yml",
            "models/staging/stg_telegram_messages.sql",
            "models/staging/_sources.yml",
            "models/marts/core/dim_channels.sql",
            "models/marts/core/dim_dates.sql",
            "models/marts/core/fct_messages.sql",
            "models/marts/core/_schema.yml",
            "tests/test_medical_content_consistency.sql",
            "tests/test_message_engagement_consistency.sql",
            "macros/medical_content_detection.sql"
        ]

        missing_files = []
        for file_path in required_files:
            full_path = self.dbt_project_dir / file_path
            if not full_path.exists():
                missing_files.append(file_path)

        if missing_files:
            logger.error(f"Missing required files: {missing_files}")
            return False
        else:
            logger.info("✓ All required dbt files present")
            return True

    def test_scripts(self):
        """Test that required scripts are present."""
        logger.info("Testing script files...")

        required_scripts = [
            "scripts/load_raw_data.py",
            "scripts/run_dbt.py"
        ]

        missing_scripts = []
        for script_path in required_scripts:
            if not Path(script_path).exists():
                missing_scripts.append(script_path)

        if missing_scripts:
            logger.error(f"Missing required scripts: {missing_scripts}")
            return False
        else:
            logger.info("✓ All required scripts present")
            return True

    def test_dbt_installation(self):
        """Test that dbt is installed and accessible."""
        logger.info("Testing dbt installation...")

        try:
            result = subprocess.run(
                ["dbt", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                logger.info(f"✓ dbt installed: {result.stdout.strip()}")
                return True
            else:
                logger.error(f"dbt command failed: {result.stderr}")
                return False

        except FileNotFoundError:
            logger.error("dbt command not found. Please install dbt-postgres")
            return False
        except subprocess.TimeoutExpired:
            logger.error("dbt command timed out")
            return False

    def test_dbt_debug(self):
        """Test dbt debug command."""
        logger.info("Testing dbt debug...")

        try:
            result = subprocess.run(
                ["cd dbt && dbt debug"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                logger.info("✓ dbt debug successful")
                return True
            else:
                logger.error(f"dbt debug failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("dbt debug timed out")
            return False

    def test_dbt_compile(self):
        """Test dbt compile command."""
        logger.info("Testing dbt compile...")

        try:
            result = subprocess.run(
                ["cd dbt && dbt compile"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                logger.info("✓ dbt compile successful")
                return True
            else:
                logger.error(f"dbt compile failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("dbt compile timed out")
            return False

    def test_environment_variables(self):
        """Test that required environment variables are set."""
        logger.info("Testing environment variables...")

        required_vars = [
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
            "POSTGRES_PORT"
        ]

        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            logger.warning(f"Missing environment variables: {missing_vars}")
            logger.info("Using default values from profiles.yml")
        else:
            logger.info("✓ All environment variables set")

        return True

    def run_all_tests(self):
        """Run all tests and return results."""
        logger.info("Starting dbt setup validation...")

        tests = [
            ("Project Structure", self.test_project_structure),
            ("Scripts", self.test_scripts),
            ("dbt Installation", self.test_dbt_installation),
            ("Environment Variables", self.test_environment_variables),
            ("dbt Debug", self.test_dbt_debug),
            ("dbt Compile", self.test_dbt_compile)
        ]

        results = []
        for test_name, test_func in tests:
            try:
                result = test_func()
                results.append((test_name, result))
            except Exception as e:
                logger.error(f"Test '{test_name}' failed with exception: {e}")
                results.append((test_name, False))

        # Print summary
        logger.info("\n" + "="*50)
        logger.info("DBT SETUP VALIDATION RESULTS")
        logger.info("="*50)

        passed = 0
        total = len(results)

        for test_name, result in results:
            status = "✓ PASS" if result else "✗ FAIL"
            logger.info(f"{test_name:<25} {status}")
            if result:
                passed += 1

        logger.info("="*50)
        logger.info(f"Overall: {passed}/{total} tests passed")

        if passed == total:
            logger.info("🎉 All tests passed! dbt setup is ready.")
        else:
            logger.warning(
                "⚠️  Some tests failed. Please check the issues above.")

        return passed == total


def main():
    """Main function."""
    tester = DbtSetupTester()
    success = tester.run_all_tests()

    if success:
        print("\nNext steps:")
        print("1. Load raw data: python scripts/load_raw_data.py")
        print("2. Run dbt models: python scripts/run_dbt.py run")
        print("3. Run tests: python scripts/run_dbt.py test")
        print("4. Generate docs: python scripts/run_dbt.py docs-generate")
    else:
        print("\nPlease fix the issues above before proceeding.")


if __name__ == "__main__":
    main()
