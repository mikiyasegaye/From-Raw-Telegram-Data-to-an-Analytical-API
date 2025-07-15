#!/usr/bin/env python3
"""
Script to run Dagster UI and manage the Ethiopian Medical Analytics Pipeline.

This script provides an easy way to launch the Dagster UI and manage pipeline operations.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

def run_dagster_ui(host="0.0.0.0", port=3000):
    """Launch the Dagster UI."""
    print(f"🚀 Launching Dagster UI on http://{host}:{port}")
    print("📊 Access the UI to monitor and manage your pipeline")
    print("⏹️  Press Ctrl+C to stop the UI")
    
    try:
        # Change to project root
        project_root = Path(__file__).parent.parent
        os.chdir(project_root)
        
        # Launch Dagster UI
        subprocess.run([
            "dagster", "dev",
            "--host", host,
            "--port", str(port)
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dagster UI stopped")
    except Exception as e:
        print(f"❌ Error launching Dagster UI: {e}")
        sys.exit(1)

def run_pipeline_job(job_name, config=None):
    """Run a specific pipeline job."""
    print(f"🔧 Running pipeline job: {job_name}")
    
    try:
        # Change to project root
        project_root = Path(__file__).parent.parent
        os.chdir(project_root)
        
        # Build command
        cmd = ["dagster", "job", "execute", "--job", job_name]
        if config:
            cmd.extend(["--config", config])
        
        # Run job
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Job {job_name} completed successfully")
            print(result.stdout)
        else:
            print(f"❌ Job {job_name} failed")
            print(result.stderr)
            
    except Exception as e:
        print(f"❌ Error running job {job_name}: {e}")
        sys.exit(1)

def list_jobs():
    """List available jobs."""
    print("📋 Available Pipeline Jobs:")
    print()
    print("1. ethiopian_medical_analytics_pipeline - Complete pipeline")
    print("2. quick_test_pipeline - Quick test with minimal data")
    print("3. data_pipeline_only - Data pipeline without YOLO")
    print("4. scraping_job - Data scraping only")
    print("5. transformation_job - dbt transformations only")
    print("6. quality_checks_job - Data quality checks only")
    print()

def main():
    parser = argparse.ArgumentParser(
        description="Dagster Pipeline Manager for Ethiopian Medical Analytics"
    )
    parser.add_argument(
        "command",
        choices=["ui", "run", "list"],
        help="Command to execute"
    )
    parser.add_argument(
        "--job",
        help="Job name to run (when using 'run' command)"
    )
    parser.add_argument(
        "--config",
        help="Config file for job execution"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host for Dagster UI (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Port for Dagster UI (default: 3000)"
    )
    
    args = parser.parse_args()
    
    if args.command == "ui":
        run_dagster_ui(args.host, args.port)
    elif args.command == "run":
        if not args.job:
            print("❌ Please specify a job name with --job")
            list_jobs()
            sys.exit(1)
        run_pipeline_job(args.job, args.config)
    elif args.command == "list":
        list_jobs()

if __name__ == "__main__":
    main() 