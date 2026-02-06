#!/usr/bin/env python3
"""
Script to generate Orchestrator files export.
"""
import os
from pathlib import Path

def create_orchestrator_export():
    """Create Orchestrator files export."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent  # This gets to /tools/generate_scripts/
    markdown_outputs_dir = script_dir.parent / "markdown_outputs"  # This gets to /tools/markdown_outputs/
    orchestrator_path = script_dir.parent.parent / "orchestrator"  # This gets to project root /orchestrator
    output_file = markdown_outputs_dir / "orchestrator_files.md"

    # Ensure the output directory exists
    markdown_outputs_dir.mkdir(exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as md_file:
        md_file.write("# Orchestrator Files Export\n\n")
        md_file.write("This document contains all files from the Orchestrator directory.\n\n")

        # Get all Python files and sort them
        py_files = sorted([f for f in orchestrator_path.rglob("*.py")])

        for file_path in py_files:
            relative_path = file_path.relative_to(orchestrator_path)

            # Read file content
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except Exception as e:
                content = f"[Could not read file: {e}]"

            # Write to markdown file
            md_file.write(f"## File: {relative_path}\n\n")
            md_file.write("```python\n")
            md_file.write(f"{content}\n")
            md_file.write("```\n\n")

    print(f"Orchestrator files exported to {output_file}")

if __name__ == "__main__":
    create_orchestrator_export()