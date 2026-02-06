#!/usr/bin/env python3
"""
Script to generate Pandadoc connector files export.
"""
import os
from pathlib import Path

def create_pandadoc_export():
    """Create Pandadoc connector files export."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent  # This gets to /tools/generate_scripts/
    markdown_outputs_dir = script_dir.parent / "markdown_outputs"  # This gets to /tools/markdown_outputs/
    pandadoc_path = script_dir.parent.parent / "connectors" / "pandadoc"
    output_file = markdown_outputs_dir / "pandadoc_connector_files.md"

    # Ensure the output directory exists
    markdown_outputs_dir.mkdir(exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as md_file:
        md_file.write("# Pandadoc Connector Files Export\n\n")
        md_file.write("This document contains all files from the Pandadoc connector directory.\n\n")

        # Get all Python files and sort them
        py_files = sorted([f for f in pandadoc_path.rglob("*.py")])

        for file_path in py_files:
            relative_path = file_path.relative_to(pandadoc_path)

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

    print(f"Pandadoc connector files exported to {output_file}")

if __name__ == "__main__":
    create_pandadoc_export()