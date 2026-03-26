#!/bin/bash

# Script to fix PR/issue links in release notes from sdk-core to target repo
# Usage: ./fix_commit_links.sh <target_repo_url> [input_file]
# If no input_file is provided, reads from stdin

if [ $# -lt 1 ]; then
    echo "Usage: $0 <target_repo_url> [input_file]"
    echo "Example: $0 https://github.com/temporalio/sdk-python"
    echo "         $0 https://github.com/temporalio/sdk-python release_notes.txt"
    exit 1
fi

TARGET_REPO_URL="$1"
INPUT_FILE="$2"

# Remove trailing slash from URL if present
TARGET_REPO_URL=$(echo "$TARGET_REPO_URL" | sed 's|/$||')

# Function to process the input
process_commits() {
    # Replace PR/issue references with correct repository links
    # Pattern: (#1234) -> (target_repo_url/pull/1234) or (target_repo_url/issues/1234)
    # Using /pull/ as it works for both PRs and issues on GitHub
    sed -E "s|\(#([0-9]+)\)|([${TARGET_REPO_URL}/pull/\1])|g"
}

if [ -n "$INPUT_FILE" ]; then
    # Read from file
    if [ ! -f "$INPUT_FILE" ]; then
        echo "Error: File '$INPUT_FILE' not found"
        exit 1
    fi
    cat "$INPUT_FILE" | process_commits
else
    # Read from stdin
    process_commits
fi