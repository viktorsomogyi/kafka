#!/bin/bash

pr_description="This is a pull request description"
new_reviewer="new_reviewer"
# Check if the pull request description's last line starts with "Reviewers: "
last_line=$(echo "pr_description" | awk '{print $NF}')
if [[ $pr_description =~ ^.*Reviewers:\ .*$ ]]; then
  # Append the reviewer to the last line
  pr_description="${pr_description}, $new_reviewer"
  echo "pr_description: $pr_description"
else
    # Add the reviewer to this new line
    pr_description="${pr_description}\n\nReviewers: $new_reviewer"
    echo "pr_description: $pr_description"
fi