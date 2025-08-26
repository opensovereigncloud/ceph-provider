#!/usr/bin/env bash

# -------------------------------------------------------------------
# Purpose:
#   This script ensures that a GitLab issue is created as a reminder
#   whenever the latest commit from the upstream branch (e.g., "main")
#   is not yet present in the downstream branch (e.g., "osc/main").
#
#   It works as follows:
#     1. Checks if the latest commit from the upstream branch exists
#        in the downstream branch.
#     2. If already synced → exits with success, nothing to do.
#     3. If not synced → checks if an open reminder issue already exists.
#     4. If no issue exists → creates a new GitLab issue with a fixed
#        title, description, assignees, and labels.
# -------------------------------------------------------------------

set -euo pipefail

declare -r projectID=238355
declare -r upstreamBranch="main"
declare -r downstreamBranch="osc/main"
declare -r syncMessage="sync ${downstreamBranch} with ${upstreamBranch}"
declare -r issueTitle="[reminder] ${syncMessage}"
declare -r description="Please ${syncMessage}."
declare -r asigneeIDs="26108,30509,40056,41270"
declare -r labels="stream:infra-development"

getLatestMainCommit() {
    git rev-parse "origin/${upstreamBranch}"
}

isCommitInOSCMain() {
    local commitHash=$1
    git merge-base --is-ancestor "${commitHash}" "origin/${downstreamBranch}"
}

issueExists() {
    gitlab -o json project-issue list \
        --project-id "${projectID}" \
        --state opened \
        --get-all 2>/dev/null \
    | jq -e --arg title "${issueTitle}" '.[] | select(.title == $title)' >/dev/null
}

createIssue() {
    gitlab project-issue create \
        --project-id "${projectID}" \
        --title "${issueTitle}" \
        --description "${description}" \
        --assignee-ids "${asigneeIDs}" \
        --labels "${labels}" \
        >/dev/null 2>&1
}

git config user.email "noreply@gitlab.devops.telekom.de"
git config user.name "sync user"

if isCommitInOSCMain "$(getLatestMainCommit)"; then
    echo "Latest commit of ${upstreamBranch} is already in ${downstreamBranch}; nothing to do."
    exit 0
fi

if issueExists; then
    echo "Issue already exists; nothing to do."
else
    createIssue
    echo "Issue created."
fi
