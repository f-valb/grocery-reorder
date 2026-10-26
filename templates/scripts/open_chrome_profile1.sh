#!/bin/sh
# Opens a URL in Chrome "Profile 1" (francisvalbuen). Used as BROWSER for `databricks auth login`.
exec open -na "Google Chrome" --args --profile-directory="Profile 1" "$1"
