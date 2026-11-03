#!/bin/sh
# Opens a URL in a Chrome profile, used as BROWSER for `databricks auth login`.
# Default "Profile 1" (francisvalbuen); override with CHROME_PROFILE, e.g. CHROME_PROFILE="Profile 3" (fvalbdbxtest1).
exec open -na "Google Chrome" --args --profile-directory="${CHROME_PROFILE:-Profile 1}" "$1"
