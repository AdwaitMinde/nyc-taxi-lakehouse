# Deploying to Streamlit Community Cloud

## Prerequisites

- This `dashboard/` folder pushed to GitHub (already done, it's part of
  the main repo)
- A Streamlit Community Cloud account (sign in with GitHub at
  share.streamlit.io)
- Your SQL warehouse's Server Hostname + HTTP Path, and a personal access
  token (same 3 values from local setup - see `dashboard/README.md`)

## Deploy steps

1. Go to share.streamlit.io, click "Create app"
2. Choose "Yup, I have an app", select this repo
3. Branch: `main`
4. Main file path: `dashboard/app.py` (not just `app.py` - it needs the
   subfolder path since that's where it actually lives in the repo)
5. Click "Advanced settings" before deploying
6. In the **Secrets** field, paste:

```toml
DATABRICKS_SERVER_HOSTNAME = "dbc-xxxxxxxx-xxxx.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"
DATABRICKS_TOKEN = "dapi..."
```

7. Click Deploy

No code changes were needed for this - root-level keys in Community
Cloud's secrets are automatically exposed as environment variables, and
`app.py` already reads `os.environ[...]`, so the same code that runs
locally runs here unmodified.

## Two cold-start layers, not one

This setup has two separate things that can go to sleep, and it's worth
understanding both rather than being confused later if the dashboard looks
slow or broken to a visitor.

**1. The Databricks SQL warehouse itself.** Auto-stops after ~10 minutes
idle (this is the default, not something we configured). When it's
stopped, the next query triggers a restart that can take a few minutes.
This is the cold start the in-app spinner message now explains directly to
whoever's looking at the dashboard.

**2. The Streamlit app's own hosting.** Independently, Community Cloud
puts apps with no traffic for 12 hours to sleep. A sleeping app shows
visitors a generic Streamlit "wake this app up" screen - before they even
see your dashboard's spinner message, because the app process itself isn't
running yet. Anyone with view access can click the wake button, not just
you.

In practice: if nobody's looked at the dashboard in over 12 hours, a
visitor sees the Streamlit sleep screen first, clicks to wake it, the app
boots, then immediately hits the Databricks warehouse cold start, so the
in-app spinner kicks in too. Worst case is two waits stacked, maybe 3-5
minutes total before real data shows up.

**Why not eliminate this entirely:** keeping a SQL warehouse permanently
running and pinging the deployed app every few hours to prevent sleep
would solve it, but the warehouse approach adds standing infrastructure to
babysit, and the app-pinging approach is fighting a platform default
rather than working with it. For a portfolio piece, the honest move is the
in-app message explaining the wait rather than a workaround that adds
fragility (or doesn't even help, since the 12-hour app sleep is the
platform's call, not something configurable per-app).

## If you want to minimize this in practice

Before sharing the link (interview, job application, etc.), visit it
yourself a few minutes ahead of time. That wakes the app and warms the
warehouse, so whoever clicks the link next gets a fast load instead of the
cold-start path.
