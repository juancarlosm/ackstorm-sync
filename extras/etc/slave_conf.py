#!/usr/bin/env python

dry_run        = False
verbose        = True
daemonize      = True

# Time to sleep between inotify syncs
sleep          = 5

# Run a full sync on startup
initial_fullsync   = True

# Run a full sync on intervals (0 to disable)
# Default is 3600*4 (4 hours)
fullsync_interval  = 3600

# Master host
master         = 'front1'

# Rsync options
rsync_cmd      = 'rsync'
rsync_user     = 'ackstorm-sync'
rsync_password = '******'
rsync_updates  = 'updates'
rsync_opts     = ["-av","-x","-r","--delete","--timeout=20","--force","--ignore-errors"]

# Write this file when sync is done
end_sync_file  = '/tmp/sync-client.done'

# Actions (will run in detached process)
# You can run multiple actions using ";" separator but will run simultaneously
actions        = [
    {'/etc/fstab': 'mount -a'},
    {'/etc/nginx/*': 'service nginx reload'},
    {'/usr/local/ackstorm/sync/etc/*.py': '/usr/local/ackstorm/sync/bin/ackstorm-sync restart'},
    {'/usr/local/ackstorm/sync/bin/*.py': '/usr/local/ackstorm/sync/bin/ackstorm-sync restart'}
]

