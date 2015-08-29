#!/usr/bin/env python

dry_run        = False
verbose        = False
daemonize      = True
sleep          = 30

# Run a full sync on startup
initial_full_sync   = True

# Run a full sync on intervals (0 to disable)
full_sync_interval  = 3600

# Master host
master         = 'front1'

# Rsync options
rsync_cmd      = 'rsync'
rsync_user     = 'ackstorm-sync'
rsync_password = '******'
rsync_updates  = 'updates'
rsync_opts     = ["-av","-r","--delete","--timeout=20","--force","--ignore-errors"]

# Write this file when sync is done
end_sync_file  = '/tmp/sync-client.done'

# Actions
actions        = [
    {'/etc/fstab': 'mount -a'},
    {'/etc/nginx/*': 'service nginx restart'},
    {'/usr/local/ackstorm/sync/etc/*.py': '/usr/local/ackstorm/sync/bin/ackstorm-sync restart'},
    {'/usr/local/ackstorm/sync/bin/*.py': '/usr/local/ackstorm/sync/bin/ackstorm-sync restart'}
]

