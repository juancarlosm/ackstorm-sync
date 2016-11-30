#!/usr/bin/env python

import os
import sys
import signal
import logging
import fnmatch

from pyinotify import *
from time import time, sleep

from common import *

LOG_FILE = './var/log/ackstorm-sync-master.log'
CONFIG_FILE = './extras/etc/master_conf.py'
VERSION_FILE = './var/.version'

DEFAULT_EVENTS = [
    "IN_CLOSE_WRITE",
    "IN_CREATE",
    "IN_DELETE",
    "IN_MOVED_FROM",
    "IN_MOVED_TO"
]


class SyncMaster():
  class Inotify(ProcessEvent):
    def process_default(self, event):
      inotify_file = os.path.join(event.path, event.name)
      logging.debug("caught %s on %s" % \
          (event.maskname, inotify_file))
          
      # Process excludes
      for exclude in config.excludes:
        logging.debug("exclude?? %s <> %s" %(inotify_file, exclude))
        if fnmatch.fnmatch(inotify_file, exclude):
          logging.info("EXCLUDED FILE: %s" % inotify_file)
          return
        
      extra = '';
      if event.maskname.startswith('IN_DELETE') or \
        event.maskname.startswith('IN_MOVED_FROM'):
          extra = '#DELETE:'
          
      # Write changes files
      _time = str(int(time()))
      changes_file = './data/' + _time + '.inotify'
      logging.info("WRITTING CHANGES ON: %s" % changes_file)
  
      with open(changes_file, 'a') as file:
        file.write("%s%s\n" %(extra,inotify_file))

  def __init__(self):
    # Create required folders
    create_dirs()
    self.config = self.load_config()
    
  def run(self, pid_file):
    # Check and write pid
    if pid_file_check(pid_file):
      print "[ERROR] Another process is running...."
      sys.exit(1)

    pid_file_write(pid_file)
    
    # Set config as global
    global config
    config = self.config

    # Configure logging
    loglevel = logging.INFO
    if self.config.verbose: loglevel = logging.DEBUG
    logging.basicConfig(filename=LOG_FILE,level=loglevel,format='%(asctime)s %(levelname)s: %(message)s')
    
    logging.info("STARTING...")
    
    # Catch signals
    self.catch_signals()
  
    wm = WatchManager()
    ev = self.Inotify()
    
    # exclude our working dirs (var and data)
    excludes = ['^' + os.path.abspath('./var'), '^' + os.path.abspath('./data')]
    excludes = excludes + config.inotify_excludes
  
    notifier = AsyncNotifier(wm, ev, read_freq=10)
    mask = reduce(lambda x,y: x|y, [EventsCodes.ALL_FLAGS[e] for e in DEFAULT_EVENTS])
    excl = ExcludeFilter(excludes)
    wds = wm.add_watch(self.config.watch_paths, mask, rec=True, exclude_filter=excl, auto_add=True)
    
    # Check if there are out of sync files from last run
    self.check_out_of_sync(self.config.watch_paths)
    
    logging.info("Main process started")
    while True:
      try:
        notifier.process_events()
        if notifier.check_events():
          notifier.read_events()
          
        self.update_last_run(int(time()))
        sleep(self.config.sleep)
    
      except KeyboardInterrupt:
        logging.info("killed by keyboard interrupt")
        self.update_last_run(int(time()))
        notifier.stop()
        break
  
    pid_file_del(pid_file)
    self.end()
  
  def check_out_of_sync(self,paths):
    last_run = None
    if os.path.isfile(VERSION_FILE):
      with open(VERSION_FILE, 'r') as file:
        last_run = file.read()
  
    if last_run:
      newer_files=[]
      _time = str(int(time()))
      for path in paths:
        logging.info("Looking for out of sync files at: %s" % path)
        
        # Find files newer than VERSION_FILE
        _cmd = 'find ' + path + ' -type f -cnewer ' + VERSION_FILE
        out,std,err = run(shlex.split(_cmd))
        
        for line in std.split('\n'):
          if not line: continue
          
          # Process excludes
          skip_this = False
          for exclude in config.excludes:
            if fnmatch.fnmatch(line, exclude):
              logging.debug("EXCLUDED FILE: %s" % line)
              skip_this = True
              
          if skip_this: continue
          logging.info('File out of sync: %s' % line)
          newer_files.append(line)
  
      if newer_files:
          changes_file = './data/' +  str(int(time())) + '.sync';
          logging.info("Writting on: %s" % changes_file)
          with open(changes_file, 'a') as file:
            file.write("%s\n" %('\n'.join(newer_files)))
            
          self.update_last_run(_time)
          
    else:
      logging.debug("No last version found: Starting from 0")
  
  def update_last_run(self,_time):
    logging.debug("Update last run: " + str(_time))
    with open(VERSION_FILE, 'w') as file:
       file.write("%s" % _time)
       
  def catch_signals(self):
    signal.signal(signal.SIGTERM, self.end)
    signal.signal(signal.SIGINT,  self.end)
    
  def load_config(self):
    if not os.path.isfile(CONFIG_FILE):
      raise RuntimeError, "Configuration file does not exist: %s" % CONFIG_FILE
  
    configdir  = os.path.dirname(CONFIG_FILE)
    configfile = os.path.basename(CONFIG_FILE)
  
    if configfile.endswith(".py"):
      configfile = configfile[0:-3]
  
    else:
      raise RuntimeError, "Configuration file must be a importable python file ending in .py"
  
    sys.path.append(configdir)
    exec("import %s as __config__" % configfile)
    sys.path.remove(configdir)
  
    config = __config__
    
    if not "verbose" in dir(config):
      config.verbose = False
      
    if not "daemonize" in dir(config):
      config.daemonize = True
      
    if not "excludes" in dir(config):
      config.excludes = []
      
    if not "actions" in dir(config):
      config.actions = []
      
    if not "sleep" in dir(config):
      config.sleep = 5

    config.sleep = int(config.sleep)
    if config.sleep < 5: config.sleep = 5
    
    if not "inotify_excludes" in dir(config):
      inotify_excludes = []
  
    if not "watch_paths" in dir(config):
      raise RuntimeError, "no paths given to watch"
      
    for wpath in config.watch_paths:
      if not os.path.isdir(wpath) and not os.path.isfile(wpath):
#        raise RuntimeError, "one of the watch paths does not exist: %s" % wpath
        pass
        
      if not os.path.isabs(wpath):
        config.watch_paths[config.watch_paths.index(wpath)] = os.path.abspath(wpath)
        
    return config

  @staticmethod
  def end(signal=None, frame=None):
    logging.info("FINISHED: Bye bye; Hasta otro ratito")
    sys.exit(1)
