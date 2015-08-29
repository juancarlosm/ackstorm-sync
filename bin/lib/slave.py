#!/usr/bin/env python

import os
import sys
import signal
import re
import logging
import fnmatch
import shutil
import shlex

from time import sleep
from common import *

LOG_FILE = './var/log/ackstorm-sync-slave.log'
CONFIG_FILE = './etc/slave_conf.py'
LAST_RUN_FILE = './var/.last_run'

RSYNC_ERROR_TO_CATCH = [23]

class SyncSlave():
  def __init__(self):
    # Create required folders
    common_create_dirs()
    self.config = self.load_config()
    
    # Read config from master
    import master
    self.master = master.SyncMaster()
    
    # exlude our paths
    self.exclude_paths = ['./var', './data']
    
  def run(self,pid_file):
    # Check and write pid
    if common_check_pid_file(pid_file):
      print "[ERROR] Another process is running...."
      sys.exit(1)
      
    common_write_pid_file(pid_file)
      
    # Configure logging
    loglevel = logging.INFO
    if self.config.verbose: loglevel = logging.DEBUG
    logging.basicConfig(filename=LOG_FILE,level=loglevel,format='%(asctime)s %(levelname)s: %(message)s')

    logging.info("STARTING...")
    
    # Catch signals
    self.catch_signals()
    
    # Run initial sync?
    if self.config.initial_full_sync:
      logging.info("RUNNING INITIAL SYNCRONIZATION")
      self.full_sync()
      
    else:
      logging.info("INITIAL SYNCRONIZATION: SKIPPED")
      
    elasped = 0
    while True:
      try:
        self.loop()
        sleep(self.config.sleep)
        
        # Do a full sync?
        if self.config.full_sync_interval:
          elasped = elasped + self.config.sleep
          if elasped >= self.config.full_sync_interval:
            logging.info("RUNNING FULL SYNCRONIZATION")
            self.full_sync()
            elasped = 0
          
      except KeyboardInterrupt:
        logging.info("KILLED BY KEYBOARD INTERRUPT")
        break
  
    common_del_pid_file(pid_file)
    self.end()
      
  def loop(self):
    # Rsync updates from origin
    logging.debug("SYNCING DATA FILES")
    _cmd = [
      self.config.rsync_cmd] + self.config.rsync_opts + [
      "--password-file", 
      self.config.rsync_secret_file,
      self.config.rsync_user + '@' + self.config.master + '::' + self.config.rsync_updates + '/',
      './data/'
    ]
    
    logging.debug("Command: " + ' '.join(_cmd))
    retval, output, error = common_run(_cmd)
    
    # Get last updated version
    read_last_version = self.read_last_run()
        
    # Build pending updates files
    pending = []
    write_last_version = read_last_version
    for filename in os.listdir('./data/'):
      try:
        file_version,file_type = filename.split('.')
        file_version = int(file_version)
    
      except:
        continue
        
      if file_version >= read_last_version:
        logging.debug("File needs to be processed: %s" %filename)
        pending.append(filename)
        
      else:
        logging.debug("Already processed: %s" %filename)
        
      # Get last processed to write time
      if file_version > write_last_version:
        write_last_version = file_version
        
    # Sync each file
    failed = False
    failed_stdout = ''
    failed_stderr = ''
    files_processed = 0
    
    # Add excludes from master
    extra_rsync_opts = []
    for exclude in self.master.config.excludes:
      if exclude.startswith('/'): exclude = exclude[1:]
      extra_rsync_opts.append("--exclude=%s" % exclude)
    
    # Add our paths
    for path in [os.path.realpath('./var'), os.path.realpath('./data')]:
      if exclude.startswith('/'): exclude = exclude[1:]
      extra_rsync_opts.append("--exclude=%s" % exclude)
    
    # Sort list of files
    ordered_pending = sorted(pending, key=lambda x: (int(re.sub('\D','',x)),x))
    
    synced_files = []
    for file in ordered_pending:
      logging.info("SYNCING FILES FROM %s" % file)
      _cmd = [self.config.rsync_cmd] + self.config.rsync_opts + extra_rsync_opts + [
        '--out-format',
        'file:%n%L',
        "--files-from=" + './data/' + file,
        "--password-file",
        self.config.rsync_secret_file,
        self.config.rsync_user + '@' + self.config.master + '::root/',
        '/'
      ]
        
      logging.debug("Command: " + ' '.join(_cmd))
      retval, output, error = common_run(_cmd)
      
      files_processed += 1
      
      # Check if there is a pending delete
      if retval in RSYNC_ERROR_TO_CATCH:
        with open('./data/' + file, 'r') as file:
          content = file.read()
          
        for line in content.splitlines():
          if line.startswith('#DELETE:'):
            _file = '/' + line[8:]
            if not self.inside_sync_paths(_file):
              logging.info("File not inside sync path: %s" % _file)
              continue
              
            try:
              if os.path.isfile(_file):
                os.remove(_file)
                logging.info("DELETE FILE: %s" %line[8:])
              
              elif os.path.isdir(_file):
                shutil.rmtree(_file)
                logging.info("DELETE DIR: %s" %line[8:])
              
              synced_files.append(_file)
            except OSError: 
              pass
            
      elif retval:
        failed = True
        failed_stdout = output
        failed_stderr = error
        
      for line in output.split('\n'):
        if not line: continue
        if not line.startswith('file:'): continue
        if line.endswith('/'): continue
        
        _file = '/' + line[5:]
        logging.debug("Synced: %s" % _file)
        synced_files.append(_file)
        
#      logging.debug("r: %i - %s %s" %(retval,output,error))

    if files_processed:    
      logging.info('FILES PROCESSED: %d' %files_processed)
    
    if synced_files:
      self.process_actions(synced_files)
    
    if self.config.dry_run:
      logging.info('DRY RUN (NOT writting last updated: %s' %write_last_version)
      sys.exit(0)
    
    if failed: 
      logging.info("Some problems happened, not writing last processed")
      logging.info("Rsync output: %s - %s" %(failed_stdout,failed_stderr))
      sys.exit(1)
      
    # Go ahead if we are using the same file  
    if files_processed == 1 and write_last_version == read_last_version:
      logging.info("SAME FILE PROCESSED: Moving one second forward")
      write_last_version = write_last_version + 1

    # Write last updated file
    if write_last_version != read_last_version:
      logging.info("UPDATING LAST RUN VERSION: %s (was %s)" %(write_last_version,read_last_version))
      self.update_last_run(write_last_version)
      
    # Write execution file  
    with open(self.config.end_sync_file, 'w') as file:
      file.write("%s" % write_last_version)
    
  def read_last_run(self):
    last_run=1
    if os.path.exists(LAST_RUN_FILE):
      for line in open(LAST_RUN_FILE):
        line = line.strip()
        if not line: continue
      
        last_run = int(float(line))
        break;
  
    return last_run
  
  def catch_signals(self):
    signal.signal(signal.SIGTERM, self.end)
    signal.signal(signal.SIGINT,  self.end)
    
  def update_last_run(self,_version):
    with open(LAST_RUN_FILE, 'w') as file:
      file.write("%s" % _version)
      
  def full_sync(self):
    logging.info("Full syncronization in progress...")
    # Prepare excludes
    extra_rsync_opts = []
    for exclude in self.master.config.excludes:
      if exclude.startswith('/'): exclude = exclude[1:]
      extra_rsync_opts.append("--exclude=%s" % exclude)
      
    # Add our dirs
    for exclude in ['var/','data/']:
      extra_rsync_opts.append("--exclude=%s" % exclude)

    synced_files = []    
    for path in self.master.config.watch_paths:
      logging.info("SYNCING PATH: %s" % path)
      _cmd = [self.config.rsync_cmd] + self.config.rsync_opts + extra_rsync_opts + [
        '--out-format',
        'file:%n%L',
        "--password-file",
        self.config.rsync_secret_file,
        self.config.rsync_user + '@' + self.config.master + '::root' + path + '/',
        path + '/'
      ]
      
      logging.debug("Command: " + ' '.join(_cmd))
      retval, output, error = common_run(_cmd)
      
      for line in output.split('\n'):
        if not line: continue
        if not line.startswith('file:'): continue
        if line.endswith('/'): continue
        
        _file = path + '/' + line[5:]
        logging.debug("Synced: %s" % _file)
        synced_files.append(_file)
        
    if synced_files:
      self.process_actions(synced_files)
      
#      logging.debug("r: %i - %s %s" %(retval,output,error))
  
  def process_actions(self,files):
    for file in files:
      logging.debug("Processing actions")
      
      # Process excludes
      todos = {}
      for action in self.config.actions:
        if fnmatch.fnmatch(file, action.keys()[0]):
          logging.info("MATCH ACTION %s IN FILE: %s" % (action[action.keys()[0]],file))
          todos[action[action.keys()[0]]] = 1
          
      for todo in todos.keys():
        if not todo: continue        
        logging.info("RUNNING ACTION: %s" %todo)
        common_run(shlex.split(todo), detached=True)
        logging.info("Done (processed in background)")
        
  def inside_sync_paths(self,filename):
    abspath = os.path.abspath(filename)
    
    for path in self.master.config.watch_paths:
      if path in abspath:
        return True
        
    return False
  
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
    
    if not "master" in dir(config):
      raise RuntimeError, "No master host is set"
      
    if not "rsync_user" in dir(config):
      raise RuntimeError, "No rsync_user is set"
      
    if not "rsync_password" in dir(config):
      raise RuntimeError, "No rsync_password is set"
      
    else:
      _file = './var/.rsync.secret'
      with open(_file,'w') as file:
        file.write(config.rsync_password)
      os.chmod(_file,0600)
      config.rsync_secret_file = _file
      
    if not "rsync_updates" in dir(config):
      raise RuntimeError, "No rsync_updates is set"
      
    if not "rsync_opts" in dir(config):
      config.rsync_opts = ["-av","-r","--delete","--timeout=20","--force","--ignore-errors"]
      
    if not "rsync_secret_file" in dir(config):
      config.rsync_secret_file = './etc/rsync.secret'
      
    if not "dry_run" in dir(config):
      config.rsync_opts.append('--dry-run')

    if not "initial_full_sync" in dir(config):
      config.initial_full_sync = False
      
    if not "verbose" in dir(config):
      config.verbose = True
      
    if not "daemonize" in dir(config):
      config.daemonize = True
      
    if not "full_sync_interval" in dir(config):       
      config.sleep = 3600*24
      
    if not "sleep" in dir(config):       
      config.sleep = 5
      
    config.sleep = int(config.sleep)
    if config.sleep < 5: config.sleep = 5
    
    if not "actions" in dir(config):
      config.actions = []
        
    return config

  @staticmethod
  def end(signal, frame):
    logging.info("FINISHED: Bye bye; Hasta otro ratito")
    sys.exit(1)
