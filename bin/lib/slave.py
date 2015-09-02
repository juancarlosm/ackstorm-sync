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
VERSION_FILE = './var/.version'

RSYNC_ERROR_TO_CATCH = [23]

class SyncSlave():
  def __init__(self):
    # Create required folders
    create_dirs()
    self.config = self.load_config()
    
    # Read config from master
    import master
    self.master = master.SyncMaster()
    
    # exlude our paths
    self.exclude_paths = ['./var', './data']
    
    # Get last updated version (read from file if needed)
    self.version = self.read_version()
    
  def run(self,pid_file):
    # Check and write pid
    if pid_file_check(pid_file):
      print "[ERROR] Another process is running...."
      sys.exit(1)
      
    pid_file_write(pid_file)
      
    # Configure logging
    loglevel = logging.INFO
    if self.config.verbose: loglevel = logging.DEBUG
    logging.basicConfig(filename=LOG_FILE,level=loglevel,format='%(asctime)s %(levelname)s: %(message)s')

    logging.info("STARTING...")
    
    # Catch signals
    self.catch_signals()
    
    # Run initial sync?
    if self.config.initial_fullsync:
      logging.info("RUNNING INITIAL SYNCRONIZATION")
      
      # Read updates and set last version (avoid to process file)
      _last_version, _ = self.sync_updates(self.version)
      self.update_version(_last_version, self.version)
      self.version = _last_version
      
      # Now run the fullsync
      self.fullsync()
      
    else:
      logging.info("INITIAL SYNCRONIZATION: SKIPPED")
      
    elasped = 0
    logging.info("Main process started")
    while True:
      try:
        self.process_pending()
        sleep(self.config.sleep)
        
        # Time to do a full sync?
        if self.config.fullsync_interval:
          elasped = elasped + self.config.sleep
          if elasped >= self.config.fullsync_interval:
            logging.info("RUNNING FULL SYNCRONIZATION")
            self.fullsync()
            elasped = 0
          
      except KeyboardInterrupt:
        logging.info("KILLED BY KEYBOARD INTERRUPT")
        break;
  
    pid_file_del(pid_file)
    self.end()
      
  def process_pending(self):
    # Build pending updates files
    last_version, pending = self.sync_updates(self.version)
        
    # Sync each file
    failed = False
    failed_stdout = ''
    failed_stderr = ''
    files_processed = 0
    
    # Add excludes from master
    extra_rsync_opts = []
    for exclude in self.master.config.excludes:
      if exclude.startswith('/'):
        exclude = exclude[1:]
      extra_rsync_opts.append("--exclude=%s" % exclude)
    
    # Add our paths 2
    for path in [os.path.realpath('./var'), os.path.realpath('./data')]:
      extra_rsync_opts.append("--exclude=%s" % path[1:])
      
    synced_files, commands = [], []
    for file in pending:
      logging.info("SYNCING FILES FROM %s" % file)

      # Run rsync
      files_processed += 1
      retval, output, error = self.rsync(
        self.config.rsync_user + '@' + self.config.master + '::root/',
        '/',
        extra_rsync_opts + ["--files-from=" + './data/' + file]
      )
      
      # Check if there is a pending delete
      if retval in RSYNC_ERROR_TO_CATCH:
        with open('./data/' + file, 'r') as ofile:
          content = ofile.read()
          
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
              
        # Process again rsync command to ensure all files exists
        self.rsync(
          self.config.rsync_user + '@' + self.config.master + '::root/',
          '/',
          extra_rsync_opts + ["--files-from=" + './data/' + file]
        )
            
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
      logging.info('FILES PROCESSED: %d' % files_processed)
      
    if self.config.dry_run:
      logging.info('NOT uptating version: %s (DRY RUN)' % last_version)
      return
    
    if synced_files:
      self.process_actions(synced_files)
    
    if failed: 
      logging.info("Some problems happened")
      logging.info("Rsync output: %s - %s" % (failed_stdout,failed_stderr))
      # but continue to not live in and endless loop
      
    # Go ahead if we are using the same file  
    if files_processed == 1 and last_version == self.version:
      logging.info("SAME FILE PROCESSED: Moving version forward")
      last_version = last_version + 1

    # Write last updated file
    if last_version != self.version:
      self.update_version(last_version,self.version)
      self.version = last_version
      
    # Write end of sync file  
    with open(self.config.end_sync_file, 'w') as ofile:
      ofile.write("%s" % self.version)
      
  def sync_updates(self, last_version):
    logging.debug("SYNCING DATA FILES")
    _data_dir = './data/'
    
    # Run rsync
    retval, output, error = self.rsync(
      self.config.rsync_user + '@' + self.config.master + '::' + self.config.rsync_updates + '/',
      _data_dir,
    )
    
    _pending = []
    for _file in os.listdir(_data_dir):
      try:
        file_version,file_type = _file.split('.')
        file_version = int(file_version)
    
      except:
        continue
        
      if file_version >= self.version:
        logging.debug("File needs to be processed: %s" %_file)
        _pending.append(_file)
        
#      else:
#        logging.debug("Already processed: %s" %_file)
        
      # Get last processed to write time
      if file_version > last_version:
        last_version = file_version
        
    # Sort list of files
    ordered = sorted(_pending, key=lambda x: (int(re.sub('\D','',x)),x))
        
    return last_version, ordered
    
  def read_version(self):
    version=1
    if os.path.exists(VERSION_FILE):
      for line in open(VERSION_FILE):
        line = line.strip()
        if not line: continue
      
        version = int(float(line))
        break;
  
    return version
  
  def catch_signals(self):
    signal.signal(signal.SIGTERM, self.end)
    signal.signal(signal.SIGINT,  self.end)
    
  def update_version(self,_version, _old_version = 1):
    logging.info("UPDATING VERSION: %s (was %s)" %(_version, _old_version))
    with open(VERSION_FILE, 'w') as ofile:
      ofile.write("%s" % _version)
      
  def fullsync(self):
    logging.info("Full syncronization in progress...")

    # Prepare excludes
    excludes = self.master.config.excludes + [
      os.path.abspath('./var') + '/*',
      os.path.abspath('./data') + '/*'
    ]

    synced_files = []
    for path in self.master.config.watch_paths:
      logging.info("SYNCING PATH: %s" % path)  
      
      if not os.path.isfile(path):
        path = path + '/'
        
      # Excludes need to be relative to path
      extra_rsync_opts = []   
      for exclude in excludes:
        if exclude.startswith('/'): # is dir
          _tmp = exclude.replace(path,'')   
          if _tmp != exclude:
            extra_rsync_opts.append("--exclude=%s" % _tmp)
               
          else:
            # exclude is not inside this sync path (ignore it)
            pass

        else:
          extra_rsync_opts.append("--exclude=%s" % exclude)

      # Run rsync
      retval, output, error = self.rsync(
        self.config.rsync_user + '@' + self.config.master + '::root' + path,
        path,
        extra_rsync_opts
      )
      
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
        logging.info("RUNNING ACTION (in background): %s" %todo)
        run(shlex.split(todo), detached=True)
        
  def inside_sync_paths(self,filename):
    abspath = os.path.abspath(filename)
    
    for path in self.master.config.watch_paths:
      if path in abspath:
        return True
        
    return False
    
  def rsync(self, rsync_from, rsync_to, rsync_ops = []):
      _cmd = [self.config.rsync_cmd] + self.config.rsync_opts + rsync_ops + [
        '--out-format',
        'file:%n%L',
        "--password-file",
        self.config.rsync_secret_file,
        rsync_from,
        rsync_to
      ]
      
      logging.debug("Executing command: " + ' '.join(_cmd))
      _retval, _output, _error = run(_cmd)
      
      if _retval:
        logging.debug("RETVAL: %s" % _retval)
        logging.debug("ERROR:  %s" % _error)
      return _retval, _output, _error
  
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
      with open(_file,'w') as ofile:
        ofile.write(config.rsync_password)

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

    if not "initial_fullsync" in dir(config):
      config.initial_fullsync = False
      
    if not "verbose" in dir(config):
      config.verbose = True
      
    if not "daemonize" in dir(config):
      config.daemonize = True
      
    if not "fullsync_interval" in dir(config):       
      config.fullsync_interval = 3600*4
      
    config.fullsync_interval = int(config.fullsync_interval)
      
    if not "sleep" in dir(config):       
      config.sleep = 5
      
    config.sleep = int(config.sleep)
    if config.sleep < 5: config.sleep = 5
    
    if not "actions" in dir(config):
      config.actions = []
        
    return config

  @staticmethod
  def end(signal=None, frame=None):
    logging.info("FINISHED: Bye bye; Hasta otro ratito")
    sys.exit(1)

