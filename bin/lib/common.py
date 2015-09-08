#!/usr/bin/env python

import os
import sys
import shlex
from subprocess import Popen, PIPE

WORKDIRS = ['./var','./var/log','./data']
LAST_RUN_FILE = './var/.last_run'

def create_dirs():
  # Create required folders
  for _dir in WORKDIRS:
    if not os.path.isdir(_dir):
      os.mkdir(_dir)
      

def pid_file_write(filename):
  with open(filename,'w') as file:
    file.write(str(os.getpid()))
    
  return True
    

def pid_file_check(filename):
  if os.path.exists(filename):
    f = open(filename, 'r')
    pid = f.read()

    proc_file = '/proc/%s/cmdline' % pid
    if pid and os.path.exists(proc_file):
      return pid

  return False

  
def pid_file_del(filename):
  if os.path.exists(filename):
    os.remove(filename)
  

def run(command, detached=False):
  if detached:
    if fork():
      return # Main process just returns
      
  p = Popen(
    command,
    bufsize=0,
    stdin=PIPE, stdout=PIPE, stderr=PIPE,
    universal_newlines=True,
    env=os.environ.copy(),
    close_fds=(os.name == 'posix')
  )

  output, error = p.communicate()

  if detached:
    sys.exit() # Just exit
    
  return p.wait(), output, error
  
def run_multi(commands, detached=False):
  # collect output in parallel
  def _get_lines(process):
    return process.communicate()[0].splitlines()

  from multiprocessing.dummy import Pool # thread pool
      
  processes = list()
  for cmd in commands:
    processes.append(subprocess.Popen(
      cmd,
      bufsize=0,
      stdin=PIPE, stdout=PIPE, stderr=PIPE,
      universal_newlines=True,
      env=os.environ.copy(),
      close_fds=(os.name == 'posix')
  ))

  outputs = Pool(len(processes)).map(_get_lines, processes)
  exitcodes = [p.wait() for p in processes]

  print "out: %s" % outputs


def load_config_role(filename):
  if not os.path.isfile(filename):
    raise RuntimeError, "Configuration file does not exist: %s" % filename

  configdir  = os.path.dirname(filename)
  configfile = os.path.basename(filename)

  if configfile.endswith(".py"):
    configfile = configfile[0:-3]

  else:
    raise RuntimeError, "Configuration file must be a importable python file ending in .py"

  sys.path.append(configdir)
  exec("import %s as __config__" % configfile)
  sys.path.remove(configdir)

  config = __config__
  
  if not "role" in dir(config):
    raise RuntimeError, "Unable to get role"
    
  return config


def fork():
  """Detach a process from the controlling terminal and run it in the
  background as a daemon.
  @returns: True is parent, False is child
  """
  try:
    pid = os.fork()
    if pid > 0:
      # Parent retuns
      return True

  except OSError:
    raise Exception

  # Second fork
  try:
    pid = os.fork()
    if pid > 0:
      # Sencond parent is useless
      os._exit(0)

  except OSError:
    raise Exception

  os.chdir('/')
  os.umask(0)

  os.open("/dev/null", os.O_RDWR)
  os.dup2(0, 1)
  os.dup2(0, 2)

  # This is the detached child
  return False
