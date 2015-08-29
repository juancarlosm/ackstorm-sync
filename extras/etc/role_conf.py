#!/usr/local/env python

# Need to set role = 'master' or 'slave' use the python logic you want
import platform

role = 'slave'
hostname = platform.node()
if hostname == 'front1':
  role = 'master'
