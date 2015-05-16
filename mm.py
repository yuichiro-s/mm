#!/usr/bin/env python

import sys
import hashlib
import os
from datetime import datetime, timedelta
import time
import subprocess
import glob

MM_FILE = ".mm"
CONFIG_FILE = ".mmconfig"

SCRIPT_DIRS = None   # only files under this directory are regarded as script

# print usage of mm
def usage():
  print "Usage:"
  print "  mm -i\t\t\tCreate .mm file"
  print "  mm SOME_COMMAND\tExecute command and record input and output files."
  print "  mm -v\t\t\tVisualize the relationships among the data."
  print "  mm -p <data>\t\tShow the command which produced the data"
  print "  mm -m <src> <dst>\tMove file."

# get list of arguments if arguments are provided as string
def expand_args(arg_str):
  args = []
  for arg in arg_str.split():
    paths = glob.glob(os.path.expanduser(arg))
    if paths:
      args.extend(paths)
    else:
      args.append(arg)
  return args

# get root directory of mm
def get_root_dir(cwd):
  if os.path.exists(cwd):
    mm_path = os.path.join(cwd, MM_FILE)
    if os.path.exists(mm_path):
      return cwd
    else:
      if cwd == "/":
        return None
      else:
        return get_root_dir(os.path.dirname(cwd))
  else:
    return None

def create_mm_file(mm_path):
  if os.path.exists(mm_path):
    print >> sys.stderr, ".mm already exists at {}".format(mm_path)
    exit()

  # create .mm file
  f = open(mm_path, "w")
  f.close()
  print >> sys.stderr, "Created .mm at {}".format(mm_path)

def exists_in_dirs(path, dirs, root_dir_path):
  if not os.path.isabs(path):
    path = os.path.join(root_dir_path, path)
  if os.path.exists(path):
    for d in dirs:
      d_path = os.path.join(root_dir_path, d) + "/"
      if os.path.commonprefix([path, d_path]) == d_path:
        return True
  return False

def load_mm(mm_path):
  mm = []
  with open(mm_path, "r") as f:
    record = {}
    for line in f:
      if line.startswith("#"):
        continue
      
      line = line.rstrip()
      if len(line) == 0:
        if record:
          mm.append(record)
          record = {}
      else:
        k, v = line.split("=")
        k = k.strip()
        v = v.strip()
        if k == "IN" or k == "OUT":
          record[k] = v.split()
        else:
          record[k] = v
    if record:
      mm.append(record)
  return mm

def append_mm(mm_path, record):
  with open(mm_path, "a") as f:
    print >> f
    for k, v in record.items():
      if isinstance(v, list):
        print >> f, k + "=" + " ".join(v)
      else:
        print >> f, k + "=" + str(v)

class Data:
  def __init__(self, name, parent):
    self.name = name
    self.parent = parent
    self.exists = True

class Script:
  def __init__(self, name, cmd, ins):
    self.name = name
    self.cmd = cmd
    self.ins = ins

class MmGraph:
  def __init__(self, data, scripts, name2data):
    self.data = data
    self.scripts = scripts
    self.name2data = name2data

def make_mm_graph(mm):
  name2data = {}   # map from name to current data
  all_data = set()     # set of data

  for record in mm:
    if record["TYPE"] == "MV":
      src = record["SRC"]
      dst = record["DST"]
      if src in name2data:
        # rename
        name2data[src].name = dst
        name2data[dst] = name2data[src]
        del name2data[src]

    elif record["TYPE"] == "CMD":
      cmd = record["CMD"]
      script = record["SCRIPT"]
      ins = record["IN"]
      outs = record["OUT"]

      in_data = set()

      for in_name in ins:
        if not in_name in name2data:
          d = Data(in_name, None)
          name2data[in_name] = d
          all_data.add(d)
        else:
          d = name2data[in_name]
        in_data.add(d)

      s = Script(script, cmd, in_data)

      for out_name in outs:
        d = Data(out_name, s)
        if out_name in name2data:
          name2data[out_name].exists = False
        name2data[out_name] = d
        all_data.add(d)

  # filter out scripts all of whose output files are not available
  scripts = set()  # set of scripts
  for d in all_data:
    if d.exists and d.parent is not None:
      scripts.add(d.parent)

  # filter out data all of whose output files are not available
  data = set()     # set of data
  for s in scripts:
    data |= s.ins
  for d in all_data:
    if d.parent in scripts:
      data.add(d)

  # set existence
  for d in data:
    if os.path.isabs(d.name):
      d.exists = os.path.exists(d.name)
    else:
      d.exists = os.path.exists(os.path.join(root_dir_path, d.name))

  return MmGraph(data, scripts, name2data)

def normalize_path(path, root_dir_path):
  return os.path.relpath(os.path.abspath(path), os.path.abspath(root_dir_path))

def visualize(mm_graph, graph_path):
  import pydot

  graph = pydot.Dot(graph_type="digraph")

  script_nodes = {}  # map from Script to Node
  data_nodes = {}    # map from Data to Node

  for s in mm_graph.scripts:
    node = pydot.Node(str(s), label=s.name, shape="rectangle")
    script_nodes[s] = node
    graph.add_node(node)

  for d in mm_graph.data:
    if d.exists:
      style = "solid"
    else:
      style = "dashed"
    node = pydot.Node(str(d), label=d.name, style=style)
    data_nodes[d] = node
    graph.add_node(node)

    if d.parent is not None:
      # connect to parent script
      graph.add_edge(pydot.Edge(script_nodes[d.parent], node))

  for s in mm_graph.scripts:
    node = script_nodes[s]
    for d in s.ins:
      graph.add_edge(pydot.Edge(data_nodes[d], node))

  graph.write_png(graph_path)

if __name__ == "__main__":

  # parse arguments
  if len(sys.argv) == 1:
    usage()
    exit()

  # detect .mm file
  root_dir_path = get_root_dir(os.getcwd())

  if len(sys.argv) == 2 and (sys.argv[1] == "-i" or sys.argv[1] == "--init"):
    # initialize .mm
    create_mm_file(os.path.join(os.getcwd(), MM_FILE))
    exit()

  if root_dir_path is None:
    # .mm file not found
    # prompt user to init mm
    print >> sys.stderr, ".mm was not detected. Initialize by \"mm --init\""
    exit()

  # open config file
  config_file_path = os.path.join(root_dir_path, CONFIG_FILE)
  if os.path.exists(config_file_path):
    for line in open(config_file_path):
      k, v = line.rstrip().split("=")
      k = k.strip()
      v = v.strip()
      if k == "SCRIPT_DIRS":
        SCRIPT_DIRS = map(lambda path: os.path.join(root_dir_path, path), v.split())

  # read .mm file
  print >> sys.stderr, "Root directory is", root_dir_path
  mm_path = os.path.join(root_dir_path, MM_FILE)
  mm = load_mm(mm_path)
  mm_graph = make_mm_graph(mm)

  if len(sys.argv) >= 2 and (sys.argv[1] == "-v" or sys.argv[1] == "--visualize"):
    # visualize
    name = "graph.png"
    if len(sys.argv) == 3:
      name = sys.argv[2]
    visualize(mm_graph, os.path.join(os.getcwd(), name))

  elif len(sys.argv) == 3 and (sys.argv[1] == "-p" or sys.argv[1] == "--parent"):
    # show parent
    name = normalize_path(sys.argv[2], root_dir_path)
    if not name in mm_graph.name2data or not os.path.exists(sys.argv[2]):
      print >> sys.stderr, "No such file:", name
    else:
      s = mm_graph.name2data[name].parent
      if s is None:
        print >> sys.stderr, "No parent info."
      else:
        print >> sys.stderr, "   CMD:", s.cmd
        print >> sys.stderr, "SCRIPT:", s.name
        for d in s.ins:
          print >> sys.stderr, "    IN:", d.name,
          if not d.exists:
            print >> sys.stderr, "(removed)",
          print >> sys.stderr

  elif len(sys.argv) == 4 and (sys.argv[1] == "-m" or sys.argv[1] == "--move"):
    src = sys.argv[2]
    dst = sys.argv[3]

    cmd_str = "mv " + src + " " + dst
    print >> sys.stderr, cmd_str
    if subprocess.call(cmd_str, shell=True) == 0:
      record = {
          "TYPE": "MV",
          "SRC": normalize_path(src, root_dir_path),
          "DST": normalize_path(dst, root_dir_path),
          }
      append_mm(mm_path, record)

  else:
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
      # redirection detected
      print >> sys.stderr, "Use of redirection is not supported. Double-quote the command."
      exit()

    cmd = sys.argv[1:]

    if len(cmd) == 1:
      cmd = expand_args(cmd[0])

    program_name = None

    # detect input files
    in_files = set()
    for arg in cmd[1:]:
      if arg == "/":
        continue

      if SCRIPT_DIRS is not None and program_name is None and exists_in_dirs(arg, SCRIPT_DIRS, root_dir_path):
        # first file which appears as argument is regarded as program_name
        # if SCRIPT_DIRS is specified, and the file exists in SCRIPT_DIRS
        program_name = normalize_path(arg, root_dir_path)
      elif os.path.exists(arg):
        if exists_in_dirs(arg, [root_dir_path], root_dir_path):
          path = normalize_path(arg, root_dir_path)
        else:
          path = os.path.abspath(arg)
        in_files.add(path)

    if program_name is None:
      program_name = cmd[0]

    # execute the command
    start_time = datetime.now()
    if subprocess.call(" ".join(cmd), shell=True) != 0:
      exit()
    end_time = datetime.now()

    # detect output files
    out_files = []
    for root, dirs, files in os.walk(root_dir_path):
      for f in files:
        if not f.startswith("."):
          # ignore hidden files
          path = os.path.join(root, f)
          mtime = datetime.fromtimestamp(os.path.getmtime(path))

          lower = start_time - timedelta(seconds=1)
          upper = end_time + timedelta(seconds=1)
          if lower <= mtime and mtime <= upper:
            # regard as an output file
            out_files.append(normalize_path(path, root_dir_path))

    # filter out output files from input files
    in_files = filter(lambda f: not f in out_files, in_files)

    if len(out_files) > 0:
      record = {
          "TYPE": "CMD",
          "START": start_time,
          "END": end_time,
          "CWD": normalize_path(os.getcwd(), root_dir_path),
          "CMD": " ".join(cmd),
          "SCRIPT": program_name,
          "IN": in_files,
          "OUT": out_files,
          }
      append_mm(mm_path, record)

      # report detected files
      print >> sys.stderr, "SCRIPT:", program_name
      for in_f in in_files:
        print >> sys.stderr, "    IN:", in_f
      for out_f in out_files:
        print >> sys.stderr, "   OUT:", out_f
    else:
      print >> sys.stderr, "No output file found."
