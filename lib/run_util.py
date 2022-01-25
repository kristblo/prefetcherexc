#!/usr/bin/env python
import sys, os, subprocess, threading, signal
from os.path import expandvars
from pipes import quote
from datetime import datetime

"""
Utility functions for running M5 with different parameters, either locally
or on a cluster using PBS.
"""

__all__ = [
    'dry_run', 'print_stats',
    'global_prefix', 'global_args', 'm5_path', 'm5_args', 'se_path',
    'after_cmd',
    'Config', 'cross', 'run_configs', 'submit_configs',
    'spec_configs', 'broken_spec_configs',
]


_prefix = ''
_global_args = []
_m5_path = 'm5/build/ALPHA_SE/m5.opt'
_se_path = expandvars('$HOME/m5/configs/example/se.py')
_m5_args = ['--remote-gdb-port=0', '-re']
_after_cmd = None

_submit_command = 'qsub'

_dry_run_mode = False

try:
    _processes = os.sysconf('SC_NPROCESSORS_ONLN')
except:
    _processes = 12

_pbs_header = """#!/bin/bash
#PBS -l nodes=1:ppn=12
#PBS -l pvmem=2000MB
#PBS -l walltime=3:00:00
#PBS -m a
#PBS -q default

echo Running on host `hostname -s`
export M5_CPU2000=~/m5files/cpu2000
module load gcc/4.4.3

cd $PBS_O_WORKDIR

"""

_signal_names = dict((getattr(signal, name), name)
    for name in dir(signal) if name.startswith('SIG') and '_' not in name)

class Popen_reporting(subprocess.Popen):
    """Print errors to stdout when a job fails."""
    def __init__(self, *args, **kwargs):
        self.__config_name = kwargs.pop('config_name')
        self.__config_dir = kwargs.pop('config_dir', None)
        subprocess.Popen.__init__(self, *args, **kwargs)

    def wait(self):
        returncode = subprocess.Popen.wait(self)
        if returncode:
            if self.__config_dir is not None:
                errfile = self.__config_dir + '/simerr'
                errors, _ = subprocess.Popen(['tail', '-n6', errfile], stdout=subprocess.PIPE).communicate()
            else:
                errors = ''
            if returncode < 0:
                print >>sys.stderr, "%s was terminated by signal %s (%d)." % \
                    (self.__config_name, _signal_names.get(-returncode, '???'), -returncode)
            else:
                print >>sys.stderr, "%s returned error %d." % \
                    (self.__config_name, returncode)
            sys.stderr.write("Last messages from %s:\n%s\n" % (self.__config_name, errors))
        return returncode


def dummy_Popen(args, *rargs, **kwargs):
    print ' '.join(quote(arg) for arg in args)
    return subprocess.Popen(['true'])

def dry_run():
    """Print commands to run instead of running them."""
    global Popen_reporting, _dry_run_mode
    if '--no-dry' in sys.argv:
        return
    Popen_reporting = dummy_Popen
    _dry_run_mode = True

class stats:
    ran = 0
    submitted = 0
    jobs = 0
    start_time = datetime.today()

def print_stats():
    if stats.ran:
        print 'Ran', stats.ran, 'configurations'
    if stats.submitted or stats.jobs:
        print 'Submitted', stats.submitted, 'configurations in', stats.jobs, 'jobs.'
    now = datetime.today()
    print 'Began', stats.start_time.isoformat() + ', finished ', now.isoformat() + '.'
    print 'Total time:', (now - stats.start_time).seconds / 3600.0, 'hours.'


def global_prefix(prefix):
    global _prefix
    _prefix = expandvars(prefix)

def global_args(*args):
    global _global_args
    _global_args = list(expandvars(arg) for arg in args)

def m5_path(path):
    global _m5_path
    _m5_path = expandvars(path)

def m5_args(*args):
    """Set Arguments to the M5 executable
    This will override the default arguments of -re and --remote-gdb-port=0.
    """
    global _m5_args
    _m5_args = list(args)

def se_path(path):
    global _se_path
    _se_path = expandvars(path)

def after_cmd(*command):
    """Set a command to run after each batch is completed.
    Note that a submission may be split into several batches.
    The following strings are provided for use with the %()s format:
        prefix: the prefix set with global_prefix.
        config_names: A space-separated list of the configurations that ran in this batch.
    """
    global _after_cmd
    _after_cmd = map(expandvars, command)


class Config:
    """A test configuration, consisting of a name and a list of command line arguments."""
    def __init__(self, name, args):
        self.name = str(name)
        self.args = args

    def __add__(self, other):
        return Config(self.name + '-' + other.name, self.args + other.args)

    def __str__(self):
        return 'Config(' + self.name + ')'


def _env_string(config):
    return ' '.join(env + '=' + quote(val) for env, val in _env_values(config).iteritems())

def  _env_values(config):
    return {'CONF_NAME': config.name, 'CONF_DIR': _prefix + config.name}

def _command_string(config):
    return ' '.join(quote(arg) for arg in _command_line(config))

def _command_line(config):
    return ([_m5_path] + _m5_args +
        ['--outdir=' + _prefix + config.name] +
        [_se_path] + _global_args + config.args
    )

def _after_cmd_args(configs):
    params = {'prefix': _prefix, 'config_names': ' '.join(c.name for c in configs)}
    return [arg % params for arg in _after_cmd]

def _make_dirs():
    dirs = _prefix.split('/')[:-1]
    try:
        os.makedirs('/'.join(dirs))
    except:
        pass


def cross(*config_lists):
    """Get the Cartesian product of lists of configurations.
    Returns an iterator.
    """
    confs, rest = config_lists[0], config_lists[1:]
    if isinstance(confs, Config):
        confs = [confs]
    if rest:
        for c in confs:
            for rc in cross(*rest):
                yield c + rc
    else:
        for c in confs:
            yield c

def _run_config(config):
    """Run a single configuration and return the Popen instance."""
    return Popen_reporting(_command_line(config),
        config_name = config.name,
        config_dir = _prefix + config.name
    )


def _run_configs_all(configs):
    """Run configs all in parallel on current computer"""
    _make_dirs()
    processes = [_run_config(c) for c in configs]
    for p in processes:
        p.wait()
        stats.ran += 1
    if _after_cmd is not None:
        Popen_reporting(_after_cmd_args(configs), config_name='after command').wait()


def run_configs(configs, max_parallel=_processes):
    """Run configs, with a maximum of max_parallel processes running at a time."""
    if max_parallel == 0:
        return _run_configs_all(configs)
    bound = threading.Semaphore(max_parallel)
    def run(config):
        _run_config(config).wait()
        bound.release()
        
    _make_dirs()
    for c in configs:
        bound.acquire()
        threading.Thread(target = run, args=(c,)).start()
        stats.ran += 1
    # Wait for processes to complete
    for i in xrange(max_parallel):
        bound.acquire()
    if _after_cmd is not None:
        Popen_reporting(_after_cmd_args(configs), config_name='after command').wait()


def _divide_equally(items, max_len):
    """Divide items into groups no longer than max_len
    
    >>>list(_divide_equally(xrange(10), 4))
    [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9]]
    """
    items = list(items)
    nitems = len(items)
    nlists = (nitems + max_len - 1) // max_len
    nlong = nitems % nlists
    for i in xrange(nlists):
        group_len = nitems // nlists
        if nlong:
            group_len += 1
            nlong -= 1
        group, items = items[:group_len], items[group_len:]
        yield group


def submit_configs(configs, name, per_job=_processes, save_pbs=True):
    """Submit configs via qsub, in groups of max size per_job.
    Mind that processes is set right if submitting remotely.
    """
    def count_subs(x):
        stats.submitted += 1
        return x
    _make_dirs()
    for i, job in enumerate(_divide_equally(configs, per_job)):
        pbs = _pbs_header + '\n'.join(
            _env_string(c) + ' ' + _command_string(count_subs(c)) + ' &' for c in job)
        pbs += '\n\nwait\n'

        # Could be made an option... Submit after_cmd in new job.
        after_in_new_job = True
        after_pbs = None
        if _after_cmd is not None:
            after_cmdline = ' '.join(quote(arg) for arg in _after_cmd_args(job)) + '\n'

            if after_in_new_job:
                after_pbs = _pbs_header + '\n' + after_cmdline
            else:
                pbs += after_cmdline

        job_name = name + '.%02d' % i
        stats.jobs += 1

        if _dry_run_mode:
            print 'Job', job_name
            print pbs
            continue

        if save_pbs:
            f = open(job_name + '.pbs', 'w')
            f.write(pbs)
            f.close()
            if after_pbs:
                f = open(job_name + 'a.pbs', 'w')
                f.write(after_pbs)
                f.close()

        qsub = subprocess.Popen([_submit_command, '-N', job_name, '-'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        job_id, _ = qsub.communicate(pbs)
        print job_id

        if after_pbs:
            qsub = subprocess.Popen([_submit_command, '-W', 'depend=afterany:' + job_id.strip(), '-N', job_name + 'a', '-'],
                    stdin=subprocess.PIPE)
            qsub.communicate(after_pbs)



# Removed because of errors:
# lucas
#  forrtl: warning: Could not open message catalog: for_msg.cat.
# parser
#  Unable to find workload for parser
# vpr_place
#  panic: ListenSocket(listen): listen() failed!
# Not checkpointed:
# mcf
#  exits before checkpoint
# gap
#  Mysterious close

specs = ['ammp', 'art110', 'art470', 'wupwise',
         'swim', 'applu', 'galgel', 'apsi',
         'twolf', 'bzip2_source', 'bzip2_graphic', 'bzip2_program']
spec_configs = [Config(spec, ['--bench=' + spec, ]) for spec in specs]

# These seem to hit cache bugs
broken_specs = ['vpr_route', 'mgrid']
broken_spec_configs = [Config(spec, ['--bench=' + spec, ]) for spec in broken_specs]
