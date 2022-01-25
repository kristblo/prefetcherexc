#! /usr/bin/env python
import operator, os, os.path, re, sys
from cgi import escape

__todo__ = """
* How to aggregate numbers for accuracy and coverage.
"""

# the name of the file with statistics output from M5
DATAFILE = 'stats.txt'

# the name of the testrun without prefetching enabled
BASELINE_PF = 'none'

# the separator used to split directory names into prefetcher and test name
PF_TEST_SEP = '-'

# <pattern_name> <pattern>
# <pattern_name> is the key used in the generated stats table
# <pattern> is what is searched for in M5's stats output
PATTERNS = {
    'pf_requests':      'system.l2.HardPFReq_mshr_misses',
    'pf_misses':        'system.l2.demand_misses',
    'pf_hits':          'system.l2.prefetch_hits',
    'pf_identified':    'system.l2.prefetcher.num_hwpf_identified',
    'pf_issued':        'system.l2.prefetcher.num_hwpf_issued',
    'ipc':              'system.switch_cpus_1.ipc_total',
}


def format_stats(pf_stats, prefetcher='all', test='all', type_='text', filename=None):
    """
    Returns statistics for given prefetcher(s) and test(s) as a string.
    In addition, if a filename is given, the stats will be written.
    Default is to do return a summary of all prefetchers as a string.

    prefetcher: <name> | all
    test:       <name> | all
    type_:      text | html
    filename:   path | None
    """


    if prefetcher == 'all' and test == 'all':
        s = summary(pf_stats, type_)
    else:
        if prefetcher != 'all' and prefetcher not in pf_stats:
            print >> sys.stderr, 'no such prefetcher: %s' % prefetcher
            return ''
        if test != 'all' and BASELINE_PF in pf_stats and test not in pf_stats[BASELINE_PF]:
            print >> sys.stderr, 'no such test: %s' % test
            return ''

        s = filter_stats(prefetcher, test, pf_stats, type_)

    if filename:
        f = open(filename, 'w')
        f.write(s)
        f.close()

    return s


def parse(lines, table):
    """ Parse each statistics file. """
    for line in lines:
        if not line: continue
        id_str, val = line.split()[:2]

        for name, pattern in PATTERNS.items():
            if id_str == pattern:
                try:
                    val = int(val)
                except ValueError:
                    val = float(val)
                table[name] = val


def arithmetic_mean(lst, weights=None):
    """
    Computes the (weighted) arithmetic mean of a list of numbers.
        avg = (w_0 * a_0 + w_1 * a_1 * ... * w_n * a_n) / n
    """
    if weights is None:
        weights = [1.0] * len(lst)

    return sum(w*x for w,x in zip(weights, lst)) / sum(weights)


def geometric_mean(lst):
    """
    Computes the geometric average of a list of numbers.
        g = nthroot(a_0 * a_1 * ... * a_n)
    """

    return reduce(operator.mul, lst, 1.0) ** (1.0 / len(lst))


def harmonic_mean(lst):
    """
    Computes the (weighted) harmonic average of a list of numbers.
        h = n / (1/a_0 + 1/a_1 + ... 1/a_n)
    """

    n = len(lst)
    s = sum(1.0 / x for x in lst)

    if s > 0:
        return float(n) / s
    else:
        return 0.0


def compare(prefetchers, prefetcher, test):
    """ Compare the prefetcher with the baseline. Adds speedup, accuracy and coverage data. """
    data = prefetchers[prefetcher][test].copy()

    acc = data.get('pf_hits', 0) / float(data.get('pf_requests', sys.maxint))
    if BASELINE_PF in prefetchers:
        pf_none = prefetchers[BASELINE_PF][test]
        speedup = data.get('ipc', 0) / pf_none['ipc']
        cov = data.get('pf_hits', 0) / float(pf_none['pf_misses'])
    else:
        speedup = float('nan')
        cov = float('nan')

    data['accuracy'] = acc
    data['coverage'] = cov
    data['speedup'] = speedup

    return data


def filter_stats(pf, test, pf_stats, type_):
    """ Pretty-print per-test results for a prefetcher. """

    if test == 'all':
        caption = "PREFETCHER: %s" % pf
        h1 = 'TEST'
    else:
        caption = "TEST: %s" % test
        h1 = 'PREFETCHER'

    headers = (h1, 'IPC', 'SPEEDUP', 'ACC', 'COV', 'IDENT', 'ISSUED', 'MISSES')
    formats = ("%-*s", "%*.2f", "%*.2f", "%*.2f", "%*.2f", "%*d", "%*d", "%*d")
    widths = (15, 4, 7, 4, 4, 9, 9, 9)
    rows = []

    # filter out the needed prefetcher/test combos
    if pf == 'all':
        table = dict((p, compare(pf_stats, p, t)) for p,d in pf_stats.items() for t in d if test==t)
    else:
        table = dict((t, compare(pf_stats, pf, t)) for t in pf_stats[pf] if test=='all' or test==t)

    for k, t in sorted(table.items(), key=lambda x: x[1]['speedup']):
        ipc = t.get('ipc', 0)
        speedup = t.get('speedup', 0)
        acc = t.get('accuracy', 0)
        cov = t.get('coverage', 0)
        ident = t.get('pf_identified', 0)
        issued = t.get('pf_issued', 0)
        misses = t.get('pf_misses', 0)
        rows.append([k, ipc, speedup, acc, cov, ident, issued, misses])

    if type_ == 'text':
        return format_table_text(caption, headers, formats, widths, rows)
    elif type_ == 'html':
        return format_table_html(caption, headers, formats, widths, rows)
    else:
        raise RuntimeError('unknown output type: %s' % type_)


def summary(pf_stats, type_):
    """ Returns aggregate test results for each prefetcher. """

    caption = 'OVERALL PERFORMANCE'
    headers = ('PREFETCHER', 'SPEEDUP')
    formats = ("%-*s", "%*.2f", "%*.2f", "%*.2f")
    widths = (24, 7)
    rows = []

    for pf, table in sorted(pf_stats.items()):
        data = [compare(pf_stats, pf, test) for test in table]
        speedup = harmonic_mean([d.get('speedup', 0) for d in data])
        rows.append([pf, speedup])

    if type_ == 'text':
        return format_table_text(caption, headers, formats, widths, rows, padding=3)
    elif type_ == 'html':
        return format_table_html(caption, headers, formats, widths, rows, padding=10)
    else:
        raise RuntimeError('unknown output type: %s' % type_)


def format_table_text(caption, headers, formats, widths, rows, padding=1):
    """ Formats a table as text, ready to be pretty-printed. """

    sep = ' ' * padding
    W = sum(widths) + len(sep) * (len(widths) + 1)
    lines = []
    a = lines.append
    a('')
    a(str.center(caption, W))
    a('-' * W)
    a(sep + sep.join(s.center(w) for s,w in zip(headers, widths)))
    a('-' * W)
    for row in rows:
        a(sep + sep.join(format % (width, field) for format, width, field in zip(formats, widths, row)))
    a('-' * W)
    a('')

    return os.linesep.join(lines)


def format_table_html(caption, headers, formats, widths, rows, padding=5):
    """ Formats a table as HTML. """

    lines = []
    a = lines.append
    a("<table border='0' cellpadding='%d' cellspacing='0'>" % padding)
    a("<caption>%s</caption>" % caption)
    a('<thead><tr>')
    for h in headers:
        a("\t<th style='border-top: medium solid black; border-bottom: medium solid black;'>%s</th>" % h)
    a('</tr></thead>')
    a('<tbody>')
    for row in rows:
        a('\t<tr>' + ' '.join("<td style='text-align: %s'>%s</td>" % (('left', 'right')[type(field)!=str], escape(format % (width, field))) for format, width, field in zip(formats, widths, row)) + '</tr>')
    a('</tbody>')
    a("<tfoot><tr><td style='border-top: medium solid black' colspan='%d'>&nbsp;</td></tr></tfoot>" % len(headers))
    a('</table>')

    return os.linesep.join(lines)


def build_stats(path, pf_stats=None):
    """
    Returns a nested dictionary structure where
    * each entry is a pair {pf_name: tests} where tests is another dict
    * each 'tests' dict is a pair {test_name: table} where 'table'
      is the final dictionary of {pattern_name: values} pairs
    * the pattern names are given in global PATTERN table
    * the values are ints, except for 'ipc' which is a float
    """
    if pf_stats is None:
        pf_stats = {}

    # Read and parse all statistics files from M5.
    for d in os.listdir(path):
        dirpath = os.path.join(path, d)
        if not os.path.isdir(dirpath):
            continue
        test, pf = d.split(PF_TEST_SEP, 1)

        statsfile = os.path.join(dirpath, DATAFILE)
        if os.path.exists(statsfile):
            f = open(statsfile, 'r')
            lines = map(str.strip, f.readlines())
            f.close()
        else:
            print >> sys.stderr, "No statistics file found in %s" % dirpath
            continue

        if len(lines) == 0:
            print >> sys.stderr,  "Empty statistics file in %s" % dirpath
            continue

        if pf not in pf_stats:
            pf_stats[pf] = {}
        if test not in pf_stats[pf]:
            pf_stats[pf][test] = {}

        parse(lines, pf_stats[pf][test])

    return pf_stats


def dump_stats(filename, pf_stats):
    """ Dump stats dictionary to file in python format. """
    f = open(filename, 'w')
    print >> f, repr(pf_stats)
    f.close()


def read_stats(*filenames):
    """ Reads in a dictionary of statistics and returns it. """
    d = {}
    for filename in filenames:
        try:
            f = open(filename, 'r')
            d.update(eval(f.readline(), {}, {}))
            f.close()
        except IOError:
            print >> sys.stderr, "Could not read saved statistics from %s" % filename
    return d


def main():
    if (len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help')) or (len(sys.argv) > 3):
        print "USAGE:   stats.py [prefetcher] [test]"
    else:
        home = os.environ['HOME']
        statsdir = os.path.join(home, 'm5files', 'stats')
        # build up stats from all runs
        pf_stats = {}
        for f in os.listdir(statsdir):
            filename = os.path.join(statsdir, f)
            if f.endswith('1e8'):
                pf_stats.update(read_stats(filename))

        pf_stats.update(build_stats('.'))

        if BASELINE_PF not in pf_stats:
            print >> sys.stderr, 'Missing baseline prefetcher [%s]' % BASELINE_PF

        if pf_stats:
            print format_stats(pf_stats, *sys.argv[1:])


if __name__ == '__main__':
    main()
