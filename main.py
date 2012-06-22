#!/usr/bin/env python
"""
This program analyzes data from NOAA weather stations.

Copyright 2012 Jeff Laughlin Consulting LLC, All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import argparse
from collections import namedtuple
import datetime
import logging
import os
import os.path
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO
import sys
import gzip

import requests
import requests_cache
import pandas
import numpy as np
import matplotlib.pyplot as plt


CACHE_DIR = os.path.join(os.environ['HOME'], '.wx')

ROOT = 'http://www1.ncdc.noaa.gov/pub/data/gsod'

ISH_HISTORY = 'ish-history.txt'


StationHistory = namedtuple('StationHistory', 
    ('usaf', 'wban', 'name', 'country', 'state', 'lat', 'lon', 'el', 'begin', 'end'))


def parse_history_line(line):
    """Parse a line of station history data and return a StationHistory object."""

    try: usaf = int(line[0:6])
    except ValueError: usaf = None

    try: wban = int(line[7:12])
    except ValueError: wban = None

    name = line[13:43]
    country = line[43:45]
    state = line[49:51]

    try: lat = float(line[58:64]) / 1000.0
    except ValueError: lat = None
        
    try: lon = float(line[65:72]) / 1000.0
    except ValueError: lon = None

    try: el = float(line[73:79]) / 10.0
    except ValueError: el = None

    try:
        begin_year = int(line[83:87])
        begin_mo = int(line[87:89])
        begin_day = int(line[89:91])
        begin = datetime.date(begin_year, begin_mo, begin_day)
    except ValueError:
        begin = None

    try:
        end_year = int(line[92:96])
        end_mo = int(line[96:98])
        end_day = int(line[98:100])
        end = datetime.date(end_year, end_mo, end_day)
    except ValueError:
        end = None

    sh = StationHistory(
        usaf = usaf,
        wban = wban,
        name = name,
        country = country,
        state = state,
        lat = lat,
        lon = lon,
        el = el,
        begin = begin,
        end = end,
    )
    return sh


def get_station_histories():
    """Return a list of StationHistory objects."""
    # TODO: Save the history in a sqlite db or something.
    ish = requests.get('/'.join((ROOT, ISH_HISTORY)))
    got_header = False
    sh_list = []
    for line in ish.text.split('\n')[22:]:
        try:
            sh_list.append(parse_history_line(line))
        except Exception:
            logging.error("Error processing line '%s'" % line, exc_info=True)
    return sh_list


def parse_gsod_line(line):
    """Given a line of text from a GSOD file, return a tuple of (date, t_mean,
    t_max, t_min)"""
    t_mean = float(line[25:30])
    t_mean = t_mean if t_mean < 9000 else float('nan')
    t_max = float(line[102:108])
    t_max = t_max if t_max < 9000 else float('nan')
    t_min = float(line[110:116])
    t_min = t_min if t_min < 9000 else float('nan')
    year = int(line[14:18])
    mo = int(line[18:20])
    day = int(line[20:22])
    date = datetime.date(year, mo, day)
    return date, t_mean, t_max, t_min


def get_wban(wban, station_histories):
    """Fetch historical data for wban."""
    # Could build an index but since we're only looking up one thing a linear
    # search is probably fastest.
    t_mean_list = []
    t_max_list = []
    t_min_list = []
    idx_list = []
    wban_sh = None
    for sh in station_histories:
        if wban == sh.wban:
            wban_sh = sh
            break
    if wban_sh is None:
        raise Exception("Couldn't find wban %s in station histories." % wban)
    for year in xrange(wban_sh.begin.year, wban_sh.end.year + 1):
        filename = '%06d-%05d-%04d.op.gz' % (wban_sh.usaf, wban_sh.wban, year)
        url = '/'.join((ROOT, '%04d' % year, filename))
        r = requests.get(url)
        content = r.content
        content_f = StringIO.StringIO(content)
        content = gzip.GzipFile(fileobj=content_f).read()
        for line in content.split('\n')[1:]:
            try:
                date, t_mean, t_max, t_min = parse_gsod_line(line)
                doy = date.timetuple()[7]
                t_mean_list.append(t_mean)
                t_max_list.append(t_max)
                t_min_list.append(t_min)
                idx_list.append((year, doy))
            except Exception:
                if len(line) > 0:
                    logging.warn("Failed to parse line '%s'" % line, exc_info=True)
    data = pandas.DataFrame(
        {
            't_mean': t_mean_list,
            't_max': t_max_list,
            't_min': t_min_list,
        },
        index = pandas.MultiIndex.from_tuples(idx_list, names=['year', 'doy'])
    )
    return data


def process_data(data):
    """Analyze historical data for wban."""
    print data
    print
    print "All time record high temp: %s" % data.t_max.max()
    print "All time record high mean temp: %s" % data.t_mean.max()
    print "All time record low temp: %s" % data.t_min.min()
    print "All time record low mean temp: %s" % data.t_mean.min()
    print
    cleaned = data.dropna()
    annual = cleaned.groupby(level='year').mean()
    plt.show(annual.plot())
    annual = cleaned.groupby(level='year').max()
    plt.show(annual.plot())
    annual = cleaned.groupby(level='year').min()
    plt.show(annual.plot())


def _setup_cache():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    requests_cache.configure(os.path.join(CACHE_DIR, 'cache'))


def main(argv=None):
    if argv is None:
        argv = sys.argv
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--wban')
    ap.add_argument('-v', '--verbose', action='store_true', default=False)
    args = ap.parse_args(argv[1:])
    if args.verbose:
        logging.getLogger('').setLevel(logging.DEBUG)
    try:
        _setup_cache()
        station_histories = get_station_histories()
        if args.wban is not None:
            data = get_wban(int(args.wban), station_histories)
            process_data(data)
    except:
        logging.critical("Exiting due to error", exc_info=True)
        return -1
    logging.debug("Exiting normally.")
    return 0


if __name__ == '__main__':
    sys.exit(main())

