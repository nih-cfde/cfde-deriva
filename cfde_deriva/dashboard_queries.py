#!/usr/bin/python3

# This script assumes latest deriva-py/master library
# talking to latest ermrest/master server

import sys
import os
import json
from deriva.core import ErmrestCatalog, urlquote
from deriva.core.datapath import Min, Max, Cnt, CntD, Avg, Sum

class DashboardQueryHelper (object):

    def __init__(self, hostname, catalogid, scheme='https'):
        self.catalog = ErmrestCatalog(scheme, hostname, catalogid)
        self.builder = self.catalog.getPathBuilder()

    def run_demo(self):
        """Run each example query and dump all results as JSON."""
        # use list() to convert each ResultSet
        # for easier JSON serialization...
        results = {
            'list_programs': list(self.list_programs()),
            'list_infotypes': list(self.list_infotypes()),
            'list_formats': list(self.list_formats()),
            'list_program_file_stats': list(self.list_program_file_stats()),
            'list_program_infotype_format_file_stats': list(self.list_program_infotype_format_file_stats()),
            'list_program_file_stats_by_time_bin': list(self.list_program_file_stats_by_time_bin()),
            'running_sum_program_file_stats': list(self.running_sum_program_file_stats()),
        }
        print(json.dumps(results, indent=2))

    def list_programs(self):
        """Return list of common fund programs

        NOTE: in demo content, program 'name' is NOT unique, e.g. GTEx
        occurs twice due to overlap in imports!  The 'id' column is
        unique.

        """
        # trivial case: just return entities of a single table
        return self.builder.CFDE.CommonFundProgram.path.entities().fetch()

    def list_infotypes(self):
        """Return list of information type terms
        """
        return self.builder.CFDE.InformationType.path.entities().fetch()
    
    def list_formats(self):
        """Return list of file format terms
        """
        return self.builder.CFDE.FileFormat.path.entities().fetch()
    
    def list_program_file_stats(self, programid=None):
        """Return list of file statistics per program.

        Optionally filtered to a single programid.

        NOTE: this query will not return a row for a program with zero files...

        NOTE: only non-null File.length values are summed, so null may
        be returned if none of the files have specified a length...

        """
        # more complex case: build joined table path
        path = self.builder.CFDE.Dataset.path
        if programid is not None:
            path.filter(path.Dataset.data_source == programid)
        path.link(self.builder.CFDE.FilesInDatasets)
        path.link(self.builder.CFDE.File)
        # and return grouped aggregate results
        results = path.groupby(
            path.Dataset.data_source  # the grouping key: a programid
        ).attributes(
            Cnt(path.File).alias('file_cnt'),
            Sum(path.File.length).alias('byte_cnt'),
        )
        return results.fetch()

    def list_program_infotype_format_file_stats(self, programid=None):
        """Return list of file statistics per (program, infotype, format).

        Like list_program_file_stats, but also include
        information_type and file_format in the group key, for more
        detailed results.

        """
        # much like list_program_file_stats, but with compound grouping keys
        path = self.builder.CFDE.Dataset.path
        if programid is not None:
            path.filter(path.Dataset.data_source == programid)
        path.link(self.builder.CFDE.FilesInDatasets)
        path.link(self.builder.CFDE.File)
        results = path.groupby(
                # compound grouping key
                path.Dataset.data_source,
                path.File.information_type,
                path.File.file_format
        ).attributes(
            Cnt(path.File).alias('file_cnt'),
            Sum(path.File.length).alias('byte_cnt'),
        )
        return results.fetch()

    def list_program_file_stats_by_time_bin(self, nbins=100, min_ts=None, max_ts=None):
        """Return list of file statistics per (data_source, ts_bin)

        :param nbins: The number of bins to divide the time range
        :param min_ts: The lower (closed) bound of the time range
        :param max_ts: The upper (open) bound of the time range

        If min_ts or max_ts are unspecified, preliminary queries are
        performed to determine the actual timestamp range found in the
        source data. These values are used to configure the binning
        distribution.

        Files generation times are found from DataEvent.event_ts where
        linked to File by the GeneratedBy association. Files without
        such linkage are considered to have null event times.

        Results are keyed by data_source and ts_bin group keys.

        Each group includes a ts_bin field which is a three-element
        list describing the time bin:

           [ bin_number, lower_bound, upper_bound ]

        The files within the selected range will be summarized in groups
        with bins:

           [ 1, min_ts, (max_ts - min_ts)/nbins ]
           ...
           [ nbins, max_ts - (max_ts - min_ts)/nbins, max_ts ]

        Files without known event_ts will be summarized in a row with
        a special null bin:

        Other files will be summarized in rows with special bins:

           [ null, null, null ]
           [ 0, null, min_ts ]
           [ nbins+1, max_ts, null ]

        i.e. for files with unknown event_ts, with event_ts below
        min_ts, or with event_ts above max_ts, respectively.

        """
        path = self.builder.CFDE.Dataset.path
        path.link(self.builder.CFDE.FilesInDatasets)
        path.link(self.builder.CFDE.File)
        path.link(self.builder.CFDE.GeneratedBy)
        path.link(self.builder.CFDE.DataEvent)
        bounds = path.aggregates(
            Min(path.DataEvent.event_ts).alias('min_ts'),
            Max(path.DataEvent.event_ts).alias('max_ts'),
        ).fetch()[0]
        if min_ts is None:
            min_ts = bounds['min_ts']
        if max_ts is None:
            max_ts = bounds['max_ts']
        if min_ts is None or max_ts is None:
            raise ValueError('Time range [%s, 0) indeterminate' % (min_ts, max_ts))
        # build custom ermrest URL to access binning feature
        # (not yet available via datapath API)
        url = (
            '/attributegroup'
            '/D:=Dataset'
            '/FilesInDatasets'
            '/F:=File'
            '/A:=left(F:id)=(GeneratedBy:FileID)'
            '/E:=left(A:DataEventID)=(DataEvent:id)'
            '/D:data_source'
            + (',ts_bin:=bin(event_ts;%d;%s;%s)' % (nbins, urlquote(min_ts), urlquote(max_ts))) +
            ';file_cnt:=cnt(F:id)'
            ',byte_cnt:=sum(F:length)'
            '@sort(data_source,ts_bin)'
        )
        return self.catalog.get(url).json()

    def running_sum_program_file_stats(self, nbins=100, min_ts=None, max_ts=None):
        """Transform results of list_program_file_stats_by_time to produce running sums

        The underlying query counts files and sums bytecounts only
        within each time bin. I.e. it represents change rather than
        total data capacities at given times.

        This function accumulates values to show total capacity trends.

        """
        data_source = None
        file_cnt = None
        byte_cnt = None
        # because underlying query results are sorted, we can just iterate...
        for row in self.list_program_file_stats_by_time_bin(nbins, min_ts, max_ts):
            if data_source != row['data_source']:
                # reset state for next group
                data_source = row['data_source']
                file_cnt = 0
                byte_cnt = 0
            if row['file_cnt'] is not None:
                file_cnt += row['file_cnt']
            if row['byte_cnt'] is not None:
                byte_cnt += row['byte_cnt']
            yield {
                'data_source': data_source,
                'ts_bin': row['ts_bin'],
                'file_cnt': file_cnt,
                'byte_cnt': byte_cnt
            }
   

## ugly CLI wrapping...
def main():
    """Runs demo of catalog dashboard queries."""
    hostname = os.getenv('DERIVA_SERVERNAME', 'cfde.derivacloud.org')
    catalogid = os.getenv('DERIVA_CATALOGID', '4')
    db = DashboardQueryHelper(hostname, catalogid)
    db.run_demo()
    return 0

if __name__ == '__main__':
    exit(main())
