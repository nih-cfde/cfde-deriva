#!/usr/bin/python3

# This script assumes latest deriva-py/master library
# talking to latest ermrest/master server

import sys
import os
import json
from deriva.core import ErmrestCatalog, urlquote
from deriva.core.datapath import Min, Max, Cnt, CntD, Avg, Sum, Bin

class DashboardQueryHelper (object):

    def __init__(self, hostname, catalogid, scheme='https'):
        self.catalog = ErmrestCatalog(scheme, hostname, catalogid)
        self.builder = self.catalog.getPathBuilder()

    def run_demo(self):
        """Run each example query and dump all results as JSON."""
        # use list() to convert each ResultSet
        # for easier JSON serialization...
        results = {
            'list_projects': list(self.list_projects()),
            'list_datatypes': list(self.list_datatypes()),
            'list_formats': list(self.list_formats()),
            'list_project_role_taxonomy_stats': list(self.list_project_role_taxonomy_stats()),
            'list_project_file_stats': list(self.list_project_file_stats()),
            'list_project_assaytype_file_stats': list(self.list_project_assaytype_file_stats()),
            'list_program_file_stats_by_time_bin': list(self.list_project_file_stats_by_time_bin()),
            'running_sum_project_file_stats': list(self.running_sum_project_file_stats()),
        }
        print(json.dumps(results, indent=2))

    def list_projects(self):
        """Return list of projects AKA funded activities

        """
        # trivial case: just return entities of a single table
        return self.builder.CFDE.project.path.entities().fetch()

    def list_datatypes(self):
        """Return list of data_type terms
        """
        return self.builder.CFDE.data_type.path.entities().fetch()
    
    def list_formats(self):
        """Return list of file format terms
        """
        return self.builder.CFDE.file_format.path.entities().fetch()

    def list_project_file_stats(self, project_id_pair=None):
        """Return list of file statistics per project.

        Optionally filtered to a single project (namespace, id) pair.

        NOTE: this query will not return a row for a program with zero files...

        NOTE: only non-null File.size_in_bytes values are summed, so null may
        be returned if none of the files have specified a length...

        """
        path = self.builder.CFDE.file.path
        if project_id_pair is not None:
            # add filter for one (project_id_namespace, project_id)
            project_id_ns, project_id = project_id_pair
            path.filter(path.file.project_id_namespace == project_id_ns)
            path.filter(path.file.project == project_id)
        # and return grouped aggregate results
        results = path.groupby(
            path.file.project_id_namespace,
            path.file.project
        ).attributes(
            Cnt(path.file).alias('file_cnt'),
            Sum(path.file.size_in_bytes).alias('byte_cnt'),
        )
        return results.fetch()

    def list_project_role_taxonomy_stats(self, subject_role=None):
        """Return list of statistics per (project, role, taxonomy)
        """
        # build 2 parallel query structures for 2 source tables
        path1 = self.builder.CFDE.subject.path
        path2 = self.builder.CFDE.file.path

        path1.link(self.builder.CFDE.subject_role_taxonomy)
        path2.link(self.builder.CFDE.file_subject_role_taxonomy)

        if subject_role is not None:
            # add filters for one subject role
            path1.filter(path.subject_role_taxonomy.role_id == subject_role)
            path2.filter(path.file_subject_role_taxonomy.subject_role_id == subject_role)

        # build group keys that we will reuse
        groupkey1 = (
            path1.subject.project_id_namespace,
            path1.subject.project,
            path1.subject_role_taxonomy.role_id,
            path1.subject_role_taxonomy.taxonomy_id
        )
        groupkey2 = (
            path2.file.project_id_namespace,
            path2.file.project,
            path2.file_subject_role_taxonomy.subject_role_id,
            path2.file_subject_role_taxonomy.subject_taxonomy_id
        )

        # define grouped and sorted aggregates
        results1 = path1.groupby(*groupkey1).attributes(
            Cnt(path1.subject).alias("num_subjects")
        ).sort(*groupkey1)
        results2 = path2.groupby(*groupkey2).attributes(
            Cnt(path2.file).alias("num_files")
        ).sort(*groupkey2)

        # fetch results and prepare for client-side merge
        results1 = {
            (row['project_id_namespace'],
             row['project'],
             row['role_id'],
             row['taxonomy_id']): row
            for row in results1.fetch()
        }
        results2 = {
            (row['project_id_namespace'],
             row['project'],
             row['subject_role_id'],
             row['subject_taxonomy_id']): row
            for row in results2.fetch()
        }
        # get all group keys and do merge
        all_groups = set(results1) & set(results2)
        return [
            {
                'project_id_namespace': group[0],
                'project': group[1],
                'role_id': group[2],
                'taxonomy_id': group[3],
                # pretend we got a null count back if group is absent in one of the result sets
                'num_subjects': results1.get(group, {"num_subjects": None})['num_subjects'],
                'num_files': results2.get(group, {"num_files":None})['num_files'],
            }
            for group in all_groups
        ]

    def list_project_assaytype_file_stats(self, project_id_pair=None):
        """Return list of file statistics per (project, assay_type).

        Like list_project_file_stats, but also include biosample
        assay_type in the group key, for more detailed result
        categories.

        """
        # include vocab table for human-readable assay_type.name field
        path = self.builder.CFDE.assay_type.path
        path.link(self.builder.CFDE.biosample)
        path.link(self.builder.CFDE.file_describes_biosample)
        # right-outer join so we can count files w/o this biosample/assay_type linkage
        path.link(
            self.builder.CFDE.file,
            on=( (path.file_describes_biosample.file_id_namespace == self.builder.CFDE.file.id_namespace)
                 & (path.file_describes_biosample.file_id == self.builder.CFDE.file.id) ),
            join_type='right'
        )
        if project_id_pair is not None:
            # add filter for one (project_id_namespace, project_id)
            project_id_ns, project_id = project_id_pair
            path.filter(path.file.project_id_namespace == project_id_ns)
            path.filter(path.file.project == project_id)
        # and return grouped aggregate results
        results = path.groupby(
            # compound grouping key
            path.file.project_id_namespace,
            path.file.project,
            path.biosample.assay_type.alias('assay_type_id'),
        ).attributes(
            # 'name' is part of Table API so we cannot use attribute-based lookup...
            path.assay_type.column_definitions['name'].alias('assay_type_name'),
            Cnt(path.file).alias('file_cnt'),
            Sum(path.file.size_in_bytes).alias('byte_cnt'),
        )
        return results.fetch()

    def list_project_file_stats_by_time_bin(self, nbins=100, min_ts='2010-01-01', max_ts='2020-12-31'):
        """Return list of file statistics per (project_id_namespace, project, ts_bin)

        :param nbins: The number of bins to divide the time range
        :param min_ts: The lower (closed) bound of the time range
        :param max_ts: The upper (open) bound of the time range

        If min_ts or max_ts are unspecified, preliminary queries are
        performed to determine the actual timestamp range found in the
        source data. These values are used to configure the binning
        distribution.

        Files generation times are found from file.creation_time which
        may be NULL.

        Results are keyed by project and ts_bin group keys.

        NOTE: Results are sparse! Groups are only returned when at
        least one matching row is found. This means that some bins,
        described next, may be absent in a particular query result.

        Each group includes a ts_bin field which is a three-element
        list describing the time bin:

           [ bin_number, lower_bound, upper_bound ]

        The files within the selected range will be summarized in groups
        with bins:

           [ 1, min_ts, (max_ts - min_ts)/nbins ]
           ...
           [ nbins, max_ts - (max_ts - min_ts)/nbins, max_ts ]

        Files without known creation_time will be summarized in a row
        with a special null bin:

        Other files will be summarized in rows with special bins:

           [ null, null, null ]
           [ 0, null, min_ts ]
           [ nbins+1, max_ts, null ]

        i.e. for files with NULL creation_time, with creation_time
        below min_ts, or with creation_time above max_ts,
        respectively.

        HACK: setting non-null default min_ts and max_ts to work
        around failure mode when the entire catalog only has null
        creation_time values (i.e. in a limited test load)...

        """
        path = self.builder.CFDE.file.path

        # build this list once so we can reuse it for grouping and sorting
        groupkey = [
            path.file.project_id_namespace,
            path.file.project,
            Bin(path.file.creation_time, nbins, min_ts, max_ts).alias('ts_bin'),
        ]

        results = path.groupby(
            *groupkey
        ).attributes(
            Cnt(path.file).alias('file_cnt'),
            Sum(path.file.size_in_bytes).alias('byte_cnt'),
        ).sort(
            *groupkey
        )
        return results.fetch()

    def running_sum_project_file_stats(self, nbins=100, min_ts='2010-01-01', max_ts='2020-12-31'):
        """Transform results of list_project_file_stats_by_time to produce running sums

        The underlying query counts files and sums bytecounts only
        within each time bin. I.e. it represents change rather than
        total data capacities at given times.

        This function accumulates values to show total capacity trends.

        """
        project = None
        file_cnt = None
        byte_cnt = None
        # because underlying query results are sorted, we can just iterate...
        for row in self.list_project_file_stats_by_time_bin(nbins, min_ts, max_ts):
            if (project != (row['project_id_namespace'], row['project'])):
                # reset state for next group
                project = (row['project_id_namespace'], row['project'])
                file_cnt = 0
                byte_cnt = 0
            if row['file_cnt'] is not None:
                file_cnt += row['file_cnt']
            if row['byte_cnt'] is not None:
                byte_cnt += row['byte_cnt']
            yield {
                'project_id_namespace': row['project_id_namespace'],
                'project': row['project'],
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
