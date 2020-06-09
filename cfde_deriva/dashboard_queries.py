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
            #'list_projects': list(self.list_projects()),
            'list_root_projects': list(self.list_root_projects()),
            #'list_datatypes': list(self.list_datatypes()),
            #'list_formats': list(self.list_formats()),
            'list_project_file_stats': list(self.list_project_file_stats()),
            'list_project_assaytype_file_stats': list(self.list_project_assaytype_file_stats()),
            'list_project_anatomy_file_stats': list(self.list_project_anatomy_file_stats()),
            #'list_program_file_stats_by_time_bin': list(self.list_project_file_stats_by_time_bin()),
            #'running_sum_project_file_stats': list(self.running_sum_project_file_stats()),
        }
        print(json.dumps(results, indent=2))

    def list_projects(self):
        """Return list of projects AKA funded activities

        """
        # trivial case: just return entities of a single table
        return self.builder.CFDE.project.path.entities().fetch()

    def list_root_projects(self):
        """Return list of root projects, i.e. those not listed as child projects of any other.
        """
        # use pre-computed table to find subset of project table
        path = self.builder.CFDE.project_root.path
        # inner join on foreign key from project_root -> project
        path.link(self.builder.CFDE.project)
        # returns entities from last table in path (project)
        return path.entities().fetch()

    def list_datatypes(self):
        """Return list of data_type terms
        """
        return self.builder.CFDE.data_type.path.entities().fetch()
    
    def list_formats(self):
        """Return list of file format terms
        """
        return self.builder.CFDE.file_format.path.entities().fetch()

    def list_project_file_stats(self, project_id_pair=None, use_root_projects=True, extras=(lambda builder, path: path, lambda path: [], lambda path: [])):
        """Return list of file statistics per project.

        :param project_pair_id: Only summarize a specific (project_id_namespace, project_id) pair.
        :param use_root_projects: Summarize by root project rather than attributed sub-projects (default true).
        :param extras: tuple of extension functions to invoke (path_func, groupkey_func, attribute_thunk).

        The extras functions allow composition of more
        material. Effectively, they modify the result as follows:

           return path_func(builder, core_path)
           .groupby(* core_groupkeys + groupkey_func(path))
           .attributes(* core_attributes + attributes_func(path))
           .fetch()

        NOTE: this query will not return a row for a program with zero files...

        NOTE: only non-null File.size_in_bytes values are summed, so null may
        be returned if none of the files have specified a length...

        """
        if use_root_projects:
            # start with smaller set of root projects
            path = self.builder.CFDE.project_root
            path = path.link(self.builder.CFDE.project)
            # linked as leader...
            path = path.link(
                self.builder.CFDE.project_in_project_transitive,
                on=(
                    (path.project.id_namespace
                     == self.builder.CFDE.project_in_project_transitive.leader_project_id_namespace)
                    & (path.project.id
                       == self.builder.CFDE.project_in_project_transitive.leader_project_id)
                )
            )
            # to actual project attribution of file
            path = path.link(
                self.builder.CFDE.file,
                on=(
                    (path.project_in_project_transitive.member_project_id_namespace
                     == self.builder.CFDE.file.project_id_namespace)
                    & (path.project_in_project_transitive.member_project_id
                       == self.builder.CFDE.file.project)
                )
            )
        else:
            # start with full project set
            path = self.builder.CFDE.project
            # linked as project attribution of file
            path = path.link(self.builder.CFDE.file)

        extend_path, extend_groupkeys, extend_attributes = extras
        path = extend_path(self.builder, path)

        if project_id_pair is not None:
            # add filter for one (project_id_namespace, project_id)
            project_id_ns, project_id = project_id_pair
            path.filter(path.project.id_namespace == project_id_ns)
            path.filter(path.project.id == project_id)
        # and return grouped aggregate results
        results = path.groupby(
            *(
                [
                    path.project.id_namespace.alias('project_id_namespace'),
                    path.project.id.alias('project_id')
                ] + extend_groupkeys(path)
            )
        ).attributes(
            *(
                [
                    Cnt(path.file).alias('file_cnt'),
                    Sum(path.file.size_in_bytes).alias('byte_cnt'),
                    # .name is part of API so need to use dict-style lookup of column...
                    path.project.column_definitions['name'].alias('project_name'),
                    path.project.RID.alias('project_RID')
                ] + extend_attributes(path)
            )
        )
        return results.fetch()

    @classmethod
    def extend_file_path_for_assaytype(cls, builder, path):
        # use explicit join in case file is not last element of path
        # i.e. due to stacked extensions
        return path.link(
            builder.CFDE.file_assay_type,
            on=( (path.file.id_namespace == builder.CFDE.file_assay_type.file_id_namespace)
                 & (path.file.id == builder.CFDE.file_assay_type.file_id) )
        ).link(builder.CFDE.assay_type)

    @classmethod
    def extend_file_groupkeys_for_assaytype(cls, path):
        return [
            path.assay_type.id
        ]

    @classmethod
    def extend_file_attributes_for_assaytype(cls, path):
        return [
            path.assay_type.column_definitions['name'].alias('assay_type_name')
        ]

    def list_project_assaytype_file_stats(self, project_id_pair=None):
        """Return list of file statistics per (project, assay_type).

        Like list_project_file_stats, but also include biosample
        assay_type in the group key, for more detailed result
        categories.

        """
        extras = (
            self.extend_file_path_for_assaytype,
            self.extend_file_groupkeys_for_assaytype,
            self.extend_file_attributes_for_assaytype
        )
        return self.list_project_file_stats(project_id_pair=project_id_pair, extras=extras)

    @classmethod
    def extend_file_path_for_anatomy(cls, builder, path):
        # use explicit join in case file is not last element of path
        # i.e. due to stacked extensions
        return path.link(
            builder.CFDE.file_anatomy,
            on=( (path.file.id_namespace == builder.CFDE.file_anatomy.file_id_namespace)
                 & (path.file.id == builder.CFDE.file_anatomy.file_id) )
        ).link(builder.CFDE.anatomy)

    @classmethod
    def extend_file_groupkeys_for_anatomy(cls, path):
        return [
            path.anatomy.id
        ]

    @classmethod
    def extend_file_attributes_for_anatomy(cls, path):
        return [
            path.anatomy.column_definitions['name'].alias('anatomy_name')
        ]

    def list_project_anatomy_file_stats(self, project_id_pair=None):
        """Return list of file statistics per (project, anatomy).

        Like list_project_file_stats, but also include biosample
        anatomy in the group key, for more detailed result
        categories.

        """
        extras = (
            self.extend_file_path_for_anatomy,
            self.extend_file_groupkeys_for_anatomy,
            self.extend_file_attributes_for_anatomy
        )
        return self.list_project_file_stats(project_id_pair=project_id_pair, extras=extras)

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
