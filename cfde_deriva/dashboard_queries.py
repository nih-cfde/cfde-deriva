#!/usr/bin/python3

# This script assumes latest deriva-py/master library
# talking to latest ermrest/master server

import sys
import os
import json
from deriva.core import ErmrestCatalog, urlquote
from deriva.core.datapath import Min, Max, Cnt, CntD, Avg, Sum, Bin

######
# utility functions to idempotently build up the path in a StatsQuery object
#
# based on minimal subset of this possible core-entity query path:
#
#   file -- file-describes-biosample -- biosample -- biosample-from-subject -- subject
#
# attaching vocabulary tables as needed to access terms:
#
#   file -- data_type
#   biosample -- anatomy
#   biosample -- assay_type
#   subject -- subject_role_taxonomy -- ncbi_taxonomy (species)
#   [stats entity] -- project_in_project_transitive -- project -- project_root
#
# when core path is extended, grouped dimension terms are correlated
# by shared table instances for all intermediate tables, i.e.
#
#    anatomy X assay_type  come from same biosample
#    anatomy X species consider a biosample and its matching subject
#

def _add_file_path(queryobj):
    """Idempotently add file to path and set it as path context"""
    if 'file' not in queryobj.path.table_instances:
        _add_biosample_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.file_describes_biosample)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.file)
    else:
        queryobj.path.context = queryobj.path.file

def _add_biosample_path(queryobj):
    """Idempotently add biosample to path and set it as path context"""
    if 'biosample' not in queryobj.path.table_instances:
        if 'file' in queryobj.path.table_instances:
            queryobj.path.context = queryobj.path.file
            queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.file_describes_biosample)
        elif 'subject' in queryobj.path.table_instances:
            queryobj.path.context = queryobj.path.subject
            queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.biosample_from_subject)
        else:
            raise NotImplementedError('prerequisites not met for _add_biosample_path')
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.biosample)
    else:
        queryobj.path.context = queryobj.path.biosample

def _add_subject_path(queryobj):
    """Idempotently add subject to path and set it as path context"""
    if 'subject' not in queryobj.path.table_instances:
        _add_biosample_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.biosample_from_subject)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.subject)
    else:
        queryobj.path.context = queryobj.path.subject

def _add_datatype_path(queryobj, **kwargs):
    """Idempotently add data_type to path"""
    if 'data_type' not in queryobj.path.table_instances:
        _add_file_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.data_type)

def _add_anatomy_path(queryobj, **kwargs):
    """Idempotently add anatomy to path"""
    if 'anatomy' not in queryobj.path.table_instances:
        _add_biosample_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.anatomy)

def _add_assaytype_path(queryobj, **kwargs):
    """Idempotently add assay_type to path"""
    if 'assay_type' not in queryobj.path.table_instances:
        _add_biosample_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.assay_type)

def _add_species_path(queryobj, **kwargs):
    """Idempotently add species concept to path"""
    if 'species' not in queryobj.path.table_instances:
        _add_subject_path(queryobj)
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.subject_role_taxonomy.alias('srt'))
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.subject_role.alias('subrole'))
        queryobj.path = queryobj.path.filter(queryobj.path.subrole.column_definitions['name'] == 'single organism')
        queryobj.path.context = queryobj.path.srt
        queryobj.path = queryobj.path.link(queryobj.helper.builder.CFDE.ncbi_taxonomy.alias('species'))
        queryobj.path = queryobj.path.filter(queryobj.path.species.clade == 'species')

def _add_rootproject_path(queryobj, **kwargs):
    """Idempotently add root project concept to path"""
    if 'pipt' in queryobj.path.table_instances:
        if 'project_root' not in queryobj.path.table_instances:
            raise TypeError('Cannot combine subproject and project_root dimensions')
        # idempotently add rootproject
        return

    entity = queryobj.path.table_instances[queryobj.entity_name]
    project_root = queryobj.helper.builder.CFDE.project_root
    pipt = queryobj.helper.builder.CFDE.project_in_project_transitive.alias('pipt')
    # need to select directionality of association, so reduce number of joins while we're at it
    queryobj.path = queryobj.path.link(
        pipt,
        on=( (entity.project_id_namespace == pipt.member_project_id_namespace)
             & (entity.project == pipt.member_project_id) )
    ).link(
        project_root,
        on=( (queryobj.path.pipt.leader_project_id_namespace == project_root.project_id_namespace)
             & (queryobj.path.pipt.leader_project_id == project_root.project_id) )
    ).link(
        queryobj.helper.builder.CFDE.project
    )

def _add_subproject_path(queryobj, **kwargs):
    """Idempotently add root project concept to path"""
    if 'pipt' in queryobj.path.table_instances:
        if 'project_root' in queryobj.path.table_instances:
            raise TypeError('Cannot combine subproject and project_root dimensions')
        # idempotently add sub_project
        return

    try:
        parent_project_RID = kwargs['parent_project_RID']
    except KeyError:
        raise TypeError('Missing required parent_project_RID keyword argument in StatsQuery.dimension("subproject", **kwargs) call')

    entity = queryobj.path.table_instances[queryobj.entity_name]
    pipt = queryobj.helper.builder.CFDE.project_in_project_transitive.alias('pipt')
    project = queryobj.helper.builder.CFDE.project
    parentproj = queryobj.helper.builder.CFDE.project.alias('parentproj')

    pip = queryobj.helper.builder.CFDE.project_in_project.alias('pip')
    # need to select directionality of association, so reduce number of joins while we're at it
    queryobj.path = queryobj.path.link(
        pipt,
        on=( (entity.project_id_namespace == pipt.member_project_id_namespace)
             & (entity.project == pipt.member_project_id) )
    ).link(
        pip,
        on=( (queryobj.path.pipt.leader_project_id_namespace == pip.child_project_id_namespace)
             & (queryobj.path.pipt.leader_project_id == pip.child_project_id) )
    ).link(
        parentproj,
        on=( (queryobj.path.pip.parent_project_id_namespace == parentproj.id_namespace)
             & (queryobj.path.pip.parent_project_id == parentproj.id) )
    ).filter(
        queryobj.path.parentproj.RID == parent_project_RID
    ).link(
        project,
        on=( (queryobj.path.pipt.leader_project_id_namespace == project.id_namespace)
             & (queryobj.path.pipt.leader_project_id == project.id) )
    )

class StatsQuery (object):
    """C2M2 statistics query generator

    Construct with a DashboardQueryHelper instance to bind to a
    catalog, select base entity for statistics, and select optional
    dimensions for multi-dimensional grouping of results.

    StatsQuery(helper)
    .entity('file')
    .dimension('data_type')
    .dimension('project_root')
    .fetch()

    Exactly one entity MUST be configured. Zero or more dimensions MAY
    be configured.

    Beware, overly complex queries (with too many dimensions) may
    timeout due to query cost.

    """

    # define supported keys, mapped to implementation bits...
    supported_entities = {
        'file': [
            lambda path: CntD(path.file.RID).alias('num_files'),
            lambda path: Sum(path.file.size_in_bytes).alias('num_bytes'),
        ],
        'biosample': [
            lambda path: CntD(path.biosample.RID).alias('num_biosamples'),
        ],
        'subject': [
            lambda path: CntD(path.subject.RID).alias('num_subjects'),
        ],
    }
    supported_dimensions = {
        'anatomy': (
            _add_anatomy_path, [
                lambda path: path.anatomy.id.alias('anatomy_id'),
            ], [
                lambda path: path.anatomy.column_definitions['name'].alias('anatomy_name'),
            ]
        ),
        'assay_type': (
            _add_assaytype_path, [
                lambda path: path.assay_type.id.alias('assay_type_id'),
            ], [
                lambda path: path.assay_type.column_definitions['name'].alias('assay_type_name'),
            ]
        ),
        'data_type': (
            _add_datatype_path, [
                lambda path: path.data_type.id.alias('data_type_id'),
            ], [
                lambda path: path.data_type.column_definitions['name'].alias('data_type_name'),
            ]
        ),
        'species': (
            _add_species_path, [
                lambda path: path.species.id.alias('species_id'),
            ], [
                lambda path: path.species.column_definitions['name'].alias('species_name'),
            ]
        ),
        'project_root': (
            _add_rootproject_path, [
                lambda path: path.project.RID.alias('project_RID'),
            ], [
                lambda path: path.project.id_namespace.alias('project_id_namespace'),
                lambda path: path.project.id.alias('project_id'),
                lambda path: path.project.column_definitions['name'].alias('project_name'),
            ]
        ),
        'subproject': (
            _add_subproject_path, [
                lambda path: path.project.RID.alias('project_RID'),
            ], [
                lambda path: path.project.id_namespace.alias('project_id_namespace'),
                lambda path: path.project.id.alias('project_id'),
                lambda path: path.project.column_definitions['name'].alias('project_name'),
            ],
        ),
    }

    def __init__(self, helper):
        """Construct a StatsQuery builder object

        :param helper: Instance of DashboardQueryHelper
        """
        self.helper = helper
        self.entity_name = None
        self.included_dimensions = set()
        self.path = None
        self.grpk_funcs = []
        self.attr_funcs = []

    def entity(self, entity_name):
        """Select entity which will be source of statistics

        :param entity_name: One of the StatsQuery.supported_entities key strings
        """
        if self.path is not None:
            raise TypeError('Cannot call .entity() method on a StatsQuery instance more than once.')

        try:
            self.attr_funcs.extend(self.supported_entities[entity_name])
            self.entity_name = entity_name
        except KeyError:
            raise ValueError('Unsupported entity_name "%s"' % (entity_name,))

        self.path = self.helper.builder.CFDE.tables[entity_name].path

        return self

    def dimension(self, dimension_name, **kwargs):
        """Configure a grouping dimension

        :param dimension_name: One of the StatsQuery.supported_dimension key strings
        :param kwargs: Keyword arguments specific to a dimension (see further documentation)

        Dimension-specific keyword arguments:

        :param parent_project_RID: Use sub-projects of specified parent project RID for "subproject" dimension (required)

        """
        if self.path is None:
            raise TypeError('Cannot call .dimension() method on a StatsQuery instance prior to calling .entity() method.')
        if dimension_name in self.included_dimensions:
            raise TypeError('Cannot use dimension_name "%s" more than once in a StatsQuery instance.' % (dimension_name,))

        try:
            add_path_func, grpk_funcs, attr_funcs = self.supported_dimensions[dimension_name]
            self.grpk_funcs.extend(grpk_funcs)
            self.attr_funcs.extend(attr_funcs)
            self.included_dimensions.add(dimension_name)
        except KeyError:
            raise ValueError('Unsupported dimension_name "%s"' % (dimension_name,))

        add_path_func(self, **kwargs)

        return self

    def fetch(self):
        """Fetch results for configured query"""
        if self.path is None:
            raise TypeError('Cannot call .fetch() method on a StatsQuery instance prior to calling .entity() method.')
        if self.grpk_funcs:
            return self.path.groupby(*[
                grpk_func(self.path)
                for grpk_func in self.grpk_funcs
            ]).attributes(*[
                attr_func(self.path)
                for attr_func in self.attr_funcs
            ]).fetch()
        else:
            return self.path.aggregates(*[
                attr_func(self.path)
                for attr_func in self.attr_funcs
            ]).fetch()

class DashboardQueryHelper (object):

    def __init__(self, hostname, catalogid, scheme='https'):
        self.catalog = ErmrestCatalog(scheme, hostname, catalogid)
        self.builder = self.catalog.getPathBuilder()

    def run_demo(self):
        """Run each example query and dump all results as JSON."""
        projects = {
            (row['id_namespace'], row['id']): row
            for row in self.list_projects(use_root_projects=True)
        }

        rid_for_4DN_proj = projects[('https://data.4dnucleome.org', '4DN')]['RID']

        # use list() to convert each ResultSet
        # for easier JSON serialization...
        results = {
            #'list_projects': list(self.list_projects()),
            #'list_root_projects': list(self.list_projects(use_root_projects=True)),
            #'list_datatypes': list(self.list_datatypes()),
            #'list_formats': list(self.list_formats()),

            'root_projects': list(self.list_projects(use_root_projects=True)),
            'subject_stats_assaytype_subproject': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('subproject', parent_project_RID=rid_for_4DN_proj).fetch()),

            'file_stats_anatomy_assaytype': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('assay_type').fetch()),
            'file_stats_anatomy_datatype': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('data_type').fetch()),
            'file_stats_anatomy_species': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('species').fetch()),
            'file_stats_anatomy_project': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('project_root').fetch()),
            'file_stats_assaytype_datatype': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('data_type').fetch()),
            'file_stats_assaytype_species': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('species').fetch()),
            'file_stats_assaytype_project': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('project_root').fetch()),
            'file_stats_datatype_species': list(StatsQuery(self).entity('file').dimension('data_type').dimension('species').fetch()),
            'file_stats_datatype_project': list(StatsQuery(self).entity('file').dimension('data_type').dimension('project_root').fetch()),

            'biosample_stats_anatomy_assaytype': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('assay_type').fetch()),
            'biosample_stats_anatomy_datatype': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('data_type').fetch()),
            'biosample_stats_anatomy_species': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('species').fetch()),
            'biosample_stats_anatomy_project': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('project_root').fetch()),
            'biosample_stats_assaytype_datatype': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('data_type').fetch()),
            'biosample_stats_assaytype_species': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('species').fetch()),
            'biosample_stats_assaytype_project': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('project_root').fetch()),
            'biosample_stats_datatype_species': list(StatsQuery(self).entity('biosample').dimension('data_type').dimension('species').fetch()),
            'biosample_stats_datatype_project': list(StatsQuery(self).entity('biosample').dimension('data_type').dimension('project_root').fetch()),

            'subject_stats_anatomy_assaytype': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('assay_type').fetch()),
            'subject_stats_anatomy_datatype': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('data_type').fetch()),
            'subject_stats_anatomy_species': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('species').fetch()),
            'subject_stats_anatomy_project': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('project_root').fetch()),
            'subject_stats_assaytype_datatype': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('data_type').fetch()),
            'subject_stats_assaytype_species': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('species').fetch()),
            'subject_stats_assaytype_project': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('project_root').fetch()),
            'subject_stats_datatype_species': list(StatsQuery(self).entity('subject').dimension('data_type').dimension('species').fetch()),
            'subject_stats_datatype_project': list(StatsQuery(self).entity('subject').dimension('data_type').dimension('project_root').fetch()),

        }
        print(json.dumps(results, indent=2))

    def list_projects(self, use_root_projects=False, parent_project_RID=None):
        """Return list of projects AKA funded activities

        :param use_root_projects: Only consider root projects (default False)
        :param parent_project_RID: Only consider children of specified project (default None)
        """
        children = self.builder.CFDE.project.alias("children")
        pip1 = self.builder.CFDE.project_in_project.alias('pip1')
        project = self.builder.CFDE.project
        path = children.path
        path = path.link(
            pip1,
            on=( (path.children.id_namespace == pip1.child_project_id_namespace)
                 & (path.children.id == pip1.child_project_id) )
        ).link(
            project,
            on=( (pip1.parent_project_id_namespace == project.id_namespace)
                 & (pip1.parent_project_id == project.id) ),
            join_type='right'
        )

        if use_root_projects:
            path = path.link(self.builder.CFDE.project_root)
        elif parent_project_RID is not None:
            pip2 = self.builder.CFDE.project_in_project.alias('pip2')
            parent = self.builder.CFDE.project.alias("parent")
            path = path.link(
                pip2,
                on=( (path.project.id_namespace == pip2.child_project_id_namespace)
                     & (path.project.id == pip2.child_project_id) )
            ).link(
                parent,
                on=( (path.pip2.parent_project_id_namespace == parent.id_namespace)
                     & (path.pip2.parent_project_id == parent.id) )
            ).filter(path.parent.RID == parent_project_RID)

        return path.groupby(
            path.project.RID,
        ).attributes(
            path.project.id_namespace,
            path.project.id,
            path.project.column_definitions['name'],
            path.project.abbreviation,
            path.project.description,
            CntD(path.children.RID).alias('num_subprojects'),
        )

    def list_datatypes(self):
        """Return list of data_type terms
        """
        return self.builder.CFDE.data_type.path.entities().fetch()
    
    def list_formats(self):
        """Return list of file format terms
        """
        return self.builder.CFDE.file_format.path.entities().fetch()

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
