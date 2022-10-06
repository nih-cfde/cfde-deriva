#!/usr/bin/python3

# This script assumes latest deriva-py/master library
# talking to latest ermrest/master server

import sys
import os
import json
from deriva.core import ErmrestCatalog, urlquote, DEFAULT_HEADERS, DEFAULT_SESSION_CONFIG, get_credential
from deriva.core.datapath import Min, Max, Cnt, CntD, Avg, Sum, Bin

######
# utility functions to idempotently build up the path in a StatsQuery object
#
# uses level1_stats data cube, which is based on this core-entity query path:
#
#   file -- file-describes-biosample -- biosample -- biosample-from-subject -- subject
#
# summarized into groups based on metadata combinations
#
#   file -- assay_type, data_type, file_format
#   biosample -- anatomy
#   subject -- subject_role_taxonomy -- ncbi_taxonomy, subject_role, subject_granularity
#   [stats entity] -- project
#   [stats entity] -- root project
#
# with each group containing num_files, num_bytes, num_biosamples, num_subjects.
#
# to retrieve stats, we choose our own dimensional grouping (as a subset of the
# available grouping dimensions) and sum those existing metrics to collapse
# results into our more coarse-grained groups.
#
# the subproject dimension joins extra structure to the project concept of the data cube.
#
# also, we join the vocab tables to get human-readable names for the dimensional concepts
#

def _add_anatomy_leaf(queryobj, show_nulls=False, **kwargs):
    if 'anatomy' in queryobj.path.table_instances:
        return
    anatomy_slim = queryobj.helper.builder.CFDE.anatomy_slim
    anatomy = queryobj.helper.builder.CFDE.anatomy
    queryobj.path = queryobj.path.link(
        anatomy_slim,
        on=( queryobj.path.level1_stats.anatomy_nid == anatomy_slim.original_term ),
        join_type= 'left' if show_nulls else ''
    )
    queryobj.path = queryobj.path.link(
        anatomy,
        on=( queryobj.path.anatomy_slim.slim_term == anatomy.nid ),
        join_type= 'left' if show_nulls else ''
    )

def _add_assaytype_leaf(queryobj, show_nulls=False, **kwargs):
    if 'assay_type' in queryobj.path.table_instances:
        return
    assay_type_slim = queryobj.helper.builder.CFDE.assay_type_slim
    assay_type = queryobj.helper.builder.CFDE.assay_type
    queryobj.path = queryobj.path.link(
        assay_type_slim,
        on=( queryobj.path.level1_stats.assay_type_nid == assay_type_slim.original_term ),
        join_type= 'left' if show_nulls else ''
    )
    queryobj.path = queryobj.path.link(
        assay_type,
        on=( queryobj.path.assay_type_slim.slim_term == assay_type.nid ),
        join_type= 'left' if show_nulls else ''
    )

def _add_datatype_leaf(queryobj, show_nulls=False, **kwargs):
    if 'data_type' in queryobj.path.table_instances:
        return
    data_type_slim = queryobj.helper.builder.CFDE.data_type_slim
    data_type = queryobj.helper.builder.CFDE.data_type
    queryobj.path = queryobj.path.link(
        data_type_slim,
        on=( queryobj.path.level1_stats.data_type_nid == data_type_slim.original_term ),
        join_type= 'left' if show_nulls else ''
    )
    queryobj.path = queryobj.path.link(
        data_type,
        on=( queryobj.path.data_type_slim.slim_term == data_type.nid ),
        join_type= 'left' if show_nulls else ''
    )

def _add_fileformat_leaf(queryobj, show_nulls=False, **kwargs):
    if 'file_format' in queryobj.path.table_instances:
        return
    file_format_slim = queryobj.helper.builder.CFDE.file_format_slim
    file_format = queryobj.helper.builder.CFDE.file_format
    queryobj.path = queryobj.path.link(
        file_format_slim,
        on=( queryobj.path.level1_stats.file_format_nid == file_format_slim.original_term ),
        join_type= 'left' if show_nulls else ''
    )
    queryobj.path = queryobj.path.link(
        file_format,
        on=( queryobj.path.file_format_slim.slim_term == file_format.nid ),
        join_type= 'left' if show_nulls else ''
    )

def _add_disease_leaf(queryobj, show_nulls=False, **kwargs):
    if 'disease' in queryobj.path.table_instances:
        return
    disease_slim = queryobj.helper.builder.CFDE.disease_slim
    disease = queryobj.helper.builder.CFDE.disease
    queryobj.path = queryobj.path.link(
        disease_slim,
        on=( queryobj.path.level1_stats.disease_nid == disease_slim.original_term ),
        join_type= 'left' if show_nulls else ''
    )
    queryobj.path = queryobj.path.link(
        disease,
        on=( queryobj.path.disease_slim.slim_term == disease.nid ),
        join_type= 'left' if show_nulls else ''
    )

def _add_species_leaf(queryobj, show_nulls=False, **kwargs):
    if 'species' in queryobj.path.table_instances:
        return
    species = queryobj.helper.builder.CFDE.ncbi_taxonomy.alias('species')
    queryobj.path = queryobj.path.link(
        species,
        on=( queryobj.path.level1_stats.species_id == species.id ),
        join_type= 'left' if show_nulls else ''
    )

def _add_sex_leaf(queryobj, show_nulls=False, **kwargs):
    if 'sex' in queryobj.path.table_instances:
        return
    sex = queryobj.helper.builder.CFDE.sex.alias('sex')
    queryobj.path = queryobj.path.link(
        sex,
        on=( queryobj.path.level1_stats.sex_id == sex.id ),
        join_type= 'left' if show_nulls else ''
    )

def _add_race_leaf(queryobj, show_nulls=False, **kwargs):
    if 'race' in queryobj.path.table_instances:
        return
    race = queryobj.helper.builder.CFDE.race.alias('race')
    queryobj.path = queryobj.path.link(
        race,
        on=( queryobj.path.level1_stats.race_id == race.id ),
        join_type= 'left' if show_nulls else ''
    )

def _add_ethnicity_leaf(queryobj, show_nulls=False, **kwargs):
    if 'ethnicity' in queryobj.path.table_instances:
        return
    ethnicity = queryobj.helper.builder.CFDE.ethnicity.alias('ethnicity')
    queryobj.path = queryobj.path.link(
        ethnicity,
        on=( queryobj.path.level1_stats.ethnicity_id == ethnicity.id ),
        join_type= 'left' if show_nulls else ''
    )

def _add_rootproject_leaf(queryobj, show_nulls=False, **kwargs):
    """Idempotently add root project concept to path"""
    # ignore show_nulls since project is always attributed
    if 'subproject' in queryobj.path.table_instances:
        raise TypeError('Cannot combine subproject and project_root dimensions')

    if 'root_project' in queryobj.path.table_instances:
        return

    root_project = queryobj.helper.builder.CFDE.project.alias('root_project')
    root_project_idn = queryobj.helper.builder.CFDE.id_namespace.alias('root_project_idn')
    queryobj.path = queryobj.path.link(
        root_project,
        on=(queryobj.path.level1_stats.root_project_nid == root_project.nid),
    ).link(
        root_project_idn,
        on=(queryobj.path.root_project.id_namespace == root_project_idn.nid),
    )

def _add_subproject_leaf(queryobj, show_nulls=False, **kwargs):
    """Idempotently add root project concept to path"""
    # ignore show_nulls since project is always attributed
    try:
        parent_project_nid = kwargs['parent_project_nid']
    except KeyError:
        raise TypeError('Missing required parent_project_nid keyword argument in StatsQuery.dimension("subproject", **kwargs) call')

    if 'root_project' in queryobj.path.table_instances:
        raise TypeError('Cannot combine subproject and project_root dimensions')

    level1_stats = queryobj.path.level1_stats
    pipt = queryobj.helper.builder.CFDE.project_in_project_transitive.alias('pipt')
    pip = queryobj.helper.builder.CFDE.project_in_project.alias('pip')
    subproject = queryobj.helper.builder.CFDE.project.alias('subproject')
    subproject_idn = queryobj.helper.builder.CFDE.id_namespace.alias('subproject_idn')
    parentproj = queryobj.helper.builder.CFDE.project.alias('parentproj')

    queryobj.path = queryobj.path.link(
        pipt,
        on=(level1_stats.project_nid == pipt.member_project),
    ).link(
        pip,
        on=(queryobj.path.pipt.leader_project == pip.child_project),
    ).link(
        parentproj,
        on=(queryobj.path.pip.parent_project == parentproj.nid),
    ).filter(
        queryobj.path.parentproj.nid == parent_project_nid
    ).link(
        subproject,
        on=(queryobj.path.pipt.leader_project == subproject.nid),
    ).link(
        subproject_idn,
        on=(queryobj.path.subproject.id_namespace == subproject_idn.nid),
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

    """

    # define supported keys, mapped to implementation bits...
    supported_entities = {
        'file': [
            lambda path: Sum(path.level1_stats.num_files).alias('num_files'),
            lambda path: Sum(path.level1_stats.num_bytes).alias('num_bytes'),
        ],
        'biosample': [
            lambda path: Sum(path.level1_stats.num_biosamples).alias('num_biosamples'),
        ],
        'subject': [
            lambda path: Sum(path.level1_stats.num_subjects).alias('num_subjects'),
        ],
    }
    supported_dimensions = {
        'anatomy': (
            _add_anatomy_leaf, [
                lambda path: path.anatomy.column_definitions['id'].alias('anatomy_id'),
            ], [
                lambda path: path.anatomy.column_definitions['name'].alias('anatomy_name'),
            ]
        ),
        'assay_type': (
            _add_assaytype_leaf, [
                lambda path: path.level1_stats.assay_type_id,
            ], [
                lambda path: path.assay_type.column_definitions['name'].alias('assay_type_name'),
            ]
        ),
        'data_type': (
            _add_datatype_leaf, [
                lambda path: path.level1_stats.data_type_id,
            ], [
                lambda path: path.data_type.column_definitions['name'].alias('data_type_name'),
            ]
        ),
        'file_format': (
            _add_fileformat_leaf, [
                lambda path: path.level1_stats.file_format_id,
            ], [
                lambda path: path.data_type.column_definitions['name'].alias('file_format_name'),
            ]
        ),
        'disease': (
            _add_disease_leaf, [
                lambda path: path.level1_stats.disease_id,
            ], [
                lambda path: path.disease.column_definitions['name'].alias('disease_name'),
            ]
        ),
        'species': (
            _add_species_leaf, [
                lambda path: path.species.id.alias('species_id'),
            ], [
                lambda path: path.species.column_definitions['name'].alias('species_name'),
            ]
        ),
        'sex': (
            _add_sex_leaf, [
                lambda path: path.sex.id.alias('sex_id'),
            ], [
                lambda path: path.sex.column_definitions['name'].alias('sex_name'),
            ]
        ),
        'race': (
            _add_race_leaf, [
                lambda path: path.race.id.alias('race_id'),
            ], [
                lambda path: path.race.column_definitions['name'].alias('race_name'),
            ]
        ),
        'ethnicity': (
            _add_ethnicity_leaf, [
                lambda path: path.ethnicity.id.alias('ethnicity_id'),
            ], [
                lambda path: path.ethnicity.column_definitions['name'].alias('ethnicity_name'),
            ]
        ),
        'project_root': (
            _add_rootproject_leaf, [
                lambda path: path.root_project.nid.alias('project_nid'),
            ], [
                lambda path: path.root_project_idn.id.alias('project_id_namespace'),
                lambda path: path.root_project.local_id.alias('project_local_id'),
                lambda path: path.root_project.column_definitions['name'].alias('project_name'),
            ]
        ),
        'subproject': (
            _add_subproject_leaf, [
                lambda path: path.subproject.nid.alias('project_nid'),
            ], [
                lambda path: path.subproject_idn.id.alias('project_id_namespace'),
                lambda path: path.subproject.local_id.alias('project_local_id'),
                lambda path: path.subproject.column_definitions['name'].alias('project_name'),
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

        self.path = self.helper.builder.CFDE.level1_stats.path

        return self

    def dimension(self, dimension_name, show_nulls=True, **kwargs):
        """Configure a grouping dimension

        :param dimension_name: One of the StatsQuery.supported_dimension key strings
        :param show_nulls: Allow null in dimensional outputs when True (default True)
        :param kwargs: Keyword arguments specific to a dimension (see further documentation)

        Dimension-specific keyword arguments:

        :param parent_project_nid: Use sub-projects of specified parent project nid for "subproject" dimension (required)

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

        add_path_func(self, show_nulls=show_nulls, **kwargs)

        return self

    def fetch(self, headers=DEFAULT_HEADERS):
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
            ]).fetch(headers=headers)
        else:
            return self.path.aggregates(*[
                attr_func(self.path)
                for attr_func in self.attr_funcs
            ]).fetch(headers=headers)

class Entity (object):
    def __init__(self, ent_name, *attr_cnames):
        self.name = ent_name
        self.attr_cnames = attr_cnames

class TermMap (object):
    vocab_cnames = ['nid', 'id', 'name', 'description']

    def __init__(self, helper, vocab_tname, **kwargs):
        headers = kwargs.get('headers', DEFAULT_HEADERS)
        path = helper.builder.CFDE.tables[vocab_tname].path
        table = path.table_instances[vocab_tname]
        self.nid_map =  {
            row['nid']: row
            for row in path.attributes(*[
                    table.column_definitions[cname]
                    for cname in self.vocab_cnames
            ]).fetch(headers=headers)
        }

    def term_array(self, nid_array):
        return [ self.nid_map[nid] for nid in nid_array ]

class DccMap (TermMap):
    vocab_cnames = ['nid', 'id', 'dcc_name', 'dcc_abbreviation', 'dcc_description']

class SlimTermMap (TermMap):
    def __init__(self, helper, vocab_tname, **kwargs):
        super(SlimTermMap, self).__init__(helper, vocab_tname, **kwargs)
        headers = kwargs.get('headers', DEFAULT_HEADERS)
        slimmap_tname = kwargs['slimmap_tname']
        path = helper.builder.CFDE.tables[slimmap_tname].path
        table = path.table_instances[slimmap_tname]
        self.slim_map = {}
        for row in path.attributes(table.original_term, table.slim_term).fetch(headers=headers):
            self.slim_map.setdefault(row['original_term'], set()).add(row['slim_term'])

    def slim_nid_array(self, original_nid_array):
        terms = set()
        for nid in original_nid_array:
            terms.update(self.slim_map[nid])
        return sorted(terms)

class AssocTermMap (TermMap):
    def __init__(self, helper, vocab_tname, **kwargs):
        super(AssocTermMap, self).__init__(helper, vocab_tname, **kwargs)
        headers = kwargs.get('headers', DEFAULT_HEADERS)
        atype_tname = kwargs['atype_tname']
        self.atype_map = TermMap(helper, atype_tname, headers=headers)

    def assoc_nid_array(self, original_nidpair_array):
        terms = set()
        for entry in original_nidpair_array:
            # guard for compat with older simple dimension arrays in mixed deployment
            if isinstance(entry, list):
                term_nid, atype_nid = entry
                # TODO: drop terms for certain atype_nids?
                terms.add(term_nid)
        return sorted(terms)

class SlimAssocTermMap (SlimTermMap, AssocTermMap):
    def __init__(self, helper, vocab_tname, **kwargs):
        super(SlimAssocTermMap, self).__init__(helper, vocab_tname, **kwargs)

class Dimension (object):
    slim = False
    assoc = False

    def __init__(self, dim_name, array_cname, **kwargs):
        self.name = dim_name
        self.array_cname = array_cname
        self.vocab_tname = kwargs.get('vocab_tname', dim_name)
        self.fact_tname = kwargs.get('fact_tname', 'core_fact')

    def get_vocab_map(self, helper, headers=DEFAULT_HEADERS):
        return TermMap(helper, self.vocab_tname, headers=headers)

class DccDimension (Dimension):
    def __init__(self):
        super(DccDimension, self).__init__('dcc', 'dccs')

    def get_vocab_map(self, helper, headers=DEFAULT_HEADERS):
        return DccMap(helper, self.vocab_tname, headers=headers)

class SlimDimension (Dimension):
    slim = True

    def __init__(self, dim_name, array_cname, **kwargs):
        super(SlimDimension, self).__init__(dim_name, array_cname, **kwargs)
        self.slimmap_tname = kwargs.get('slimmap_tname', ('%s_slim' % self.vocab_tname))

    def get_vocab_map(self, helper, headers=DEFAULT_HEADERS):
        return SlimTermMap(helper, self.vocab_tname, slimmap_tname=self.slimmap_tname, headers=headers)

class AssocTypeDimension (Dimension):
    assoc = True

    def __init__(self, dim_name, array_cname, **kwargs):
        super(AssocTypeDimension, self).__init__(dim_name, array_cname, **kwargs)
        self.atype_tname = kwargs.get('atype_tname', ('%s_association_type' % self.vocab_tname))

    def get_vocab_map(self, helper, headers=DEFAULT_HEADERS):
        return AssocTermMap(helper, self.vocab_tname, atype_tname=self.atype_tname, headers=headers)

class SlimAssocTypeDimension (SlimDimension, AssocTypeDimension):
    def __init__(self, dim_name, array_cname, **kwargs):
        super(SlimAssocTypeDimension, self).__init__(dim_name, array_cname, **kwargs)

    def get_vocab_map(self, helper, headers=DEFAULT_HEADERS):
        return SlimAssocTermMap(helper, self.vocab_tname, slimmap_tname=self.slimmap_tname, atype_tname=self.atype_tname, headers=headers)

class StatsQuery2 (object):
    """C2M2 statistics query generator

    Construct with a DashboardQueryHelper instance to bind to a
    catalog, select base entity for statistics, and select optional
    dimensions for multi-dimensional grouping of results.

    StatsQuery2(helper)
    .entity('file')
    .dimension('data_type')
    .dimension('dcc')
    .fetch()

    Exactly one entity MUST be configured. Zero or more dimensions MAY
    be configured.

    While StatsQuery result groups are identified by a single concept
    ID in each dimension, StatsuQuery2 groups are identified by a set
    of terms for each dimension.

    """
    # define supported keys, mapped to implementation bits...
    supported_entities = {
        ent.name: ent
        for ent in [
                Entity('file', 'num_files', 'total_size_in_bytes'),
                Entity('biosample', 'num_biosamples'),
                Entity('subject', 'num_subjects'),
                Entity('collection', 'num_collections'),
        ]
    }
    supported_dimensions = {
        dim.name: dim
        for dim in [
                DccDimension(),

                Dimension('analysis_type', 'analysis_types'),
                SlimDimension('anatomy', 'anatomies'),
                SlimDimension('assay_type', 'assay_types'),
                Dimension('compression_format', 'compression_formats', vocab_tname='file_format'),
                SlimDimension('data_type', 'data_types'),
                SlimAssocTypeDimension('disease', 'diseases'),
                Dimension('ethnicity', 'ethnicities'),
                SlimDimension('file_format', 'file_formats'),
                Dimension('gene', 'genes', fact_tname='gene_fact'),
                Dimension('mime_type', 'mime_types'),
                SlimDimension('ncbi_taxonomy', 'ncbi_taxons'),
                AssocTypeDimension('phenotype', 'phenotypes'),
                Dimension('protein', 'proteins', fact_tname='protein_fact'),
                Dimension('race', 'races'),
                Dimension('sample_prep_method', 'sample_prep_methods'),
                Dimension('sex', 'sexes'),
                Dimension('species', 'subject_species', vocab_tname='ncbi_taxonomy'),
                Dimension('substance', 'substances', fact_tname='pubchem_fact'),
                Dimension('subject_granularity', 'subject_granularities'),
                Dimension('subject_role', 'subject_roles'),
        ]
    }

    def __init__(self, helper):
        """Construct a StatsQuery builder object

        :param helper: Instance of DashboardQueryHelper
        """
        self.helper = helper
        self.included_entities = set()
        self.included_dimensions = set()
        self.path = self.helper.builder.CFDE.combined_fact.path

    def entity(self, entity_name):
        """Select entity which will be source of statistics

        :param entity_name: One of the StatsQuery2.supported_entities key strings
        """
        if self.included_entities:
            # could relax this later...?
            raise TypeError('Cannot call .entity() method more than once.')

        try:
            ent = self.supported_entities[entity_name]
        except KeyError:
            raise ValueError('Unsupported entity_name "%s"' % (entity_name,))

        self.included_entities.add(ent)

        return self

    def dimension(self, dimension_name):
        """Configure a grouping dimension

        :param dimension_name: One of the StatsQuery2.supported_dimension key strings

        """
        if self.path is None:
            raise TypeError('Cannot call .dimension() method prior to calling .entity() method.')

        try:
            dim = self.supported_dimensions[dimension_name]
        except KeyError:
            raise ValueError('Unsupported dimension_name "%s"' % (dimension_name,))

        if dim in self.included_dimensions:
            raise TypeError('Cannot use dimension_name "%s" more than once.' % (dim.name,))

        self.included_dimensions.add(dim)

        if dim.fact_tname not in self.path.table_instances:
            self.path = self.path.combined_fact.link(self.helper.builder.CFDE.tables[dim.fact_tname])

        return self

    def _sort_and_merge(self, rows, sort_key, sums_dict):
        if rows:
            rows.sort(key=sort_key)
            prev_row = rows[0]
            prev_key = sort_key(prev_row)
            sums = sums_dict(prev_row)
            for row in rows[1:]:
                key = sort_key(row)
                if prev_key == key:
                    # accumulate another row w/ identical keying
                    for k, v in sums_dict(row).items():
                        sums[k] = sums[k] + v
                else:
                    # emit previous accumulation and start a new one for new keying
                    prev_row.update(sums)
                    yield prev_row
                    prev_row = row
                    prev_key = key
                    sums = sums_dict(prev_row)
            prev_row.update(sums)
            yield prev_row

    def fetch(self, headers=DEFAULT_HEADERS):
        """Fetch results for configured query"""
        if self.path is None:
            raise TypeError('Cannot call .fetch() method prior to calling .entity() method.')

        entities = list(self.included_entities)
        dimensions = list(self.included_dimensions)

        filters = self.path.combined_fact.column_definitions[entities[0].attr_cnames[0]] > 0
        for ent in entities[1:]:
            filters = filters | (self.path.combined_fact.column_definitions[ent.attr_cnames[0]] > 0)

        if dimensions:
            attributes = []
            for ent in self.included_entities:
                attributes.extend([ Sum(self.path.combined_fact.column_definitions[cname]).alias(cname) for cname in ent.attr_cnames ])

            api = self.path.filter(filters).groupby(*[
                self.path.table_instances[dim.fact_tname].column_definitions[dim.array_cname]
                for dim in dimensions
            ]).attributes(*attributes)
        else:
            aggregates = []
            for ent in self.included_entities:
                aggregates.extend([ Sum(self.path.combined_fact.column_definitions[cname]).alias(cname) for cname in ent.attr_cnames ])

            api = self.path.filter(filters).aggregates(*aggregates)

        rows = api.fetch(headers=headers)

        def with_update(d1, d2):
            d1.update(d2)
            return d1

        vocab_term_maps = {
            dim.name: dim.get_vocab_map(self.helper, headers=headers)
            for dim in dimensions
        }

        slim_dimensions = [ dim for dim in dimensions if dim.slim ]
        assoc_dimensions = [ dim for dim in dimensions if dim.assoc ]

        def slim_row(row):
            for dim in slim_dimensions:
                term_map = vocab_term_maps[dim.name]
                row[dim.array_cname] = term_map.slim_nid_array(row[dim.array_cname])
            return row

        def assoc_row(row):
            for dim in assoc_dimensions:
                atype_map = vocab_term_maps[dim.name]
                row[dim.array_cname] = atype_map.assoc_nid_array(row[dim.array_cname])
            return row

        def rewrite_row(row):
            for dim in dimensions:
                term_map = vocab_term_maps[dim.name]
                row[dim.array_cname] = term_map.term_array(row[dim.array_cname])
            return row

        if slim_dimensions or assoc_row:
            # have to re-aggregate after term slimming or assoc type masking
            rows = [ slim_row(assoc_row(row)) for row in rows ]

            def sort_key(row):
                return tuple([ row[dim.array_cname] for dim in dimensions ])

            def sums_dict(row):
                return {
                    cname: 0 if row[cname] is None else row[cname]
                    for ent in self.included_entities
                    for cname in ent.attr_cnames
                }

            rows = self._sort_and_merge(rows, sort_key, sums_dict)

        return [ rewrite_row(row) for row in rows ]

class DashboardQueryHelper (object):

    def __init__(self, hostname, catalogid, scheme='https', caching=True, credential=None):
        session_config = DEFAULT_SESSION_CONFIG.copy()
        session_config["allow_retry_on_all_methods"] = True
        self.catalog = ErmrestCatalog(scheme, hostname, catalogid, caching=caching, session_config=session_config, credentials=credential)
        self.builder = self.catalog.getPathBuilder()
        self.cfde_schema = self.catalog.getCatalogModel().schemas['CFDE']
        if 'combined_fact' not in self.cfde_schema.tables:
            raise ValueError('Target %s catalog %r lacks the required CFDE.combined_fact table' % (hostname, catalogid))

    def run_demo1(self):
        """Run each example query and dump all results as JSON."""
        projects = {
            (row['id_namespace'], row['local_id']): row
            for row in self.list_projects(use_root_projects=True)
        }

        for proj in [
                ('tag:hmpdacc.org,2021-08-04:', 'HMP'),
                ('tag:hmpdacc.org,2021-06-04:', 'HMP'),
                ('https://www.lincsproject.org/', 'LINCS'),
                ('https://www.metabolomicsworkbench.org/', 'PPR00001'),
                ('tag:hubmapconsortium.org,2021:', 'HuBMAP'),
        ]:
            if proj in projects:
                nid_for_parent_proj = projects[proj]['nid']
                break

        # use list() to convert each ResultSet
        # for easier JSON serialization...
        results = {
            #'list_projects': list(self.list_projects()),
            #'list_root_projects': list(self.list_projects(use_root_projects=True)),
            #'list_datatypes': list(self.list_datatypes()),
            #'list_formats': list(self.list_formats()),

            'root_projects': list(self.list_projects(use_root_projects=True)),
            'subject_stats_assaytype_subproject': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('subproject', parent_project_nid=nid_for_parent_proj).fetch()),

            'file_stats_anatomy_assaytype': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('assay_type').fetch()),
            'file_stats_anatomy_datatype': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('data_type').fetch()),
            'file_stats_anatomy_species': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('species').fetch()),
            'file_stats_anatomy_project': list(StatsQuery(self).entity('file').dimension('anatomy').dimension('project_root').fetch()),
            'file_stats_assaytype_datatype': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('data_type').fetch()),
            'file_stats_assaytype_species': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('species').fetch()),
            'file_stats_assaytype_project': list(StatsQuery(self).entity('file').dimension('assay_type').dimension('project_root').fetch()),
            'file_stats_datatype_species': list(StatsQuery(self).entity('file').dimension('data_type').dimension('species').fetch()),
            'file_stats_datatype_project': list(StatsQuery(self).entity('file').dimension('data_type').dimension('project_root').fetch()),
            'file_stats_datatype_disease': list(StatsQuery(self).entity('file').dimension('data_type').dimension('disease').fetch()),
            'file_stats_datatype_clinical': list(StatsQuery(self).entity('file').dimension('sex').dimension('race').dimension('ethnicity').fetch()),

            'biosample_stats_anatomy_assaytype': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('assay_type').fetch()),
            'biosample_stats_anatomy_datatype': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('data_type').fetch()),
            'biosample_stats_anatomy_species': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('species').fetch()),
            'biosample_stats_anatomy_project': list(StatsQuery(self).entity('biosample').dimension('anatomy').dimension('project_root').fetch()),
            'biosample_stats_assaytype_datatype': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('data_type').fetch()),
            'biosample_stats_assaytype_species': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('species').fetch()),
            'biosample_stats_assaytype_project': list(StatsQuery(self).entity('biosample').dimension('assay_type').dimension('project_root').fetch()),
            'biosample_stats_datatype_species': list(StatsQuery(self).entity('biosample').dimension('data_type').dimension('species').fetch()),
            'biosample_stats_datatype_project': list(StatsQuery(self).entity('biosample').dimension('data_type').dimension('project_root').fetch()),
            'biosample_stats_datatype_disease': list(StatsQuery(self).entity('biosample').dimension('data_type').dimension('disease').fetch()),
            'biosample_stats_datatype_clinical': list(StatsQuery(self).entity('biosample').dimension('sex').dimension('race').dimension('ethnicity').fetch()),

            'subject_stats_anatomy_assaytype': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('assay_type').fetch()),
            'subject_stats_anatomy_datatype': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('data_type').fetch()),
            'subject_stats_anatomy_species': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('species').fetch()),
            'subject_stats_anatomy_project': list(StatsQuery(self).entity('subject').dimension('anatomy').dimension('project_root').fetch()),
            'subject_stats_assaytype_datatype': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('data_type').fetch()),
            'subject_stats_assaytype_species': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('species').fetch()),
            'subject_stats_assaytype_project': list(StatsQuery(self).entity('subject').dimension('assay_type').dimension('project_root').fetch()),
            'subject_stats_datatype_species': list(StatsQuery(self).entity('subject').dimension('data_type').dimension('species').fetch()),
            'subject_stats_datatype_project': list(StatsQuery(self).entity('subject').dimension('data_type').dimension('project_root').fetch()),
            'subject_stats_datatype_disease': list(StatsQuery(self).entity('subject').dimension('data_type').dimension('disease').fetch()),
            'subject_stats_datatype_clinical': list(StatsQuery(self).entity('subject').dimension('sex').dimension('race').dimension('ethnicity').fetch()),

        }
        print(json.dumps(results, indent=2))

    def run_demo2(self):
        """Run each example query and dump all results as JSON."""
        # use list() to convert each ResultSet
        # for easier JSON serialization...
        x = StatsQuery2(self)

        results = {
            'file': list(StatsQuery2(self).entity('file').fetch()),

            'file_stats_anatomy_assaytype': list(StatsQuery2(self).entity('file').dimension('anatomy').dimension('assay_type').fetch()),
            'file_stats_disease_gene': list(StatsQuery2(self).entity('file').dimension('disease').dimension('gene').fetch()),
            'file_stats_datatype_dcc': list(StatsQuery2(self).entity('file').dimension('data_type').dimension('dcc').fetch()),
            'file_stats_datatype_species': list(StatsQuery2(self).entity('file').dimension('data_type').dimension('species').fetch()),

            'biosample_stats_datatype_disease': list(StatsQuery2(self).entity('biosample').dimension('data_type').dimension('disease').fetch()),

            'subject_stats_datatype_substance': list(StatsQuery2(self).entity('subject').dimension('data_type').dimension('substance').fetch()),

            'file_gene': list(StatsQuery2(self).entity('file').dimension('gene').fetch()),
            'file_substance': list(StatsQuery2(self).entity('file').dimension('substance').fetch()),
            'file_core': list(StatsQuery2(self).entity('file')
                             .dimension('anatomy')
                             .dimension('assay_type')
                             .dimension('analysis_type').dimension('compression_format').dimension('data_type').dimension('file_format').dimension('mime_type')
                             .dimension('subject_granularity').dimension('subject_role').dimension('species').dimension('ncbi_taxonomy')
                             .dimension('sex').dimension('race').dimension('ethnicity')
                             .dimension('disease').dimension('phenotype')
                             .fetch()
                             )
        }
        print(json.dumps(results, indent=2))

    def list_projects(self, use_root_projects=False, parent_project_nid=None, headers=DEFAULT_HEADERS):
        """Return list of projects AKA funded activities

        :param use_root_projects: Only consider root projects (default False)
        :param parent_project_nid: Only consider children of specified project (default None)

        """
        children = self.builder.CFDE.project.alias("children")
        pip1 = self.builder.CFDE.project_in_project.alias('pip1')
        project = self.builder.CFDE.project
        idn = self.builder.CFDE.id_namespace
        path = children.path
        path = path.link(
            pip1,
            on=( path.children.nid == pip1.child_project ),
        ).link(
            project,
            on=( pip1.parent_project == project.nid ),
            join_type='right'
        ).link(
            idn,
            on=(project.id_namespace == idn.nid),
        )

        if use_root_projects:
            root = self.builder.CFDE.project_root
            path = path.link(
                root,
                on=(path.project.nid == root.project),
            )
        elif parent_project_nid is not None:
            pip2 = self.builder.CFDE.project_in_project.alias('pip2')
            parent = self.builder.CFDE.project.alias("parent")
            path = path.link(
                pip2,
                on=( path.project.nid == pip2.child_project ),
            ).link(
                parent,
                on=( path.pip2.parent_project == parent.nid ),
            ).filter(path.parent.nid == parent_project_nid)

        return path.groupby(
            path.project.nid,
        ).attributes(
            path.id_namespace.id.alias('id_namespace'),
            path.project.local_id,
            path.project.column_definitions['name'],
            path.project.abbreviation,
            path.project.description,
            CntD(path.children.nid).alias('num_subprojects')
        ).fetch(headers=headers)

    def list_datatypes(self, headers=DEFAULT_HEADERS):
        """Return list of data_type terms
        """
        return self.builder.CFDE.data_type.path.entities().fetch(headers=headers)
    
    def list_formats(self, headers=DEFAULT_HEADERS):
        """Return list of file format terms
        """
        return self.builder.CFDE.file_format.path.entities().fetch(headers=headers)

## ugly CLI wrapping...
def main():
    """Runs demo of catalog dashboard queries."""
    hostname = os.getenv('DERIVA_SERVERNAME', 'app-dev.nih-cfde.org')
    credential = get_credential(hostname)
    catalogid = os.getenv('DERIVA_CATALOGID', '1')
    db = DashboardQueryHelper(hostname, catalogid, credential=credential)
    db.run_demo1()
    db.run_demo2()
    return 0

if __name__ == '__main__':
    exit(main())
