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
            #'list_root_projects': list(self.list_projects(use_root_projects=True)),
            #'list_datatypes': list(self.list_datatypes()),
            #'list_formats': list(self.list_formats()),
            'list_project_anatomy_file_stats': list(self.query_combination(True, "file", "anatomy")),
            'list_project_anatomy_biosample_stats': list(self.query_combination(True, "biosample", "anatomy")),
            'list_project_anatomy_subject_stats': list(self.query_combination(True, "subject", "anatomy")),
            'list_project_assaytype_file_stats': list(self.query_combination(True, "file", "assay_type")),
            'list_project_assaytype_biosample_stats': list(self.query_combination(True, "biosample", "assay_type")),
            'list_project_assaytype_subject_stats': list(self.query_combination(True, "subject", "assay_type")),
            'list_project_datatype_file_stats': list(self.query_combination(True, "file", "data_type")),
            'list_project_datatype_biosample_stats': list(self.query_combination(True, "biosample", "data_type")),
            'list_project_datatype_subject_stats': list(self.query_combination(True, "subject", "data_type")),
            'list_project_species_file_stats': list(self.query_combination(True, "file", "species")),
            'list_project_species_biosample_stats': list(self.query_combination(True, "biosample", "species")),
            'list_project_species_subject_stats': list(self.query_combination(True, "subject", "species")),
        }
        print(json.dumps(results, indent=2))

    def list_projects(self, use_root_projects=False, path_func=(lambda builder, path: path), proj_func=(lambda path: path.entities())):
        """Return list of projects AKA funded activities

        :param use_root_projects: Only consider root projects (default False)
        :param path_func: Function to allow path chaining (default no-change)
        :param proj_func: Function to project set from path (default get .entities())
        """
        if use_root_projects:
            path = (
                self.builder.CFDE.project_root
                .link(self.builder.CFDE.project)
            )
        else:
            path = self.builder.CFDE.project
        return proj_func(path_func(self.builder, path)).fetch()

    def list_datatypes(self):
        """Return list of data_type terms
        """
        return self.builder.CFDE.data_type.path.entities().fetch()
    
    def list_formats(self):
        """Return list of file format terms
        """
        return self.builder.CFDE.file_format.path.entities().fetch()

    @classmethod
    def extend_project_path_to_file(cls, builder, path, use_root_projects, path_func=(lambda builder, path: path)):
        """Function to link file to existing project path by attribution.

        :param builder: A builder appropriate to use when extending path
        :param path: The path we will build from
        :param use_root_projects: Only consider root projects (default False)
        :param path_func: Function to allow path chaining (default no-change)
        """
        file = builder.CFDE.file
        if use_root_projects:
            # link to transitively-attributed root project
            pipt = builder.CFDE.project_in_project_transitive.alias('pipt')
            path = path.link(
                pipt,
                on=( (path.project.id_namespace == pipt.leader_project_id_namespace)
                     & (path.project.id == pipt.leader_project_id) )
            ).link(
                file,
                on=( (path.pipt.member_project_id_namespace == file.project_id_namespace)
                     & (path.pipt.member_project_id == file.project) )
            )
        else:
            # link to directly attributed project
            path = path.link(file)
        # allow chained path-extension for caller
        return path_func(builder, path)

    @classmethod
    def projection_for_file_stats(cls, path, grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Function to build grouped projection of file stats.

        :param path: The path we will project from
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return path.groupby(*(
            [
                path.project.id_namespace.alias('project_id_namespace'),
                path.project.id.alias('project_id')
            ] + grpk_func(path)
        )).attributes(*(
            [
                Cnt(path.file).alias('file_cnt'),
                Sum(path.file.size_in_bytes).alias('byte_cnt'),
                # .name is part of API so need to use dict-style lookup of column...
                path.project.column_definitions['name'].alias('project_name'),
                path.project.RID.alias('project_RID')
            ] + attr_func(path)
        ))

    @classmethod
    def extend_project_path_to_biosample(cls, builder, path, use_root_projects, path_func=(lambda builder, path: path)):
        """Function to link biosample to existing project path by attribution.

        :param builder: A builder appropriate to use when extending path
        :param path: The path we will build from
        :param use_root_projects: Only consider root projects (default False)
        :param path_func: Function to allow path chaining (default no-change)
        """
        biosample = builder.CFDE.biosample
        if use_root_projects:
            # link to transitively-attributed root project
            pipt = builder.CFDE.project_in_project_transitive.alias('pipt')
            path = path.link(
                pipt,
                on=( (path.project.id_namespace == pipt.leader_project_id_namespace)
                     & (path.project.id == pipt.leader_project_id) )
            ).link(
                biosample,
                on=( (path.pipt.member_project_id_namespace == biosample.project_id_namespace)
                     & (path.pipt.member_project_id == biosample.project) )
            )
        else:
            # link to directly attributed project
            path = path.link(biosample)
        # allow chained path-extension for caller
        return path_func(builder, path)

    @classmethod
    def projection_for_biosample_stats(cls, path, grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Function to build grouped projection of biosample stats.

        :param path: The path we will project from
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return path.groupby(*(
            [
                path.project.id_namespace.alias('project_id_namespace'),
                path.project.id.alias('project_id')
            ] + grpk_func(path)
        )).attributes(*(
            [
                Cnt(path.biosample).alias('biosample_cnt'),
                # .name is part of API so need to use dict-style lookup of column...
                path.project.column_definitions['name'].alias('project_name'),
                path.project.RID.alias('project_RID')
            ] + attr_func(path)
        ))

    @classmethod
    def extend_project_path_to_subject(cls, builder, path, use_root_projects, path_func=(lambda builder, path: path)):
        """Function to link subject to existing project path by attribution.

        :param builder: A builder appropriate to use when extending path
        :param path: The path we will build from
        :param use_root_projects: Only consider root projects (default False)
        :param path_func: Function to allow path chaining (default no-change)
        """
        subject = builder.CFDE.subject
        if use_root_projects:
            # link to transitively-attributed root project
            pipt = builder.CFDE.project_in_project_transitive.alias('pipt')
            path = path.link(
                pipt,
                on=( (path.project.id_namespace == pipt.leader_project_id_namespace)
                     & (path.project.id == pipt.leader_project_id) )
            ).link(
                subject,
                on=( (path.pipt.member_project_id_namespace == subject.project_id_namespace)
                     & (path.pipt.member_project_id == subject.project) )
            )
        else:
            # link to directly attributed project
            path = path.link(subject)
        # allow chained path-extension for caller
        return path_func(builder, path)

    @classmethod
    def projection_for_subject_stats(cls, path, grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Function to build grouped projection of subject stats.

        :param path: The path we will project from
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return path.groupby(*(
            [
                path.project.id_namespace.alias('project_id_namespace'),
                path.project.id.alias('project_id')
            ] + grpk_func(path)
        )).attributes(*(
            [
                Cnt(path.subject).alias('subject_cnt'),
                # .name is part of API so need to use dict-style lookup of column...
                path.project.column_definitions['name'].alias('project_name'),
                path.project.RID.alias('project_RID')
            ] + attr_func(path)
        ))

    @classmethod
    def extend_file_path_to_assaytype(cls, builder, path):
        return path.link(
            builder.CFDE.file_assay_type,
            on=( (path.file.id_namespace == builder.CFDE.file_assay_type.file_id_namespace)
                 & (path.file.id == builder.CFDE.file_assay_type.file_id) )
        ).link(builder.CFDE.assay_type)

    @classmethod
    def extend_biosample_path_to_assaytype(cls, builder, path):
        return path.link(builder.CFDE.assay_type)

    @classmethod
    def extend_subject_path_to_assaytype(cls, builder, path):
        return (
            path.link(builder.CFDE.biosample_from_subject)
            .link(builder.CFDE.biosample)
            .link(builder.CFDE.assay_type)
        )

    @classmethod
    def extend_groupkeys_for_assaytype(cls, path):
        return [
            path.assay_type.id.alias('assay_type_id')
        ]

    @classmethod
    def extend_attributes_for_assaytype(cls, path):
        return [
            path.assay_type.column_definitions['name'].alias('assay_type_name')
        ]

    @classmethod
    def extend_file_path_to_anatomy(cls, builder, path):
        return path.link(
            builder.CFDE.file_anatomy,
            on=( (path.file.id_namespace == builder.CFDE.file_anatomy.file_id_namespace)
                 & (path.file.id == builder.CFDE.file_anatomy.file_id) )
        ).link(builder.CFDE.anatomy)

    @classmethod
    def extend_biosample_path_to_anatomy(cls, builder, path):
        return path.link(builder.CFDE.anatomy)

    @classmethod
    def extend_subject_path_to_anatomy(cls, builder, path):
        return (
            path.link(builder.CFDE.biosample_from_subject)
            .link(builder.CFDE.biosample)
            .link(builder.CFDE.anatomy)
        )

    @classmethod
    def extend_groupkeys_for_anatomy(cls, path):
        return [
            path.anatomy.id.alias('anatomy_id')
        ]

    @classmethod
    def extend_attributes_for_anatomy(cls, path):
        return [
            path.anatomy.column_definitions['name'].alias('anatomy_name')
        ]

    @classmethod
    def extend_file_path_to_datatype(cls, builder, path):
        return path.link(builder.CFDE.data_type)

    @classmethod
    def extend_groupkeys_for_datatype(cls, path):
        return [
            path.data_type.id.alias('data_type_id')
        ]

    @classmethod
    def extend_attributes_for_datatype(cls, path):
        return [
            path.data_type.column_definitions['name'].alias('data_type_name')
        ]

    @classmethod
    def extend_subject_path_to_file(cls, builder, path):
        return (
            path.link(builder.CFDE.file_describes_subject)
            .link(builder.CFDE.file)
        )

    @classmethod
    def extend_biosample_path_to_file(cls, builder, path):
        return (
            path.link(builder.CFDE.file_describes_biosample)
            .link(builder.CFDE.file)
        )

    @classmethod
    def extend_file_path_to_species(cls, builder, path):
        fsrt = builder.CFDE.file_subject_role_taxonomy.alias('fsrt')
        sr = builder.CFDE.subject_role.alias('sr')
        tax = builder.CFDE.ncbi_taxonomy.alias('tax')
        path = path.link(
            fsrt,
            on=( (path.file.id_namespace == fsrt.file_id_namespace)
                 & (path.file.id == fsrt.file_id) )
        ).link(
            sr,
            on=( path.fsrt.subject_role_id == sr.id )
        ).link(
            tax,
            on=( path.fsrt.subject_taxonomy_id == tax.id )
        )
        path = path.filter( sr.column_definitions['name'] == 'single organism' )
        path = path.filter( tax.clade == 'species' )
        return path

    @classmethod
    def extend_subject_path_to_species(cls, builder, path):
        srt = builder.CFDE.subject_role_taxonomy.alias('srt')
        sr = builder.CFDE.subject_role.alias('sr')
        tax = builder.CFDE.ncbi_taxonomy.alias('tax')
        path = path.link(
            srt,
            on=( (path.subject.id_namespace == srt.subject_id_namespace)
                 & (path.subject.id == srt.subject_id) )
        ).link(
            sr,
            on=( path.srt.role_id == sr.id )
        ).link(
            tax,
            on=( path.srt.taxonomy_id == tax.id )
        )
        path = path.filter( sr.column_definitions['name'] == 'single organism' )
        path = path.filter( tax.clade == 'species' )
        return path

    @classmethod
    def extend_biosample_path_to_subject(cls, builder, path):
        return (
            path.link(builder.CFDE.biosample_from_subject)
            .link(builder.CFDE.subject)
        )

    @classmethod
    def extend_groupkeys_for_species(cls, path):
        return [
            path.tax.id.alias('species_id')
        ]

    @classmethod
    def extend_attributes_for_species(cls, path):
        return [
            path.tax.column_definitions['name'].alias('species_name')
        ]

    def query_combination(self, root_projects=True, entity="file", vocabulary="anatomy"):
        """Perform dashboard query for desired combination of project, vocabulary, and entity stats.

        :param root_projects: Whether to only use root projects (default True)
        :param entity: Which entity table to summarize (default "file")
        :param vocabulary: Which concept to cross with project for grouping (default "anatomy")

        Allowed values:
        root_projects: True, False
        entity: "subject", "biosample", "file"
        vocabulary: "anatomy", "assay_type", "data_type"
        """
        if root_projects not in { True, False }:
            raise ValueError("Bad dimension key root_projects=%r" % root_projects)
        if entity not in { "subject", "biosample", "file" }:
            raise ValueError("Bad dimension key entity=%r" % entity)
        if vocabulary not in { "anatomy", "assay_type", "data_type", "species" }:
            raise ValueError("Bad dimension key vocabular=%r" % vocabulary)

        path_func2 = {
            ("file", "anatomy"): self.extend_file_path_to_anatomy,
            ("file", "assay_type"): self.extend_file_path_to_assaytype,
            ("file", "data_type"): self.extend_file_path_to_datatype,
            ("file", "species"): self.extend_file_path_to_species,

            ("biosample", "anatomy"): self.extend_biosample_path_to_anatomy,
            ("biosample", "assay_type"): self.extend_biosample_path_to_assaytype,
            ("biosample", "data_type"): lambda builder, path: self.extend_file_path_to_datatype(builder, self.extend_biosample_path_to_file(builder, path)),
            ("biosample", "species"): lambda builder, path: self.extend_subject_path_to_species(builder, self.extend_biosample_path_to_subject(builder, path)),

            ("subject", "anatomy"): self.extend_subject_path_to_anatomy,
            ("subject", "assay_type"): self.extend_subject_path_to_assaytype,
            ("subject", "data_type"): lambda builder, path: self.extend_file_path_to_datatype(builder, self.extend_subject_path_to_file(builder, path)),
            ("subject", "species"): self.extend_subject_path_to_species,
        }[(entity, vocabulary)]

        path_func, proj_func = {
            "file": (self.extend_project_path_to_file, self.projection_for_file_stats),
            "biosample": (self.extend_project_path_to_biosample, self.projection_for_biosample_stats),
            "subject": (self.extend_project_path_to_subject, self.projection_for_subject_stats),
        }[entity]

        grpk_func, attr_func = {
            "anatomy": (self.extend_groupkeys_for_anatomy, self.extend_attributes_for_anatomy),
            "assay_type": (self.extend_groupkeys_for_assaytype, self.extend_attributes_for_assaytype),
            "data_type": (self.extend_groupkeys_for_datatype, self.extend_attributes_for_datatype),
            "species": (self.extend_groupkeys_for_species, self.extend_attributes_for_species),
        }[vocabulary]

        return self.list_projects(
            use_root_projects=root_projects,
            path_func=lambda builder, path: path_func(builder, path, root_projects, path_func2),
            proj_func=lambda path: proj_func(path, grpk_func, attr_func),
        )


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
