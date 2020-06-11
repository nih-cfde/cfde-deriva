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
            #'list_project_biosample_stats': list(self.list_project_biosample_stats()),
            #'list_project_file_stats': list(self.list_project_file_stats()),
            #'list_project_subject_stats': list(self.list_project_subject_stats()),
            'list_project_anatomy_file_stats': list(self.list_project_anatomy_file_stats()),
            'list_project_anatomy_biosample_stats': list(self.list_project_anatomy_biosample_stats()),
            'list_project_anatomy_subject_stats': list(self.list_project_anatomy_subject_stats()),
            'list_project_assaytype_file_stats': list(self.list_project_assaytype_file_stats()),
            'list_project_assaytype_biosample_stats': list(self.list_project_assaytype_biosample_stats()),
            'list_project_assaytype_subject_stats': list(self.list_project_assaytype_subject_stats()),
            'list_project_datatype_file_stats': list(self.list_project_datatype_file_stats()),
            'list_project_datatype_biosample_stats': list(self.list_project_datatype_biosample_stats()),
            'list_project_datatype_subject_stats': list(self.list_project_datatype_subject_stats()),
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

    def list_project_file_stats(self, use_root_projects=True, path_func=(lambda builder, path: path), grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Return list of file statistics per project.

        :param use_root_projects: Summarize by root project rather than attributed sub-projects (default true).
        :param path_func: Function to allow path chaining (default no-change)
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return self.list_projects(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_project_path_to_file(builder, path, use_root_projects, path_func)),
            proj_func=(lambda path: self.projection_for_file_stats(path, grpk_func, attr_func))
        )

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

    def list_project_biosample_stats(self, use_root_projects=True, path_func=(lambda builder, path: path), grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Return list of biosample statistics per project.

        :param use_root_projects: Summarize by root project rather than attributed sub-projects (default true).
        :param path_func: Function to allow path chaining (default no-change)
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return self.list_projects(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_project_path_to_biosample(builder, path, use_root_projects, path_func)),
            proj_func=(lambda path: self.projection_for_biosample_stats(path, grpk_func, attr_func))
        )

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

    def list_project_subject_stats(self, use_root_projects=True, path_func=(lambda builder, path: path), grpk_func=(lambda path: []), attr_func=(lambda path: [])):
        """Return list of subject statistics per project.

        :param use_root_projects: Summarize by root project rather than attributed sub-projects (default true).
        :param path_func: Function to allow path chaining (default no-change)
        :param grpk_func: Function returning extra groupby cols (default empty)
        :param attr_func: Function returning extra attribute cols (default empty)
        """
        return self.list_projects(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_project_path_to_subject(builder, path, use_root_projects, path_func)),
            proj_func=(lambda path: self.projection_for_subject_stats(path, grpk_func, attr_func))
        )

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

    def list_project_assaytype_file_stats(self, use_root_projects=True):
        return self.list_project_file_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_file_path_to_assaytype(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_assaytype(path)),
            attr_func=(lambda path: self.extend_attributes_for_assaytype(path))
        )

    def list_project_assaytype_biosample_stats(self, use_root_projects=True):
        return self.list_project_biosample_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_biosample_path_to_assaytype(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_assaytype(path)),
            attr_func=(lambda path: self.extend_attributes_for_assaytype(path))
        )

    def list_project_assaytype_subject_stats(self, use_root_projects=True):
        return self.list_project_subject_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_subject_path_to_assaytype(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_assaytype(path)),
            attr_func=(lambda path: self.extend_attributes_for_assaytype(path))
        )

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

    def list_project_anatomy_file_stats(self, use_root_projects=True):
        return self.list_project_file_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_file_path_to_anatomy(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_anatomy(path)),
            attr_func=(lambda path: self.extend_attributes_for_anatomy(path))
        )

    def list_project_anatomy_biosample_stats(self, use_root_projects=True):
        return self.list_project_biosample_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_biosample_path_to_anatomy(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_anatomy(path)),
            attr_func=(lambda path: self.extend_attributes_for_anatomy(path))
        )

    def list_project_anatomy_subject_stats(self, use_root_projects=True):
        return self.list_project_subject_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_subject_path_to_anatomy(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_anatomy(path)),
            attr_func=(lambda path: self.extend_attributes_for_anatomy(path))
        )

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

    def list_project_datatype_file_stats(self, use_root_projects=True):
        return self.list_project_file_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_file_path_to_datatype(builder, path)),
            grpk_func=(lambda path: self.extend_groupkeys_for_datatype(path)),
            attr_func=(lambda path: self.extend_attributes_for_datatype(path))
        )

    def list_project_datatype_biosample_stats(self, use_root_projects=True):
        return self.list_project_biosample_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_file_path_to_datatype(builder, self.extend_biosample_path_to_file(builder, path))),
            grpk_func=(lambda path: self.extend_groupkeys_for_datatype(path)),
            attr_func=(lambda path: self.extend_attributes_for_datatype(path))
        )
    def list_project_datatype_subject_stats(self, use_root_projects=True):
        return self.list_project_subject_stats(
            use_root_projects=use_root_projects,
            path_func=(lambda builder, path: self.extend_file_path_to_datatype(builder, self.extend_subject_path_to_file(builder, path))),
            grpk_func=(lambda path: self.extend_groupkeys_for_datatype(path)),
            attr_func=(lambda path: self.extend_attributes_for_datatype(path))
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
