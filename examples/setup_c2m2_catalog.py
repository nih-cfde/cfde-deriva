#!/usr/bin/python3

from deriva.core import DerivaServer, get_credential, urlquote, AttrDict
from deriva.core.ermrest_model import builtin_types, Schema, Table, Column, Key, ForeignKey
from deriva.core.ermrest_config import tag

"""
Basic C2M2 catalog sketch

Demonstrates use of deriva-py APIs:
- server authentication (assumes active deriva-auth agent)
- catalog creation
- model provisioning
- basic configuration of catalog ACLs
- small Chaise presentation tweaks via model annotations
- simple insertion of tabular content

"""

# this is the deriva server where we will create a catalog
servername = 'demo.derivacloud.org'

## bind to server
credentials = get_credential(servername)
server = DerivaServer('https', servername, credentials)

## create catalog
catalog = server.create_ermrest_catalog()
print('New catalog has catalog_id=%s' % catalog.catalog_id)
print("Don't forget to delete it if you are done with it!")

## provision a model and basic ACLs/configuration

# get catalog's model and configuration management API
model_root = catalog.getCatalogModel()

# create named schema for our demo tables
cfde_schema = model_root.create_schema(
    catalog, # this argument will go away in a future version of deriva-py
    Schema.define(
        "CFDE",
        comment="Core CFDE C2M2 tables."
    )
)

# set some reasonable catalog-wide ACLs for demo...

# some useful group IDs to use later in ACLs...
grp = AttrDict({
    # USC/ISI ISRD roles
    "isrd_staff": "https://auth.globus.org/176baec4-ed26-11e5-8e88-22000ab4b42b",
    'isrd_testers':    "https://auth.globus.org/9d596ac6-22b9-11e6-b519-22000aef184d",
    # demo.derivacloud.org roles
    "demo_admin": "https://auth.globus.org/5a773142-e2ed-11e8-a017-0e8017bdda58",
    "demo_creator": "https://auth.globus.org/bc286232-a82c-11e9-8157-0ed6cb1f08e0",
    "demo_writer": "https://auth.globus.org/caa11064-e2ed-11e8-9d6d-0a7c1eab007a",
    "demo_curator": "https://auth.globus.org/a5cfa412-e2ed-11e8-a768-0e368f3075e8",
    "demo_reader": "https://auth.globus.org/b9100ea4-e2ed-11e8-8b39-0e368f3075e8",
})
writers = [grp.demo_curator, grp.demo_writer]

model_root.acls.update({
    "owner": [grp.demo_admin, grp.demo_creator],
    "insert": writers,
    "update": writers,
    "delete": writers,
    "select": [grp.demo_reader, grp.isrd_testers, grp.isrd_staff],
    "enumerate": ["*"],
})

# set custom chaise configuration values for this catalog
model_root.annotations[tag.chaise_config] = {
    # hide system metadata by default in tabular listings, to focus on CFDE-specific content
    "SystemColumnsDisplayCompact": [],
}

# have Chaise display underscores in model element names as whitespace
cfde_schema.display.name_style = {"underline_space": True}

# prettier display of built-in ERMrest_Client table entries
model_root.table('public', 'ERMrest_Client').table_display.row_name = {
    "row_markdown_pattern": "{{{Full_Name}}} ({{{Display_Name}}})"
}

## apply the above ACL and annotation changes to server
model_root.apply(catalog)

## now define several tables for the C2M2 sketch

# core dataset tracking table
dataset_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Dataset",
        [
            Column.define(
                "ID",
                builtin_types.text, 
                nullok=False,
                comment="The globally unique ID (such as URI) for this dataset.",
            ),
            Column.define(
                "Title",
                builtin_types.text,
                nullok=False,
                comment="The human-readable title for this dataset.",
            ),
            Column.define(
                "Description",
                builtin_types.text,
                nullok=False,
                comment="The human-readable description of this dataset.",
            ),
            Column.define(
                "Part_Of",
                builtin_types.text,
                nullok=True,
                comment="Parent dataset ID, if this dataset is part of a parent.",
            )
        ],
        [
            Key.define(
                ["ID"],
                constraint_names=[["CFDE", "Dataset_ID_key"]],
                comment="Dataset ID should be globally unique.",
            ),
            Key.define(
                ["Title"],
                constraint_names=[["CFDE", "Dataset_Title_key"]],
                comment="Dataset Title should be unique.",
            ),
        ],
        [
            ForeignKey.define(
                ["Part_Of"],
                "CFDE",
                "Dataset",
                ["ID"],
                on_update="CASCADE",
                on_delete="SET NULL",
                constraint_names=[["CFDE", "Dataset_Part_Of_fkey"]],
                comment="Dataset may be Part_Of another parent dataset.",
            ),
        ] ,
        comment="Dataset is a partitive representation of externally-hosted data resources.",
    )
)

# distribution table for tracking external URLs
distribution_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Distribution",
        [
            Column.define(
                "Dataset",
                builtin_types.text,
                nullok=False,
                comment="The dataset ID which corresponds to the content in this distribution.",
            ),
            Column.define(
                "Landing_Page",
                builtin_types.text,
                nullok=False,
                comment="A web resource URL meant to be visited by users interested in this distribution.",
            ),
        ],
        [
            # no extra keys at present...
        ],
        [
            ForeignKey.define(
                ["Dataset"],
                "CFDE",
                "Dataset",
                ["ID"],
                on_update="CASCADE",
                on_delete="NO ACTION",
                constraint_names=[["CFDE", "Distribution_Dataset_fkey"]],
            ),
        ] ,
        comment="Distribution represents individually retrievable data endpoints."
    )
)

# organization provenance
org_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Organization",
        [
            Column.define(
                "ID",
                builtin_types.text,
                nullok=False,
                comment="A unique abbreviated identifier for this organization.",
            ),
            Column.define(
                "Name",
                builtin_types.text,
                nullok=False,
                comment="The unique name for this organization.",
            ),
        ],
        [
            Key.define(
                ["ID"],
                constraint_names=[["CFDE", "Organization_ID_key"]],
            ),
            Key.define(
                ["Name"],
                constraint_names=[["CFDE", "Organization_Name_key"]],
            ),
        ],
        [
            # no foreign keys at present...
        ] ,
        comment="Organization represents external operating entities who may produce data.",
    )
)
# TBD: what about Person etc?
# should we have one association per creator type?  or some other polymorphic model?

# associate datasets and organizations
dataset_creator_org = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Dataset_Creator_Organization",
        [
            Column.define(
                "Dataset",
                builtin_types.text,
                nullok=False,
                comment="The ID of the dataset which was created.",
            ),
            Column.define(
                "Organization",
                builtin_types.text,
                nullok=False,
                comment="The ID for the organization who created the dataset.",
            )
        ],
        [
            Key.define(
                ["Dataset", "Organization"],
                constraint_names=[["CFDE", "Dataset_Creator_Organization_natural_key"]],
                comment="Each combination of dataset and organization can be asserted at most once.",
            ),
        ],
        [
            ForeignKey.define(
                ["Dataset"],
                "CFDE",
                "Dataset",
                ["ID"],
                on_update="CASCADE",
                on_delete="CASCADE",
                constraint_names=[["CFDE", "Dataset_Creator_Organization_Dataset_fkey"]],
            ),
            ForeignKey.define(
                ["Organization"],
                "CFDE",
                "Organization",
                ["ID"],
                on_update="CASCADE",
                on_delete="CASCADE",
                constraint_names=[["CFDE", "Dataset_Creator_Organization_Organization_fkey"]],
            ),
        ] ,
    )
)

# some vocabulary term tables for externally defined concepts
# these also demonstrate configuration of the ermrest+ermresolve
# mechanism using CURIE and URI templates for column defaults

info_term_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define_vocabulary(
        "Information_Term",
        "CFDE_Test_%s:{RID}" % (catalog.catalog_id,),
        "/id/%s/{RID}" % (catalog.catalog_id,),
        comment="Information_Term is a controlled vocabulary for kinds of information.",
    )
)

method_term_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define_vocabulary(
        "Method_Term",
        "CFDE_Test_%s:{RID}" % (catalog.catalog_id,),
        "/id/%s/{RID}" % (catalog.catalog_id,),
        comment="Method_Term is a controlled vocabulary for data capture methods.",
    )
)

platform_term_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define_vocabulary(
        "Platform_Term",
        "CFDE_Test_%s:{RID}" % (catalog.catalog_id,),
        "/id/%s/{RID}" % (catalog.catalog_id,),
        comment="Platform_Term is a controlled vocabulary for data capture platforms.",
    )
)

# now, the data type table using controlled vocabularies from above
datatype_table = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Data_Type",
        [
            Column.define(
                "Information",
                builtin_types.text,
                nullok=False,
                comment="The kind of information represented by the data (as a term ID).",
            ),
            Column.define(
                "Method",
                builtin_types.text,
                nullok=False,
                comment="The method of data capture (as a term ID).",
            ),
            Column.define(
                "Platform",
                builtin_types.text,
                nullok=False,
                comment="The data capture platform (as a term ID).",
            ),
        ],
        [
            Key.define(
                ["Information", "Method", "Platform"],
                constraint_names=[["CFDE", "Data_Type_natural_key"]],
                comment="This assumes we want to reuse a common tuple for each descriptive combination.",
            ),
        ],
        [
            ForeignKey.define(
                ["Information"],
                "CFDE",
                "Information_Term",
                ["ID"],
                on_update="CASCADE",
                on_delete="NO ACTION",
                constraint_names=[["CFDE", "Data_Type_Information_fkey"]],
            ),
            ForeignKey.define(
                ["Method"],
                "CFDE",
                "Method_Term",
                ["ID"],
                on_update="CASCADE",
                on_delete="NO ACTION",
                constraint_names=[["CFDE", "Data_Type_Method_fkey"]],
            ),
            ForeignKey.define(
                ["Platform"],
                "CFDE",
                "Platform_Term",
                ["ID"],
                on_update="CASCADE",
                on_delete="NO ACTION",
                constraint_names=[["CFDE", "Data_Type_Platform_fkey"]],
            ),
        ] ,
        comment="Data_Type characterizes captured data.",
    )
)

# associate dataset and data-type
dataset_datatype_assoc = cfde_schema.create_table(
    catalog, # this argument will go away in a future version of deriva-py
    Table.define(
        "Dataset_Data_Type",
        [
            Column.define(
                "Dataset",
                builtin_types.text,
                nullok=False,
                comment="The dataset ID to which a type is being associated.",
            ),
            Column.define(
                "Data_Type",
                builtin_types.text,
                nullok=False,
                comment="The data type RID to which a dataset is being associated.",
            ),
        ],
        [
            Key.define(
                ["Dataset", "Data_Type"],
                constraint_names=[["CFDE", "Dataset_Data_Type_natural_key"]],
                comment="Each combination of dataset and data type can be expressed at most once.",
            ),
        ],
        [
            ForeignKey.define(
                ["Dataset"],
                "CFDE",
                "Dataset",
                ["ID"],
                on_update="CASCADE",
                on_delete="CASCADE",
                constraint_names=[["CFDE", "Dataset_Data_Type_Dataset_fkey"]],
            ),
            ForeignKey.define(
                ["Data_Type"],
                "CFDE",
                "Data_Type",
                ["RID"],
                on_update="CASCADE",
                on_delete="CASCADE",
                constraint_names=[["CFDE", "Dataset_Data_Type_Data_Type_fkey"]],
            ),
        ] ,
        comment="A binary association of Dataset and Data_Type records by their respective IDs.",
    )
)

# TODO
# - dimension controlled vocabulary table
# - dimension to dataset association table
# - cohort/specimen/assay detailed metadata tables?
# - extension table for source-specific (e.g. GTEx) metadata columns?

print("Model populated.")

## now load some sample data for this demo, scraped from minimal C2M2 sketch

# get root of datapath API
pb = catalog.getPathBuilder()

# get path-builder's idea of schema
cfde_schema = pb.schemas["CFDE"]

cfde_schema.Information_Term.insert(
    [
        {
            "ID": "obo:OBI_0000626",
            "URI": "http://purl.obolibrary.org/obo/OBI_0000626",
            "Name": "DNA sequencing",
            "Description": "DNA sequencing longer description here",
            "Synonyms": None
        },
        {
            "ID": "obo:OBI_0000424",
            "URI": "http://purl.obolibrary.org/obo/OBI_0000424",
            "Name": "Transcription profiling",
            "Description": "Transcription profiling longer description...",
            "Synonyms": None
        }
    ]
)

cfde_schema.Method_Term.insert(
    [
        {
            "ID": "obo:OBI_0002117",
            "URI": "http://purl.obolibrary.org/obo/OBI_0002117",
            "Name": "Whole genome sequencing assay",
            "Description": "Whole genome sequencing assay longer description...",
            "Synonyms": None
        },
        {
            "ID": "obo:OBI_0001271",
            "URI": "http://purl.obolibrary.org/obo/OBI_0001271",
            "Name": "RNA-seq assay",
            "Description": "RNA-seq assay longer description...",
            "Synonyms": None
        }
    ]
)

cfde_schema.Platform_Term.insert(
    [
        {
            "ID": "obo:OBI_0000759",
            "URI": "http://purl.obolibrary.org/obo/OBI_0000759",
            "Name": "Illumina",
            "Description": "Illumina longer description...",
            "Synonyms": None
        }
    ]
)

# we'll need a mapping of (Information, Method, Platform)->RID later
# so collect the response from insert() into a suitable dictionary
data_types = {
    (row["Information"], row["Method"], row["Platform"]): row["RID"]
    for row in cfde_schema.Data_Type.insert(
            [
                {
                    "Information": "obo:OBI_0000626",
                    "Method": "obo:OBI_0002117",
                    "Platform": "obo:OBI_0000759"
                },
                {
                    "Information": "obo:OBI_0000424",
                    "Method": "obo:OBI_0001271",
                    "Platform": "obo:OBI_0000759"
                }
            ]
    )
}

cfde_schema.Dataset.insert(
    [
        {
            "ID": "top-level-uri-goes-here",
            "Title": "Genotype-Tissue Expression Project (GTEx)",
            "Description": "GTEx provides a resource with which to study human gene expression and regulation and its relationship to genetic variation. It is funded by the NIH Common Fund.",
            "Part_Of": None
        },
        {
            "ID": "dbgap_study:phs000424.v7.p2",
            "Title": "Genotype-Tissue Expression Project (GTEx) WGS and RNA-Seq data",
            "Description": "Better description of this part...",
            "Part_Of": "top-level-uri-goes-here"
        }
    ]
)

cfde_schema.Distribution.insert(
    [
        {
            "Dataset": "top-level-uri-goes-here",
            "Landing_Page": "https://www.ncbi.nlm.nih.gov/gap/?term=phs000424"
        }
    ]
)

cfde_schema.Organization.insert(
    [
        {
            "ID": "NHGRI",
            "Name": "National Human Genome Research Institute"
        }
    ]
)

cfde_schema.Dataset_Creator_Organization.insert(
    [
        {
            "Dataset": "top-level-uri-goes-here",
            "Organization": "NHGRI"
        },
        {
            "Dataset": "dbgap_study:phs000424.v7.p2",
            "Organization": "NHGRI"
        }
    ]
)

cfde_schema.Dataset_Data_Type.insert(
    [
        {
            "Dataset": row["Dataset"],
            # we have to normalize the type combinations into a data-type RID here
            "Data_Type": data_types[(
                row["Information"],
                row["Method"],
                row["Platform"]
            )]
        }
        for row in [
                {
                    "Dataset": "top-level-uri-goes-here",
                    "Information": "obo:OBI_0000626",
                    "Method": "obo:OBI_0002117",
                    "Platform": "obo:OBI_0000759"
                },
                {
                    "Dataset": "top-level-uri-goes-here",
                    "Information": "obo:OBI_0000424",
                    "Method": "obo:OBI_0001271",
                    "Platform": "obo:OBI_0000759"
                },
                {
                    "Dataset": "dbgap_study:phs000424.v7.p2",
                    "Information": "obo:OBI_0000626",
                    "Method": "obo:OBI_0002117",
                    "Platform": "obo:OBI_0000759"
                },
                {
                    "Dataset": "dbgap_study:phs000424.v7.p2",
                    "Information": "obo:OBI_0000424",
                    "Method": "obo:OBI_0001271",
                    "Platform": "obo:OBI_0000759"
                }
        ]
    ]
)

print("Sample data populated.")

print("Try visiting 'https://%s/chaise/recordset/#%s/CFDE:Dataset'" % (servername, catalog.catalog_id))

## to re-bind to the same catalog in the future, extract catalog_id from URL

# server = DerivaServer('https', servername, credentials)
# catalog_id = '1234'
# catalog = server.connect_ermrest(catalog_id)

## after binding to your catalog, you can delete it too
## but we force you to be explicit:

# catalog.delete_ermrest_catalog(really=True)

