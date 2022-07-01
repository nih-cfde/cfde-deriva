
import sys
import csv

def dbgap_search_result_to_cv(f_in, f_out):
    """Convert a dbGaP search result CSV listing to our dbgap_study_id quasi vocab TSV"""
    reader = csv.reader(f_in)
    writer = csv.writer(f_out, delimiter='\t', lineterminator='\n')

    header = next(reader)
    indices = {
        header[i]: i
        for i in range(len(header))
    }

    writer.writerow( ('id','name','description') )

    for row in reader:
        id = row[ indices['accession'] ].split('.')[0] # phsXXX prefix
        name = row[ indices['name'] ]
        description = row[ indices['description'] ]
        writer.writerow( (id, name, description) )

    return 0

def main(infile, outfile=None):

    def get_file(f, default, mode):
        if f is None:
            return default
        elif isinstance(f, str):
            return open(f, mode)
        else:
            return f
    
    f_in = get_file(infile, None, 'r')
    f_out = get_file(outfile, sys.stdout, 'w')

    return dbgap_search_result_to_cv(f_in, f_out)

if __name__ == '__main__':
    try:
        r = main(*sys.argv[1:])
    except Exception as e:
        print("""
error: %s
usage: %s infile [ outfile ]

This script expects a CSV input file that has been downloaded
from the dbGaP query page:

  https://www.ncbi.nlm.nih.gov/gap/advanced_search/

Listing all publically known dbGaP accession IDs.

""" % (e, sys.argv[0]))
        raise
    exit(r)
