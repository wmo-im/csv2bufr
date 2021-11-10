#!/usr/bin/python3
import sys
import argparse
import json
import pandas as pd

BUFR_PATH = "./bufr4/"

class bufr_sequence:
    table_D = None
    table_B = None
    def __init__(self, unexpandedSequence, version):
        assert isinstance( unexpandedSequence, list)
        assert isinstance( version, int )
        # first load tables
        if self.table_B is None:
            self.table_B = pd.read_csv( "{}/{}/BUFRCREX_TableB_en.txt".format(BUFR_PATH, version), dtype='object')
            self.table_B['BUFR_DataWidth_Bits'] = self.table_B['BUFR_DataWidth_Bits'].map(int)
            self.table_B['BUFR_Scale'] = self.table_B['BUFR_Scale'].map(int)
            self.table_B['BUFR_ReferenceValue'] = self.table_B['BUFR_ReferenceValue'].map(float)
            self.table_B_tmp = self.table_B.copy()
        if self.table_D is None:
            self.table_D = pd.read_csv("{}/{}/BUFR_TableD_en.txt".format(BUFR_PATH, version), dtype='object')
        self.unexpandedSequence = unexpandedSequence

    def expandedSequence(self):
        return( self._expand_sequence( self.unexpandedSequence ) )

    def _expand_sequence(self, sequence ):
        # first iterate over sequence performing any replication
        idx = 0
        content = list()
        sequence_length = len( sequence )
        while idx < sequence_length :
            d = sequence[idx]
            idx += 1
            if d[0] == '1':
                nelem = int(d[1:3]) # get number of elements
                nreps = int(d[3:6]) # number of replications
                assert nelem > 0, "Replication error, nelem < 1"
                if nreps < 1:
                    nreps = 1
                assert nreps > 0, "Delayed replication unsupported in call to expand_sequence"
                for rep in range( nreps ) :
                    content.extend( sequence[idx : idx + nelem] )
                idx += nelem
            else :
                content.append( d )
        # next expand D sequences
        content2 = list()
        nested = False
        sequence_length = len( content )
        idx = 0
        while idx < sequence_length :
            d = content[ idx ]
            idx += 1
            if d[0] == '3' :
                expanded_sequence = (list(self.table_D.loc[self.table_D['FXY1'] == d, 'FXY2'].copy()))
                for e in expanded_sequence:
                    if (e[0] == '3') | (e[0] == '1'):
                        nested = True
                    content2.append( e )
            else:
                content2.append( d )
        if nested:
            content2 = self._expand_sequence( content2 )
        return( content2 )


def eccodes_abbreviation(  element_name ):
    # first convert to title case
    element_name = element_name.title()
    # remove punctuation and spaces
    element_name = element_name.replace(" ","")
    # make first character lower case and return
    return( ''.join(    [element_name[0].lower(), element_name[1:] ] ) )

def eccodes_type( bufr_units ):
    if bufr_units == "CCITT IA5":
        type = "string"
    elif bufr_units == "Code table":
        type = "table"
    elif bufr_units == "Flag table":
        type = "flag"
    else:
        type = "double"

def main( argv ):
    parser = argparse.ArgumentParser(description='expand sequence')
    parser.add_argument("--input", dest="input", required=True, help="JSON file containing sequence to expand")
    parser.add_argument("--version", dest="version", required=False, default=36, type=int, help="Version of tables to use")
    args = parser.parse_args()

    with open( args.input ) as fh:
        sequence = json.load( fh )

    seq = bufr_sequence( sequence["unexpandedSequence"], args.version )
    expanded_sequence = seq.expandedSequence()

    print( expanded_sequence )
    eccodes_tableB = pd.read_csv("/opt/eccodes/share/eccodes/definitions/bufr/tables/0/wmo/36/element.table" ,
                                   dtype='object' , sep="|")

    keyCount = dict()

    for elem in expanded_sequence:
        if elem[0] == "0":
            if elem not in eccodes_tableB["#code"].values:
                # create eccodes like entry
                print(elem)

                #print( seq.table_B.loc[ seq.table_B['FXY'] == elem ] )
                if elem not in seq.table_B['FXY'].values:
                    assert False
                new_row = seq.table_B.loc[seq.table_B['FXY']==elem, ('FXY','ElementName_en', 'BUFR_Unit', 'BUFR_Scale',
                                                              'BUFR_ReferenceValue', 'BUFR_DataWidth_Bits', 'CREX_Unit',
                                                              'CREX_Scale', 'CREX_DataWidth_Char') ]
                eccodes_map = {
                    'FXY':'#code', 'ElementName_en':'name', 'BUFR_Unit':'unit', 'BUFR_Scale':'scale',
                     'BUFR_ReferenceValue':'reference', 'BUFR_DataWidth_Bits':'width', 'CREX_Unit':'crex_unit',
                     'CREX_Scale':'crex_scale', 'CREX_DataWidth_Char':'crex_width'
                }
                new_row.rename( columns = eccodes_map , inplace = True)
                new_row = new_row.assign( abbreviation = eccodes_abbreviation(new_row['name'].values[0] ) )
                new_row = new_row.assign( type=eccodes_type( new_row['unit'].values[0] ) )
                eccodes_tableB = eccodes_tableB.append( new_row )
            key = list(eccodes_tableB.loc[ eccodes_tableB["#code"]==elem , "abbreviation"])[0]
            if key in keyCount:
                keyCount[key] += 1
            else:
                keyCount[key] = 1
            key = "#{}#{}".format(keyCount[key], key )
            print( '  {"key":"'+key+'", "value":null, "column":null, "valid-min":null, "valid-max":null}')
        else:
            print( elem )

    eccodes_tableB = eccodes_tableB.sort_values( by = "#code" )
    eccodes_tableB.to_csv( "test.txt", sep = "|", index = False, na_rep='NA')

    return 0


if __name__ == "__main__":
    main( sys.argv[1:] )