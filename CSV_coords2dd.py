#!/usr/bin/env python2

## Erik Husby, PGC

import sys
import getopt

coordField_names = ["Point 1", "Point 2", "Point 3", "Point 4",
                    "TARGET LANDING AREA"]


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

class BadError(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:v", ["help", "output="])
        except getopt.error, msg:
             raise Usage(msg)
        output = None
        verbose = False
        for o, a in opts:
            if o == "-v":
                verbose = True
            elif o in ("-h", "--help"):
                print "Looks like you need help."
                sys.exit()
            elif o in ("-o", "--output"):
                output = a
            else:
                assert False, "unhandled option"
                
        try:
            iFile = open(args[0], "r")
        except:
            sys.exit()
            
        header = iFile.readline().rstrip().split(",")
        coordField_indices = list()
        try:
            for name in coordField_names:
                coordField_indices.append(header.index(name))
        except ValueError, msg:
            raise BadError('Cannot find the specified field name in header: "' +
                           str(msg).split("'")[1] + '"\nTerminating program.')
        
        coord_dim = 0
        line = iFile.readline()
        row = line.rstrip().split(",")
        i = 0
        length = len(coordField_indices)
        while i < length:
            test = testCoord(row[coordField_indices[i]])
            if test != "failed":
                coord_dim = test[0]
                coord_order = test[1]
            i += 1
        if coord_dim == 0:
            raise BadError("Coordinate values are missing cardinality.\n\
Terminating program.")
            
        rowInsert(newFieldNames, header, coordField_indices, coord_dim, coord_order)
        rowInsert(makeCoordinates, row, coordField_indices, coord_dim, coord_order)
        
        oFile = open(args[0].replace(".csv","--EDIT.csv"), "w")
        writeRow(oFile, header)
        writeRow(oFile, row)
        
        line = iFile.readline()
        while line != "":
            row = line.rstrip().split(",")
            rowInsert(makeCoordinates, row, coordField_indices, coord_dim, coord_order)
            writeRow(oFile, row)
            line = iFile.readline()
            
        iFile.close()
        oFile.close()
        print args[0].replace(".csv","--EDIT.csv") + " successfully created!"
        
    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2
    
    except BadError, err:
        print >>sys.stderr, err.msg
        try:
            iFile.close()
            oFile.close()
        except UnboundLocalError:
            None


def testCoord(coord):
    dim = 0
    ordering = list()
    for ele in coord:
        if ele in "NS":
            ordering.append("Lat")
            dim += 1
        elif ele in "EW":
            ordering.append("Long")
            dim += 1
    if dim not in (1, 2):
        return "failed"
    else:
        return (dim, ordering)


def coord2DDM(coord, dim, ordering):
    sign_lat = ""
    sign_long = ""
    
    coordLst = list()
    coordPart = -1
    numSection = False
    length = len(coord)
    if length == 0:
        if dim == 1:
            return "0.000000"
        elif dim == 2:
            return ("0.000000", "0.000000")
        else:
            raise BadError("Invalid dimensions in coord2DDM.")
            return None
    
    i = 0
    while i < length:
        ele = coord[i]
        if ele.isdigit() or ele == ".":
            if not numSection:
                coordLst.append(ele)
                coordPart += 1
                numSection = True
            else:
                coordLst[coordPart] += ele
        else:
            numSection = False
            if ele == "S":
                sign_lat = "-"
            elif ele == "W":
                sign_long = "-"
        i += 1
        
    if dim == 1:
        try:
            degrees = float(coordLst[0])
            degrees += float(coordLst[1]) / 60
            degrees += float(coordLst[2]) / 3600
        except:
            None
        return sign_lat + sign_long + '%.6f' % degrees
    
    elif dim == 2:
        terms = int(len(coordLst) / 2)
        index_lat = ordering.index("Lat") * terms
        index_long = ordering.index("Long") * terms
        deg_lat = 0
        deg_long = 0
        i = 0
        while i < terms:
            deg_lat += float(coordLst[i + index_lat]) / pow(60, i)
            deg_long += float(coordLst[i + index_long]) / pow(60, i)
            i += 1
        return (sign_lat + '%.6f' % deg_lat,
                sign_long + '%.6f' % deg_long)
    
    else:
        raise BadError("Invalid dimensions in coord2DDM.")
        return None


def rowInsert(fn, row, indicesLst, outDim, ordering):
    i = 0
    length = len(indicesLst)
    while i < length:
        bumpedIndex = indicesLst[i] + (outDim * i)
        inserts = fn(row, bumpedIndex, outDim, ordering)
        if outDim == 1:
            row.insert(bumpedIndex + 1, inserts)
        elif outDim == 2:
            row.insert(bumpedIndex + 1, inserts[1])
            row.insert(bumpedIndex + 1, inserts[0])
        else:
            raise BadError("Invalid dimensions in rowInsert.")
        i += 1


def newFieldNames(header, fIndex, dim, ordering):
    name = header[fIndex]
    if dim == 1:
        return name + "_DD"
    elif dim == 2:
        return (name + "_Lat_DD", name + "_Long_DD")
    else:
        raise BadError("Invalid dimensions in newFieldNames.")
        return None


def makeCoordinates(row, index, dim, ordering):
    return coord2DDM(row[index], dim, ordering)


def writeRow(f, lst):
    length = len(lst) - 1
    i = 0
    while i < length:
        f.write(lst[i] + ",")
        i += 1
    f.write(lst[i] + "\n")



if __name__ == "__main__":
    sys.exit(main())
