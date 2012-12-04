// -*- c-file-style: "bsd"; c-file-offsets: ((innamespace . 0)) -*-

#ifndef _WEBGRAPH_H
#define _WEBGRAPH_H

#include <stdexcept>
#include <string>
#include <vector>

#include <stdint.h>

namespace WebGraph {

struct IOError : public std::runtime_error
{
        IOError(const std::string& msg) : runtime_error(msg) {}
};

struct FormatError : public std::runtime_error
{
        FormatError(const std::string& msg) : runtime_error(msg) {}
};

class Node;
class URLReader;

/**
 * A sequential reader for WebGraph databases.
 */
class Reader
{
private:
        class BitReader *r;
        const std::string basePath;

        int windowSize;
        int minIntervalLength;
        int zetaK;

        Node *window;
        int node;

        int nat2int(uint32_t nat);

public:
        /**
         * The number of nodes in this graph.
         */
        int nodes;

        /**
         * Construct a reader for the given WebGraph database.  The
         * basePath should not include any extension (.graph.gz, etc
         * will be added automatically as appropriate).  The path can
         * either point to a regular BVGraph, or an ImmutableGraph
         * that in turn points to a BVGraph.  In either case, this
         * reads the gzip'ed files directly, so there's no need to
         * decompress anything.
         *
         * Throws IOError if any graph-related file cannot be opened
         * and FormatError if any graph metadata is malformed.
         */
        Reader(const std::string &basePath);
        ~Reader();

        /**
         * Return a new URLReader for the URL's database associated
         * with this WebGraph.
         *
         * Throws IOError if the URL's database cannot be opened.
         */
        URLReader *newURLReader();
        /**
         * Read the next node.  Return NULL if there are no more
         * nodes.  Nodes are returned in sequential order, starting
         * with node 0.  The returned Node may be modified by later
         * calls to readNode.
         *
         * This doesn't explicitly validate the file format.  It'll
         * probably just crash on malformed data.
         */
        const Node *readNode();
};

/**
 * A node in a WebGraph database.  Each node has an ID and a list of
 * node ID's to which it links.
 */
class Node
{
public:
        int node;
        std::vector<int> links;
        typedef std::vector<int>::iterator iterator;

        void dump() const;
};

/**
 * Read a sequence of URL's from a WebGraph in increasing node order.
 * Because of how nodes are ordered, these are also read in
 * lexicographic order (though different collating orders may apply to
 * different data sets).
 */
class URLReader
{
private:
        class GZByteReader *r;

        // It turns out gzgets is 10-20X slower than gzread, so we
        // implement our own block buffering and line parsing.  The
        // result is that this is nearly as fast as zcat'ing the file.
        char buf[16*1024];
        unsigned int bufLen, bufPos;

        bool fill();

public:
        /**
         * Construct a URLReader that reads from the given URL
         * database.  This database can be compressed or
         * decompressed.
         *
         * Throws IOError if the file cannot be opened.
         */
        URLReader(const std::string &path);
        ~URLReader();

        /**
         * Read the URL of the next node into the given string.
         * Returns true if the URL was successfully read, or false if
         * there are no more URL's to read.  Taking a string from the
         * caller allows the caller to reuse the same string object
         * and thus avoid repeated memory allocation.
         */
        bool readURL(std::string *out);
};

}

#endif  // _WEBGRAPH_H
