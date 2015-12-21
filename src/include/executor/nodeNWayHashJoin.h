/*-------------------------------------------------------------------------
 *
 * nodeNWayHashJoin.h
 *	  prototypes for nodeNWayHashJoin.c
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeNWayHashJoin.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODENWAYHASHJOIN_H
#define NODENWAYHASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"

typedef struct NWayHashTable
{
        int                     nbuckets;       /* # buckets in the in-memory hash table */
        int                     log2_nbuckets;  /* its log2 (nbuckets must be a power of 2) */

        /* buckets[i] is head of list of tuples in i'th in-memory bucket */
        struct HashJoinTupleData **buckets;
        /* buckets array is per-batch storage, as are all the tuples */

        int                     nbatch;         /* number of batches */

        double          totalTuples;    /* # tuples obtained from inner plan */

        /* These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.  A file is opened only when we first write a tuple into it
         * (otherwise its pointer remains NULL).  Note that the zero'th array
         * elements never get used, since we will process rather than dump out any
 	 * tuples of batch zero.
 	 */
        BufFile   **batchFile; /* buffered virtual temp file per batch */

        /*
 	 * Info about the datatype-specific hash functions for the datatypes being
 	 * hashed. These are arrays of the same length as the number of hash join
 	 * clauses (hash keys).
 	 */
        FmgrInfo   *hashfunctions;        /* lookup data for hash functions */

        MemoryContext hashCxt;          /* context for whole-hash-join storage */
        MemoryContext batchCxt;         /* context for this-batch-only storage */
} NWayHashTable;

extern TupleTableSlot * ExecNWayHashJoin(HashJoinState *node);

#endif
