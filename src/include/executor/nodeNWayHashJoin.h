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

//#define INDIRECT_HASH_TABLE_SIZE 1048576
//#define INDIRECT_HASH_TABLE_SIZE 65536
//#define INDIRECT_HASH_TABLE_SIZE 2097152
#define INDIRECT_HASH_TABLE_SIZE 4194304
#define BIT_WIDTH 32
#define IHT_BUCKET_PARTS 1
#define PROBE_BATCH_SIZE 5000

typedef struct IndirectHashTable
{
	long *cols;
	long **bucketids;
}IndirectHashTable;

//typedef struct IndirectHashTableCell
//{
	//bool isPresent;
	//List *bucketID;
//}IndirectHashTableCell;

typedef struct NWayHashTable
{
        int                     nbuckets;       /* # buckets in the in-memory hash table */
        int                     log2_nbuckets;  /* its log2 (nbuckets must be a power of 2) */

        /* buckets[i] is head of list of tuples in i'th in-memory bucket */
        struct HashJoinTupleData **buckets;
        /* buckets array is per-batch storage, as are all the tuples */

        int                     nbatch;         /* number of batches */

        double          totalTuples;    /* # tuples obtained from inner plan */
        int			      curbatch;		/* current batch #; 0 during 1st pass */

        /* These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.  A file is opened only when we first write a tuple into it
         * (otherwise its pointer remains NULL).  Note that the zero'th array
         * elements never get used, since we will process rather than dump out any
 	 * tuples of batch zero.
 	 */
        BufFile   **batchFile; /* buffered virtual temp file per batch */

        IndirectHashTable *indirectHashTable; // nbatches
	int ihtWidthCols;
	int ihtWidthBucketIDs;

        /*
 	 * Info about the datatype-specific hash functions for the datatypes being
 	 * hashed. These are arrays of the same length as the number of hash join
 	 * clauses (hash keys).
 	 */
        FmgrInfo   hashfunction;        /* lookup data for hash functions */

        MemoryContext hashCxt;          /* context for whole-hash-join storage */
        MemoryContext batchCxt;         /* context for this-batch-only storage */
} NWayHashTable;

extern TupleTableSlot * ExecNWayHashJoin(HashJoinState *node);

#endif
