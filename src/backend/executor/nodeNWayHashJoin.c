/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeNWayHashJoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/matview.h"
#include "commands/trigger.h"
#include "executor/execdebug.h"
#include "foreign/fdwapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

void NWayChooseHashTableSize(PlanState *node, int *nbuckets, int *nbatch);
NWayHashTable* NWayCreateHashTable(int nbuckets, int nbatch);
void NWaySetHashFunction(NWayHashTable **hashtable, List *hashOperators, bool isRight);
void NWayBuildHashTable1(NWayHashTable **hashtable, PlanState  *node,
												ExprContext *econtext, List *hashkeys);
void NWayBuildHashTable2(NWayHashTable **hashtable, PlanState  *node,  FmgrInfo   hashfunction, int attr1, int attr2);
void NWayBuildHashTable3(NWayHashTable **hashtable, PlanState *node, int **indirectHashTable, FmgrInfo hashfunction, int attrNo);
bool NWayGetHashValue(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, uint32 *hashvalue);
void NWayGetHashValueSimple(Datum keyval, FmgrInfo   hashfunction, uint32 *hashvalue);
void NWayGetBucketAndBatch(NWayHashTable *hashtable, uint32 hashvalue, int *bucketno, int *batchno);
void NWayHashTableInsertBucket(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue, int bucketno);
void NWayHashTableInsert(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue);
int    NWayHashTableInsert2(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue);
void NWayHashTableInsert3(NWayHashTable **hashtable, TupleTableSlot *slot, Datum keyVal, int **indirectHashTable, FmgrInfo hashfunction);
void NWayPrintBuckets1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot);
void NWayPrintBuckets2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2);
void NWayPrintBuckets3(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo);
bool NWayFindKeys(List *keys, PlanState *node, int *attrNo);
void NWayPrintIndirectHashTable(int **indirectHashTable, int numRows, int numCols);
bool NWayLoadNextBatchFile(NWayHashTable **hashtable, TupleTableSlot *tupSlot);
void NWayBatchReset(NWayHashTable **hashtable);
TupleTableSlot *NWayGetSavedTuple(BufFile *file, uint32 *hashvalue, TupleTableSlot *tupleSlot);

TupleTableSlot * ExecNWayHashJoin(HashJoinState *node) /* return: a tuple or NULL */
{
	PlanState  *outerNode, *outerNode1, *outerNode2, *outerNode3;
	HashState  *hashNode, *hashNode2;
	HashJoinState *node2;
    int nbuckets, nbatch, nbuckets2, nbatch2;
    NWayHashTable *hashTable1, *hashTable2, *hashTable3;
    int table2attr1, table2attr2, table3attr;
    int i;

	printf("ExecNWayHashJoin node.type: %d\n", node->js.ps.type);

	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);

	outerNode1 = outerPlanState(hashNode);

	printf("hashNode: %d - outerNode.type: %d - outerNode1.type: %d\n",hashNode->ps.type, outerNode->type, outerNode1->type);

	node2 = (HashJoinState*)outerNode;

	hashNode2 = (HashState*) innerPlanState(node2);
	outerNode3 = outerPlanState(node2);
	outerNode2 = outerPlanState(hashNode2);
	printf("node2.type: %d - hashNode2.type: %d - outerNode3.type: %d - outerNode2.type: %d\n", node2->js.ps.type, hashNode2->ps.type, outerNode3->type, outerNode2->type);
	if (outerNode1->type == T_SeqScanState)
	{
		SeqScanState *seqScan = (SeqScanState*)outerNode1;
		printf("outerNode1.RelName: %s\n", RelationGetRelationName(seqScan->ss_currentRelation));
	}
	if (outerNode3->type == T_SeqScanState)
	{
		SeqScanState *seqScan = (SeqScanState*)outerNode3;
		printf("outerNode3.RelName: %s\n", RelationGetRelationName(seqScan->ss_currentRelation));
	}
	if (outerNode2->type == T_SeqScanState)
	{
		SeqScanState *seqScan = (SeqScanState*)outerNode2;
		printf("outerNode2.RelName: %s\n", RelationGetRelationName(seqScan->ss_currentRelation));
	}

	NWayChooseHashTableSize(outerNode1, &nbuckets, &nbatch);
	printf("1 - nbuckets: %d - nbatch: %d\n",nbuckets, nbatch);

	NWayChooseHashTableSize(outerNode2, &nbuckets2, &nbatch2);
	if (nbuckets < nbuckets2)
		nbuckets = nbuckets2;
	if (nbatch < nbatch2)
	    nbatch = nbatch2;

	 printf("2 - nbuckets2: %d - nbatch2: %d - nbuckets: %d - nbatch: %d\n",nbuckets2, nbatch2, nbuckets, nbatch);

	 NWayChooseHashTableSize(outerNode3, &nbuckets2, &nbatch2);
	 if (nbuckets < nbuckets2)
		 nbuckets = nbuckets2;
	 if (nbatch < nbatch2)
		 nbatch = nbatch2;

	 printf("3 - nbuckets2: %d - nbatch2: %d - nbuckets: %d - nbatch: %d\n",nbuckets2, nbatch2, nbuckets, nbatch);

	 printf("Scanning outerTable1");
	 hashTable1 = NWayCreateHashTable(nbuckets, nbatch);
	 NWaySetHashFunction(&hashTable1, node->hj_HashOperators, true);
	 NWayBuildHashTable1(&hashTable1, outerNode1, hashNode->ps.ps_ExprContext, hashNode->hashkeys);
	 printf("PrintBuckets1\n");
	 NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);

	 if (!NWayFindKeys(node->hj_OuterHashKeys, outerNode3, &table2attr1) )
	 {
		 printf("Unable to find Table2Attr1\n");
		 exit(0);
	 }
	 if (!NWayFindKeys(node2->hj_OuterHashKeys, outerNode3, &table2attr2))
	 {
		 printf("Unable to find Table2Attr2\n");
		 exit(0);
	 }
	 if (!NWayFindKeys(node2->hj_InnerHashKeys, outerNode2, &table3attr))
	 	 {
	 		 printf("Unable to find Table3Attr\n");
	 		 exit(0);
	 	 }
	 printf("Table2Attr1: %d - Table2Attr2: %d - Table3Attr: %d\n", table2attr1, table2attr2, table3attr);

	 hashTable2 = NWayCreateHashTable(nbuckets, nbatch);
	 NWaySetHashFunction(&hashTable2, node2->hj_HashOperators, false);
	 NWayBuildHashTable2(&hashTable2, outerNode3, hashTable1->hashfunction, table2attr1, table2attr2);
	 printf("PrintBuckets2\n");
	 NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);

	 hashTable3 = NWayCreateHashTable(nbuckets, nbatch);
	 NWayBuildHashTable3(&hashTable3, outerNode2, hashTable2->indirectHashTable, hashTable2->hashfunction, table3attr);
	 printf("PrintBuckets3\n");
	 NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);

	 for(i=1; i<nbatch; i++)
	 {
		 printf("Table 1 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFile(&hashTable1, outerNode1->ps_ResultTupleSlot);
		 printf("Table 1 - PrintBatch%d\n",i);
		 NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);
		 printf("Table 2 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFile(&hashTable2, outerNode3->ps_ResultTupleSlot);
		 printf("Table 2 - PrintBatch%d\n",i);
		 NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);
		 printf("Table 3 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFile(&hashTable3, outerNode2->ps_ResultTupleSlot);
		 printf("Table 3 - PrintBatch%d\n",i);
		 NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
	 }

#if 0
	 printf("Table 1 - LoadBatch1\n");
	 NWayLoadNextBatchFile(&hashTable1, outerNode1->ps_ResultTupleSlot);
	 printf("Table 1 - PrintBatch1\n");
	 NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);
	 printf("Table 1 - LoadBatch2\n");
	 NWayLoadNextBatchFile(&hashTable1, outerNode1->ps_ResultTupleSlot);
	 printf("Table 1 - PrintBatch2\n");
	 NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);

	 printf("Table 2 - LoadBatch1\n");
	 NWayLoadNextBatchFile(&hashTable2, outerNode3->ps_ResultTupleSlot);
	 printf("Table 2 - PrintBatch1\n");
	 NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);
	 printf("Table 2 - LoadBatch2\n");
	 NWayLoadNextBatchFile(&hashTable2, outerNode3->ps_ResultTupleSlot);
	 printf("Table 2 - PrintBatch2\n");
	 NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);

	 printf("Table 3 - LoadBatch1\n");
	 NWayLoadNextBatchFile(&hashTable3, outerNode2->ps_ResultTupleSlot);
	 printf("Table 3 - PrintBatch1\n");
	 NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
	 printf("Table 3 - LoadBatch2\n");
	 NWayLoadNextBatchFile(&hashTable3, outerNode2->ps_ResultTupleSlot);
	 printf("Table 3 - PrintBatch2\n");
	 NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
#endif
#if 0
	 if (1)
	 {
		 ListCell *lc;
		 printf("node->InnerHashKeys\n");
		 foreach (lc, node->hj_InnerHashKeys)
		 {
			 ExprState  *keyexpr = (ExprState *) lfirst(lc);
			 printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			 if(keyexpr->expr->type == T_Var)
			 {
				 Var *varExpr = (Var*)keyexpr->expr;
				 printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						 	 varExpr->varnoold, varExpr->varoattno);
			 }
		 }
	 }
	 if (1)
	 {
		ListCell *lc;
		printf("node->OuterHashKeys\n");
		foreach (lc, node->hj_OuterHashKeys)
		{
			ExprState  *keyexpr = (ExprState *) lfirst(lc);
			printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			if(keyexpr->expr->type == T_Var)
			{
				Var *varExpr = (Var*)keyexpr->expr;
				printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						varExpr->varnoold, varExpr->varoattno);
			}
		}
	 }
	 if (1)
	 {
		 ListCell *lc;
		 printf("node2->InnerHashKeys\n");
		 foreach (lc, node2->hj_InnerHashKeys)
		 {
			 ExprState  *keyexpr = (ExprState *) lfirst(lc);
			 printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			 if(keyexpr->expr->type == T_Var)
			 {
				 Var *varExpr = (Var*)keyexpr->expr;
				 printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						 varExpr->varnoold, varExpr->varoattno);
			 }
		 }
	 }
	 if (1)
	 {
		 ListCell *lc;
		 printf("node2->OuterHashKeys\n");
		 foreach (lc, node2->hj_OuterHashKeys)
		 {
			 ExprState  *keyexpr = (ExprState *) lfirst(lc);
			 printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			 if(keyexpr->expr->type == T_Var)
			 {
				 Var *varExpr = (Var*)keyexpr->expr;
				 printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						 varExpr->varnoold, varExpr->varoattno);
			 }
		 }
	 }
	 if (1)
	 {
		 ListCell *lc;
		 printf("hashNode->hashKeys\n");
		 foreach (lc, hashNode->hashkeys)
		 {
			 ExprState  *keyexpr = (ExprState *) lfirst(lc);
			 printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			 if(keyexpr->expr->type == T_Var)
			 {
				 Var *varExpr = (Var*)keyexpr->expr;
				 printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						 varExpr->varnoold, varExpr->varoattno);
			 }
		 }
	 }
	 if (1)
	 {
		 ListCell *lc;
		 printf("hashNode2->hashKeys\n");
		 foreach (lc, hashNode2->hashkeys)
		 {
			 ExprState  *keyexpr = (ExprState *) lfirst(lc);
			 printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
			 if(keyexpr->expr->type == T_Var)
			 {
				 Var *varExpr = (Var*)keyexpr->expr;
				 printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
						 varExpr->varnoold, varExpr->varoattno);
			 }
		 }
	 }

	 if(1)
	 {
		 ProjectionInfo *projInfo = outerNode->ps_ProjInfo;
		 printf("OuterNode\n");
		 if (projInfo == NULL)
			 printf("\tProjInfo is NULL\n");
		 else
		 {
			 int *varSlotOffsets = projInfo->pi_varSlotOffsets;
			 int *varNumbers = projInfo->pi_varNumbers;
			 ExprContext *econtext = projInfo->pi_exprContext;
			 int i;
			 printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
			 Assert(projInfo->pi_directMap == true);
			 if(projInfo->pi_directMap)
				 printf("\tprojInfo->directMap is TRUE\n");
			 else
				 printf("\tprojInfo->directMap is FALSE\n");
			 /* especially simple case where vars go to output in order */
			 for (i = 0; i < projInfo->pi_numSimpleVars; i++)
			 {
				 char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
				 TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
				 int			varNumber = varNumbers[i] - 1;

				 printf("\t\ti: %d - varNumber: %d\n", i, varNumber);
			 }
		 }
	 }

	 if(1)
	 {
		 ProjectionInfo *projInfo = outerNode1->ps_ProjInfo;
		 printf("OuterNode1\n");
		 if (projInfo == NULL)
			 printf("\tProjInfo is NULL\n");
		 else
		 {
			 int *varSlotOffsets = projInfo->pi_varSlotOffsets;
			 int *varNumbers = projInfo->pi_varNumbers;
			 ExprContext *econtext = projInfo->pi_exprContext;
			 int i;
			 printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
			 Assert(projInfo->pi_directMap == true);
			 if(projInfo->pi_directMap)
				 printf("\tprojInfo->directMap is TRUE\n");
			 else
				 printf("\tprojInfo->directMap is FALSE\n");
			 /* especially simple case where vars go to output in order */
			 for (i = 0; i < projInfo->pi_numSimpleVars; i++)
			 {
				 char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
				 TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
				 int			varNumber = varNumbers[i] - 1;

				 printf("\t\ti: %d - varNumber: %d\n", i, varNumber);
			 }
		 }
	 }
	 if(1)
	 {
		 ProjectionInfo *projInfo = outerNode2->ps_ProjInfo;
		 printf("OuterNode2\n");
		 if (projInfo == NULL)
			 printf("\tProjInfo is NULL\n");
		 else
		 {
			 int *varSlotOffsets = projInfo->pi_varSlotOffsets;
			 int *varNumbers = projInfo->pi_varNumbers;
			 ExprContext *econtext = projInfo->pi_exprContext;
			 int i;
			 printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
			 Assert(projInfo->pi_directMap == true);
			 if(projInfo->pi_directMap)
				 printf("\tprojInfo->directMap is TRUE\n");
			 else
				 printf("\tprojInfo->directMap is FALSE\n");
			 /* especially simple case where vars go to output in order */
			 for (i = 0; i < projInfo->pi_numSimpleVars; i++)
			 {
				 char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
				 TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
				 int			varNumber = varNumbers[i] - 1;

				 printf("\t\ti: %d - varNumber: %d\n", i, varNumber);
			 }
		 }
	 }
	 if(1)
	 {
		 ProjectionInfo *projInfo = outerNode3->ps_ProjInfo;
		 printf("OuterNode3\n");
		 if (projInfo == NULL)
			 printf("\tProjInfo is NULL\n");
		 else
		 {
			 int *varSlotOffsets = projInfo->pi_varSlotOffsets;
			 int *varNumbers = projInfo->pi_varNumbers;
			 ExprContext *econtext = projInfo->pi_exprContext;
			 int i;
			 printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
			 Assert(projInfo->pi_directMap == true);
			 if(projInfo->pi_directMap)
				 printf("\tprojInfo->directMap is TRUE\n");
			 else
				 printf("\tprojInfo->directMap is FALSE\n");
			 /* especially simple case where vars go to output in order */
			 for (i = 0; i < projInfo->pi_numSimpleVars; i++)
			 {
				 char	   *slotptr = ((char *) econtext) + varSlotOffsets[i];
				 TupleTableSlot *varSlot = *((TupleTableSlot **) slotptr);
				 int			varNumber = varNumbers[i] - 1;

				 printf("\t\ti: %d - varNumber: %d\n", i, varNumber);
			 }
		 }
	 }
#endif
	/*
	//printf("node.type: %d - node2.type: %d\n", node->type, node2->type);
	printf("outerNode.type: %d - outerNode1.type: %d - outerNode2.type: %d - outerNode3.type: %d\n",outerNode->type, outerNode1->type, outerNode2->type, outerNode3->type);
*/
	exit(0);
}

void NWayChooseHashTableSize(PlanState *node, int *nbuckets, int *nbatch)
{
        int num_skew_mcvs;

        ExecChooseHashTableSize(node->plan->plan_rows, node->plan->plan_width, false, nbuckets, nbatch, &num_skew_mcvs);
}

NWayHashTable* NWayCreateHashTable(int nbuckets, int nbatch)
{
        NWayHashTable *hashtable;
        MemoryContext oldcxt;
        int log2_nbuckets;

        /* nbuckets must be a power of 2 */
        log2_nbuckets = my_log2(nbuckets);
        Assert(nbuckets == (1 << log2_nbuckets));

        hashtable = (NWayHashTable*) palloc(sizeof(NWayHashTable));
        hashtable->nbuckets = nbuckets;
        hashtable->log2_nbuckets = log2_nbuckets;
        hashtable->curbatch = 0;
        hashtable->buckets = NULL;
        hashtable->nbatch = nbatch;
        hashtable->totalTuples = 0;
        hashtable->batchFile = NULL;

        /*
         * Create temporary memory contexts in which to keep the hashtable working
         * storage.  See notes in executor/hashjoin.h.
         */
        hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext, "HashTableContext",
                                                   ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                   ALLOCSET_DEFAULT_MAXSIZE);

        hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt, "HashBatchContext",
                                                    ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                    ALLOCSET_DEFAULT_MAXSIZE);

        /* Allocate data that will live for the life of the hashjoin */

        oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

        if (nbatch > 1)
        {
                /*
                 * allocate and initialize the file arrays in hashCxt
                 */
                hashtable->batchFile = (BufFile **) palloc0(nbatch * sizeof(BufFile *));
                /* The files will not be opened until needed... */
                /* ... but make sure we have temp tablespaces established for them */
                PrepareTempTablespaces();
        }

        /*
         * Prepare context for the first-scan space allocations; allocate the
         * hashbucket array therein, and set each bucket "empty".
         */
        MemoryContextSwitchTo(hashtable->batchCxt);

        hashtable->buckets = (HashJoinTuple *) palloc0(nbuckets * sizeof(HashJoinTuple));

        MemoryContextSwitchTo(oldcxt);

        return hashtable;
}

void NWaySetHashFunction(NWayHashTable **hashtable, List *hashOperators, bool isRight)
{
	ListCell   *ho;
	int nkeys = list_length(hashOperators);

	Assert(nkeys == 1); // For now, we deal with join predicates with single attribute
	foreach(ho, hashOperators)
	{
			Oid			hashop = lfirst_oid(ho);
			Oid			left_hashfn;
			Oid			right_hashfn;

			if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
				elog(ERROR, "could not find hash function for hash operator %u",
					 hashop);

			if (isRight)
				fmgr_info(right_hashfn, &(*hashtable)->hashfunction);
			else
				fmgr_info(left_hashfn, &(*hashtable)->hashfunction);
	}
}

void NWayBuildHashTable1(NWayHashTable **hashtable, PlanState  *node,
												ExprContext *econtext, List *hashkeys)
{
	TupleTableSlot *slot;
	uint32		hashvalue;

	for (;;)
	{
		slot = ExecProcNode(node);

		if (TupIsNull(slot))
			break;

		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;

		if( NWayGetHashValue(econtext, hashkeys, (*hashtable)->hashfunction, &hashvalue))
		{
			NWayHashTableInsert(hashtable, slot, hashvalue);
			(*hashtable)->totalTuples += 1;
		}
	}
}

void NWayGetHashValueSimple(Datum keyval, FmgrInfo   hashfunction, uint32 *hashvalue)
{
	uint32		hashkey = 0;
	uint32		hkey;

	/* rotate hashkey left 1 bit at each step */
	hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
	/* Compute the hash function */


	hkey = DatumGetUInt32(FunctionCall1(&hashfunction, keyval));
	hashkey ^= hkey;
	*hashvalue = hashkey;
}

bool NWayGetHashValue(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, uint32 *hashvalue)
{
	uint32		hashkey = 0;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		printf("KeyVal: %lu - ", (unsigned long) keyval);
		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.  However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (!isNull)
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunction, keyval));
			hashkey ^= hkey;
		}
		else
			return false;

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	printf("HashValue: %lu\n", (unsigned long) hashkey);
	*hashvalue = hashkey;
	return true;
}

void NWayGetBucketAndBatch(NWayHashTable *hashtable, uint32 hashvalue,
						  	  	  	  	  	  	  	 int *bucketno, int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		/* we can do MOD by masking, DIV by shifting */
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = 0;
	}
}

void NWayHashTableInsertBucket(NWayHashTable **hashtable, TupleTableSlot *slot,
														  uint32 hashvalue, int bucketno)
{
	/*
	 * put the tuple in hash table
	 */
	HashJoinTuple hashTuple;
	int			hashTupleSize;
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);

	/* Create the HashJoinTuple */
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	hashTuple = (HashJoinTuple) MemoryContextAlloc((*hashtable)->batchCxt,
												   hashTupleSize);
	hashTuple->hashvalue = hashvalue;
	memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);

	/*
	 * We always reset the tuple-matched flag on insertion.  This is okay
	 * even when reloading a tuple from a batch file, since the tuple
	 * could not possibly have been matched to an outer tuple before it
	 * went into the batch file.
	 */
	HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

	/* Push it onto the front of the bucket's list */
	hashTuple->next = (*hashtable)->buckets[bucketno];
	(*hashtable)->buckets[bucketno] = hashTuple;
}

void NWayHashTableInsert(NWayHashTable **hashtable, TupleTableSlot *slot,
									      	  uint32 hashvalue)
{
	int bucketno, batchno;
	NWayGetBucketAndBatch(*hashtable, hashvalue, &bucketno, &batchno);
	printf("HashValue: %lu - bucketno: %d - batchno: %d - ", (unsigned long) hashvalue,  bucketno, batchno);
	//printf("--> bucketno: %d - batchno: %d\n", bucketno, batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == (*hashtable)->curbatch)
	{
		printf("--> Insertion into Bucket\n");
		NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
	}
	else
	{
		MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
		printf("#### Insertion into Batch\n");
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > (*hashtable)->curbatch);
		ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[batchno]);
	}
}

int NWayHashTableInsert2(NWayHashTable **hashtable, TupleTableSlot *slot,
									      	  uint32 hashvalue)
{
	int bucketno, batchno;
	NWayGetBucketAndBatch(*hashtable, hashvalue, &bucketno, &batchno);
	printf("--> bucketno: %d - batchno: %d\n", bucketno, batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == (*hashtable)->curbatch)
		NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
	else
	{
		MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > (*hashtable)->curbatch);
		ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[batchno]);
	}

	return batchno;
}

void NWayPrintBuckets1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot)
{
	int i;

	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");

	for(i=0; i<hashtable->nbuckets; i++)
	{
		TupleTableSlot *inntuple;
		uint32 hashvalue;
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */
			econtext->ecxt_innertuple = inntuple;
			printf("Buckets[%d] - \n", i);
			if( NWayGetHashValue(econtext, hashkeys, hashtable->hashfunction, &hashvalue))
			{
			}
			hashTuple = hashTuple->next;
		}
	}
}

void NWayPrintBuckets2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2)
{
	int i;

	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");

	for(i=0; i<hashtable->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal1, keyVal2;
			bool isNull;
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */

			keyVal1 = slot_getattr(inntuple, attrNo1+1, &isNull);
			keyVal2 = slot_getattr(inntuple, attrNo2+1, &isNull);
			printf("Buckets[%d] - KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", i, (unsigned long) keyVal1, (unsigned long) hashTuple->hashvalue, (unsigned long) keyVal2);

			hashTuple = hashTuple->next;
		}
	}
}

bool NWayFindKeys(List *keys, PlanState *node, int *attrNo)
{
	ListCell *lc;
	int nkeys = list_length(keys);
	int actualAttr=-1;
	ProjectionInfo *projInfo = node->ps_ProjInfo;
	int *varSlotOffsets;
	int *varNumbers;
	ExprContext *econtext;
	int i;

	if ( (nkeys == -1) || (projInfo == NULL))
		return false;

	varSlotOffsets = projInfo->pi_varSlotOffsets;
	varNumbers = projInfo->pi_varNumbers;
	econtext = projInfo->pi_exprContext;

	foreach (lc, keys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(lc);
		//printf("\tKeyExpr->type: %d = KeyExpr->expr->type: %d\n", keyexpr->type, keyexpr->expr->type);
		if(keyexpr->expr->type == T_Var)
		{
			Var *varExpr = (Var*)keyexpr->expr;
			actualAttr = varExpr->varoattno-1;
			//printf("\t\tvarExpr->varno: %d - varExpr->varattno: %d - varExpr->varnoold: %d - varExpr->varoattno: %d\n",varExpr->varno,varExpr->varattno,
			//		varExpr->varnoold, varExpr->varoattno);
		}
	}
	printf("ActualAttr: %d\n", actualAttr);
	if(actualAttr == -1) // make sure we found the join key
		return false;

	//printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
	Assert(projInfo->pi_directMap == true);

	/* especially simple case where vars go to output in order */
	for (i = 0; i < projInfo->pi_numSimpleVars; i++)
	{
		int			varNumber = varNumbers[i] - 1;

		printf("varNumber: %d \n", varNumber);
		if (varNumber == actualAttr)
		{
			(*attrNo) = i;
			return true;
		}
		//printf("\t\ti: %d - varNumber: %d\n", i, varNumber);
	}

	return false;
}

void NWayPrintIndirectHashTable(int **indirectHashTable, int numRows, int numCols)
{
	int i,j;
	int min=numCols, min2 = numCols, max=-1;
	for(i=0; i<numRows; i++)
	{
		int count=0;
		printf("Indirect[%d]: ",i);
		for(j=0; j<numCols; j++)
		{
			if (indirectHashTable[i][j] == 1)
			{
				printf("%d,", j);
				count++;
			}
		}
		if (min > count)
			min = count;
		if (count > 0 && min2 > count)
			min2 = count;
		if (max < count)
			max = count;
		printf(" - [%d]\n", count);
	}
	printf("HERE - HERE - Min: %d - Min2: %d - Max: %d\n",min, min2, max);
}

void NWayBuildHashTable2(NWayHashTable **hashtable, PlanState  *node, FmgrInfo   hashfunction,
												int attr1, int attr2)
{
	TupleTableSlot *slot;
	int i;

	MemoryContextSwitchTo((*hashtable)->hashCxt);

	(*hashtable)->indirectHashTable = (int**)palloc(sizeof(int*)*INDIRECT_HASH_TABLE_SIZE);
	for(i=0; i<INDIRECT_HASH_TABLE_SIZE; i++)
		(*hashtable)->indirectHashTable[i] = (int*)palloc0(sizeof(int)*(*hashtable)->nbatch);

	for (;;)
	{
		Datum keyVal1, keyVal2;
		uint32	hashvalue1, hashvalue2;
		int batchno;
		slot = ExecProcNode(node);

		if (TupIsNull(slot))
			break;

		keyVal1 = slot->tts_values[attr1];
		keyVal2 = slot->tts_values[attr2];

		NWayGetHashValueSimple(keyVal1, hashfunction, &hashvalue1);
		printf("KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", (unsigned long) keyVal1, (unsigned long) hashvalue1, (unsigned long) keyVal2);
		batchno = NWayHashTableInsert2(hashtable, slot, hashvalue1);

		//NWayGetHashValueSimple(keyVal2, hashfunction, &hashvalue2);
		NWayGetHashValueSimple(keyVal2, (*hashtable)->hashfunction, &hashvalue2);
		(*hashtable)->indirectHashTable[hashvalue2%INDIRECT_HASH_TABLE_SIZE][batchno] = 1;

#if 0
		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;

		if( NWayGetHashValue(econtext, hashkeys, (*hashtable)->hashfunction, &hashvalue))
		{
			NWayHashTableInsert(hashtable, slot, hashvalue);
			(*hashtable)->totalTuples += 1;
		}
#endif
	}

	NWayPrintIndirectHashTable((*hashtable)->indirectHashTable, INDIRECT_HASH_TABLE_SIZE, (*hashtable)->nbatch);
}

void NWayBuildHashTable3(NWayHashTable **hashtable, PlanState *node, int **indirectHashTable, FmgrInfo hashfunction, int attrNo)
{
	TupleTableSlot *slot;
	for (;;)
	{
		Datum keyVal;
		slot = ExecProcNode(node);

		if (TupIsNull(slot))
			break;

		keyVal = slot->tts_values[attrNo];
		NWayHashTableInsert3(hashtable, slot, keyVal, indirectHashTable, hashfunction);
	}

}

void NWayHashTableInsert3(NWayHashTable **hashtable, TupleTableSlot *slot, Datum keyVal, int **indirectHashTable, FmgrInfo hashfunction)
{

	uint32	hashvalue;
	int idxNum, i;

	NWayGetHashValueSimple(keyVal, hashfunction, &hashvalue);
	idxNum = hashvalue%INDIRECT_HASH_TABLE_SIZE;

	printf("KeyVal3: %lu - HashValue3: %lu - idxNum: %d\n", (unsigned long) keyVal, (unsigned long) hashvalue, idxNum);
	for(i=0; i<(*hashtable)->nbatch; i++)
	{
		if (indirectHashTable[idxNum][i] == 1) // batch number 'i' is valid
		{
			printf("\tbatchNum: %d - ", i);
			if (i==0)
			{
				int bucketno, batchno2;
				NWayGetBucketAndBatch(*hashtable, hashvalue, &bucketno, &batchno2);
				NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
				printf("BucketNo: %d\n", bucketno);
			}
			else
			{
				MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
				/*
				 * put the tuple into a temp file for later batches
				 */
				Assert(i > (*hashtable)->curbatch);
				ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[i]);
				printf("\n");
			}
		}
	}
}

void NWayPrintBuckets3(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo)
{
	int i;

	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");

	for(i=0; i<hashtable->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal;
			bool isNull;
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */

			keyVal = slot_getattr(inntuple, attrNo+1, &isNull);
			printf("Buckets[%d] - KeyVal: %lu - HashValue: %lu\n", i, (unsigned long) keyVal, (unsigned long) hashTuple->hashvalue);

			hashTuple = hashTuple->next;
		}
	}
}

bool NWayLoadNextBatchFile(NWayHashTable **hashtable, TupleTableSlot *tupSlot)
{
	int curbatch = (*hashtable)->curbatch;
	int nbatch = (*hashtable)->nbatch;
	BufFile    *bFile;

	if (curbatch > 0) // close the current file if it is not the first one
	{
		if ((*hashtable)->batchFile[curbatch])
			(*hashtable)->batchFile[curbatch] = NULL;
	}

	curbatch++;
	while((curbatch < nbatch) && ((*hashtable)->batchFile[curbatch] == NULL))
		curbatch++;

	if (curbatch >= nbatch) /* no more batches */
		return false;

	(*hashtable)->curbatch = curbatch;
	NWayBatchReset(hashtable);

	bFile = (*hashtable)->batchFile[curbatch];
	if (bFile != NULL)
	{
		TupleTableSlot *slot;
		uint32		hashvalue;
		if (BufFileSeek(bFile, 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));

		while ((slot = NWayGetSavedTuple(bFile, &hashvalue, tupSlot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
				NWayHashTableInsert(hashtable, slot, hashvalue);
		}

		/*
		 * after we build the hash table, the inner batch file is no longer
		 * needed
		 */
		BufFileClose(bFile);
		(*hashtable)->batchFile[curbatch] = NULL;
	}

	return true;
}

void NWayBatchReset(NWayHashTable **hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = (*hashtable)->nbuckets;

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset((*hashtable)->batchCxt);
	oldcxt = MemoryContextSwitchTo((*hashtable)->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	(*hashtable)->buckets = (HashJoinTuple *)	palloc0(nbuckets * sizeof(HashJoinTuple));

	MemoryContextSwitchTo(oldcxt);
}

TupleTableSlot *NWayGetSavedTuple(BufFile *file, uint32 *hashvalue, TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}
