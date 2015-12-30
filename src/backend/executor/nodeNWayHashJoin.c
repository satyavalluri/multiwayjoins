/*-------------------------------------------------------------------------
 *
 * nodeNWayHashJoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNWayHashJoin.c
 *
 *-------------------------------------------------------------------------
 */

#include <sys/time.h>

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
void NWayBuildHashTable3(NWayHashTable **hashtable, PlanState *node, IndirectHashTable *indirectHashTable, FmgrInfo hashfunction, int attrNo);
bool NWayGetHashValue(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, uint32 *hashvalue);
bool NWayGetHashValuePrint(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, Datum *value, uint32 *hashvalue);
void NWayGetHashValueSimple(Datum keyval, FmgrInfo   hashfunction, uint32 *hashvalue);
void NWayGetBucketAndBatch(NWayHashTable *hashtable, uint32 hashvalue, int *bucketno, int *batchno);
void NWayHashTableInsertBucket(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue, int bucketno);
void NWayHashTableInsert(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue);
//int    NWayHashTableInsert2(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue);
void NWayHashTableInsert2(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue, int *bucketno, int *batchno);
void NWayHashTableInsert3(NWayHashTable **hashtable, TupleTableSlot *slot, Datum keyVal, IndirectHashTable *indirectHashTable, FmgrInfo hashfunction);

void NWayPrintBuckets1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot);
void NWayPrintBuckets2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2);
void NWayPrintBuckets3(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo);
bool NWayFindKeys(List *keys, PlanState *node, int *attrNo);
void NWayPrintIndirectHashTable(int **indirectHashTable, int numRows, int numCols);
void NWayPrintIndirectHashTable2(IndirectHashTable *indirectHashTable, int numRows, int nbuckets);
bool NWayLoadNextBatchFile(NWayHashTable **hashtable, TupleTableSlot *tupSlot);
void NWayBatchReset(NWayHashTable **hashtable);
TupleTableSlot *NWayGetSavedTuple(BufFile *file, uint32 *hashvalue, TupleTableSlot *tupleSlot);

bool NWayGetKeyValue(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, Datum *keyval);
bool NWayComputeActualJoinProbeTable2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2, 
				      Datum inKey, Datum *outKey, int *outBucket);
bool NWayComputeActualJoinProbeTable1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot,
		 		      int bucketno, Datum inKey);
int NWayComputeActualJoin(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
		          NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attr2No1, int attr2No2,
			  NWayHashTable *hashtable3, TupleTableSlot *tupSlot3, int attr3);
void UpdateIndirectHashTable(IndirectHashTable **iht, int rowID, uint32 valueForCol);
void GetIHTColumnOffset(uint32 valueForCol, int *colID, int *offset);
int GetIndirectHashTableColValue(IndirectHashTable *iht, int rowID, uint32 valueForCol);
//int GetIndirectHashTableBucketValue(IndirectHashTable *iht, int rowID, int firstColID, int bucketNo);
double NWayComputeActualJoinBatch(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
		          NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attr2No1, int attr2No2,
			  NWayHashTable *hashtable3, TupleTableSlot *tupSlot3, int attr3);
void NWayComputeActualJoinProbeTable2Batch(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2, 
				      	   Datum *inKey, int inArrSize, bool **success, Datum **outKey, int **outBucket, int *outArrSize);
double NWayComputeActualJoinProbeTable2BatchDriver(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
						NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attrNo1, int attrNo2, 
				      	   	Datum *inKey, int inArrSize, bool **success, Datum **outKey, int **outBucket, int *outArrSize);
bool NWayLoadNextBatchFileLastTable(NWayHashTable **hashtable, TupleTableSlot *tupSlot);

TupleTableSlot * ExecNWayHashJoin(HashJoinState *node) /* return: a tuple or NULL */
{
	PlanState  *outerNode, *outerNode1, *outerNode2, *outerNode3;
	HashState  *hashNode, *hashNode2;
	HashJoinState *node2;
    int nbuckets, nbatch, nbuckets2, nbatch2;
    NWayHashTable *hashTable1, *hashTable2, *hashTable3;
    int table2attr1, table2attr2, table3attr;
    int i;
		struct timeval  tvm1, tvm2;
	double totalJoinCount=0;
	double totalht1=0.0, totalht2=0.0, totalht3=0.0;

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

		gettimeofday(&tvm1, NULL);
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

	 //printf("Scanning outerTable1");
	 hashTable1 = NWayCreateHashTable(nbuckets, nbatch);
	 NWaySetHashFunction(&hashTable1, node->hj_HashOperators, true);
	 NWayBuildHashTable1(&hashTable1, outerNode1, hashNode->ps.ps_ExprContext, hashNode->hashkeys);
	 //printf("PrintBuckets1\n");
	 //NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);

	 /* table2attr1 -> l_orderkey
 	  * table2attr2 -> l_partkey
 	  * */
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

	 hashTable3 = NWayCreateHashTable(nbuckets, nbatch);
	 NWayBuildHashTable3(&hashTable3, outerNode2, hashTable2->indirectHashTable, hashTable2->hashfunction, table3attr);

	gettimeofday(&tvm2, NULL);
	printf ("Build time = %f seconds\n", (double) (tvm2.tv_usec - tvm1.tv_usec) / 1000000 + (double) (tvm2.tv_sec - tvm1.tv_sec));

#if 0
	 printf("PrintBuckets2\n");
	 NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);
	 printf("PrintBuckets3\n");
	 NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
	 NWayPrintIndirectHashTable2(hashTable2->indirectHashTable, hashTable2->nbatch, hashTable2->nbuckets);
#endif
#if 0
	if(1)
	{
		List *myList = NULL;
		List *myList2;
		ListCell *lc;
		
		for(i=10; i<40; i=i+5)
		{
			printf("Insertion: %d\n",i);
			myList = lappend_oid(myList, (Oid)i);
		}

		myList2 = myList;
		foreach(lc, myList2)
		{
			Oid val = lfirst_oid(lc);
			int val2 = (int) val;
			printf("Val: %u - val2: %d\n",val, val2);
			//myList = myList->next;
		}
	}
#endif
	if(1)
	{
		struct timeval  tv1, tv2;
		double joincount;
		gettimeofday(&tv1, NULL);
		printf("DEFGHIJ - 0 - ht1->numBucketTuples: %lf - ht2->numBucketTuples: %lf - ht3->numBucketTuples: %lf\n",
			hashTable1->numBucketTuples, hashTable2->numBucketTuples, hashTable3->numBucketTuples);
		//joincount = NWayComputeActualJoin(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot,
			      			      //hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2,
			      			      //hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);

		totalht1 += hashTable1->numBucketTuples;
		totalht2 += hashTable2->numBucketTuples;
		totalht3 += hashTable3->numBucketTuples;
		joincount = NWayComputeActualJoinBatch(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot,
			      			      hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2,
			      			      hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
		gettimeofday(&tv2, NULL);
		printf("0 - Join Count: %lf\n",joincount);
		printf ("0 - Join time = %f seconds\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec));
		totalJoinCount += joincount;
	}

#if 1
	 for(i=1; i<nbatch; i++)
	 //for(i=1; i<2; i++)
	 {
		struct timeval  tv1, tv2;
		double joincount;

		 //printf("Table 1 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFile(&hashTable1, outerNode1->ps_ResultTupleSlot);
		 //printf("Table 1 - PrintBatch%d\n",i);
		 //NWayPrintBuckets1(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot);
		 //printf("Table 2 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFile(&hashTable2, outerNode3->ps_ResultTupleSlot);
		 //printf("Table 2 - PrintBatch%d\n",i);
		 //NWayPrintBuckets2(hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2);
		 //printf("Table 3 - LoadBatch%d\n",i);
		 NWayLoadNextBatchFileLastTable(&hashTable3, outerNode2->ps_ResultTupleSlot);

		totalht1 += hashTable1->numBucketTuples;
		totalht2 += hashTable2->numBucketTuples;
		totalht3 += hashTable3->numBucketTuples;

		printf("DEFGHIJ - %d - ht1->numBucketTuples: %lf - ht2->numBucketTuples: %lf - ht3->numBucketTuples: %lf\n",
			i, hashTable1->numBucketTuples, hashTable2->numBucketTuples, hashTable3->numBucketTuples);
		 //printf("Table 3 - PrintBatch%d\n",i);
		 //NWayPrintBuckets3(hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
		gettimeofday(&tv1, NULL);
		//joincount = NWayComputeActualJoin(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot,
			      			      //hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2,
			      			      //hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);

		joincount = NWayComputeActualJoinBatch(hashTable1, hashNode->ps.ps_ExprContext, hashNode->hashkeys, node->hj_HashTupleSlot,
			      			      hashTable2, outerNode3->ps_ResultTupleSlot, table2attr1, table2attr2,
			      			      hashTable3, outerNode2->ps_ResultTupleSlot, table3attr);
		gettimeofday(&tv2, NULL);
		printf("%d - Join Count: %lf\n",i,joincount);
		printf ("%d - Join time = %f seconds\n", i, (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec));
		totalJoinCount += joincount;
	 }
	 printf("Total Join Count: %lf\n",totalJoinCount);
	 printf("DEFGHIJ - Final - ht1->totalTuples: %lf - ht2->totalTuples: %lf - ht3->totalTuples: %lf\n",
			hashTable1->totalTuples, hashTable2->totalTuples, hashTable3->totalTuples);

	 printf("DEFGHIJ - Final - totalht1: %lf - totalht2: %lf - totalht3: %lf\n",
		totalht1, totalht2, totalht3);
#endif

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
	hashtable->numBucketTuples = 0;

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

		//printf("KeyVal: %lu - ", (unsigned long) keyval);
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

	//printf("HashValue: %lu\n", (unsigned long) hashkey);
	*hashvalue = hashkey;
	return true;
}

bool NWayGetHashValuePrint(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, Datum *value, uint32 *hashvalue)
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

		//printf("KeyVal: %lu - ", (unsigned long) keyval);
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
			(*value) = keyval;
		}
		else
			return false;

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	//printf("HashValue: %lu\n", (unsigned long) hashkey);
	*hashvalue = hashkey;
	return true;
}



bool NWayGetKeyValue(ExprContext *econtext, List *hashkeys, FmgrInfo   hashfunction, Datum *keyval)
{
	ListCell   *hk;
	MemoryContext oldContext;
	bool success = false;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		bool		isNull;

		/*
		 * Get the join attribute value of the tuple
		 */
		(*keyval) = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);
		Assert (isNULL == false);
		success = true;
	}

	MemoryContextSwitchTo(oldContext);

	return success;
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
	//printf("HashValue: %lu - bucketno: %d - batchno: %d - ", (unsigned long) hashvalue,  bucketno, batchno);
	//printf("--> bucketno: %d - batchno: %d\n", bucketno, batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == (*hashtable)->curbatch)
	{
		//printf("--> Insertion into Bucket\n");
		NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
		(*hashtable)->numBucketTuples += 1;
	}
	else
	{
		MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
		//printf("#### Insertion into Batch\n");
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > (*hashtable)->curbatch);
		ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[batchno]);
	}
}

void NWayHashTableInsert2(NWayHashTable **hashtable, TupleTableSlot *slot, uint32 hashvalue,
			  int *bucketno, int *batchno)
{
	NWayGetBucketAndBatch(*hashtable, hashvalue, bucketno, batchno);
	//printf("--> bucketno: %d - batchno: %d\n", bucketno, batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if ((*batchno) == (*hashtable)->curbatch)
	{
		NWayHashTableInsertBucket(hashtable, slot, hashvalue, *bucketno);
		(*hashtable)->numBucketTuples += 1;
	}
	else
	{
		MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert((*batchno) > (*hashtable)->curbatch);
		ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[(*batchno)]);
	}
}

bool NWayComputeActualJoinProbeTable2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2, 
				      Datum inKey, Datum *outKey, int *outBucket)
{
	int i;

	for(i=0; i<hashtable->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal1, keyVal2;
			bool isNull;
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */

			keyVal2 = slot_getattr(inntuple, attrNo2+1, &isNull);
			//printf("Buckets[%d] - KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", i, (unsigned long) keyVal1, (unsigned long) hashTuple->hashvalue, (unsigned long) keyVal2);
			if ((unsigned long)keyVal2 == (unsigned long)inKey)
			{
				keyVal1 = slot_getattr(inntuple, attrNo1+1, &isNull);
				(*outKey) = keyVal1;
				(*outBucket) = i;
				return true;
			}

			hashTuple = hashTuple->next;
		}
	}

	return false;
}

bool NWayComputeActualJoinProbeTable1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot,
		 		      int bucketno, Datum inKey)
{
	TupleTableSlot *inntuple;
	HashJoinTuple hashTuple = hashtable->buckets[bucketno];

	while (hashTuple != NULL)
	{
		Datum keyval;
		inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */
		econtext->ecxt_innertuple = inntuple;
		if( NWayGetKeyValue(econtext, hashkeys, hashtable->hashfunction, &keyval))
		{
			if ((ullong)keyval == (ullong)inKey)
				return true;
		}
		hashTuple = hashTuple->next;
	}
	return false;
}

int NWayComputeActualJoin(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
		          NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attr2No1, int attr2No2,
			  NWayHashTable *hashtable3, TupleTableSlot *tupSlot3, int attr3)
{
	int i, count=0;
	double timeval1 = 0.0, timeval2 = 0.0;

	Assert (tupSlot1 != NULL);
	Assert (tupSlot2 != NULL);
	Assert (tupSlot3 != NULL);

	for(i=0; i<hashtable3->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable3->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal2, keyVal3;
			bool isNull;
			int tab2Bucket;
			struct timeval  tu1, tu2;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot3, false);	 /* do not pfree */
			gettimeofday(&tu1, NULL);

			keyVal3 = slot_getattr(inntuple, attr3+1, &isNull);
			//printf("Buckets[%d] - KeyVal: %lu - HashValue: %lu\n", i, (unsigned long) keyVal, (unsigned long) hashTuple->hashvalue);
			if (NWayComputeActualJoinProbeTable2(hashtable2, tupSlot2, attr2No1, attr2No2, keyVal3, &keyVal2, &tab2Bucket))
			{
				struct timeval  tv1, tv2;
				gettimeofday(&tv1, NULL);
				if (NWayComputeActualJoinProbeTable1(hashtable1, econtext1, hashkeys1, tupSlot1, tab2Bucket, keyVal2))
					count++;
				gettimeofday(&tv2, NULL);
				timeval1 += (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
			}

			hashTuple = hashTuple->next;
				gettimeofday(&tu2, NULL);
				timeval2 += (double) (tu2.tv_usec - tu1.tv_usec) / 1000000 + (double) (tu2.tv_sec - tu1.tv_sec);
		}
	}

	printf("TimeVal1: %lf - TimeVal2: %lf\n",timeval1,timeval2);
	return count;
}


void NWayPrintBuckets1(NWayHashTable *hashtable, ExprContext *econtext, List *hashkeys, TupleTableSlot *tupSlot)
{
	int i;
	//FILE *fp = fopen("/tmp/b1","w");
/*
	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");
*/

	for(i=0; i<hashtable->nbuckets; i++)
	{
		TupleTableSlot *inntuple;
		uint32 hashvalue;
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal;
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */
			econtext->ecxt_innertuple = inntuple;
			//printf("Buckets[%d] - \n", i);
			if( NWayGetHashValuePrint(econtext, hashkeys, hashtable->hashfunction, &keyVal, &hashvalue))
			{
				//fprintf(fp, "%d %llu %llu\n",i, (ullong)keyVal, (ullong)hashvalue);
			}
			hashTuple = hashTuple->next;
		}
	}
	//fclose(fp);
}

void NWayPrintBuckets2(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2)
{
	int i;

	//FILE *fp = fopen("/tmp/b2","w");
/*
	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");
*/

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
			//printf("Buckets[%d] - KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", i, (unsigned long) keyVal1, (unsigned long) hashTuple->hashvalue, (unsigned long) keyVal2);
			//fprintf(fp,"%d %llu %llu %llu\n",i,(ullong)keyVal1, (ullong)hashTuple->hashvalue, (ullong)keyVal2);

			hashTuple = hashTuple->next;
		}
	}
	//fclose(fp);
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
	//printf("ActualAttr: %d\n", actualAttr);
	if(actualAttr == -1) // make sure we found the join key
		return false;

	//printf("\tProjectionInfo.pi_numSimpleVars: %d\n",  projInfo->pi_numSimpleVars);
	Assert(projInfo->pi_directMap == true);

	/* especially simple case where vars go to output in order */
	for (i = 0; i < projInfo->pi_numSimpleVars; i++)
	{
		int			varNumber = varNumbers[i] - 1;

		//printf("varNumber: %d \n", varNumber);
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

void NWayPrintIndirectHashTable2(IndirectHashTable *indirectHashTable, int numRows, int nbuckets)
{
	int i, j, k;
	printf("XYZ - numRows: %d - nbuckets: %d\n", numRows, nbuckets);
	for(i=0; i<INDIRECT_HASH_TABLE_SIZE; i++)
	{
		printf("HashValue[%d]: ", i);
		for(j=0; j<numRows; j++)
		{
			int bitValue = GetIndirectHashTableColValue(indirectHashTable, j, (uint32) i);
			if (bitValue == 1)
				printf("%d,",j);
		}
		printf("\n");
	}

#if 0
	for(i=0; i<numRows; i++)
	{
		int totalLen=0;
		for(k=0; k<IHT_BUCKET_PARTS; k++)
		{
			int len=0;
			printf("Row[%d][%d]: ",i,k);
			for(j=0; j<nbuckets; j++)
			{
				int bitValue = GetIndirectHashTableBucketValue(indirectHashTable, i, k, (uint32) j);
				if (bitValue == 1)
				{
					len = len+1;
					printf("%d,",j);
				}
			}
			printf("\nRowLen[%d][%d]: %d\n", i, k, len);
			totalLen += len;
		}
		printf("Total RowLen[%d]: %d\n",i, totalLen);
	}
#endif
}

void NWayBuildHashTable2(NWayHashTable **hashtable, PlanState  *node, FmgrInfo   hashfunction,
												int attr1, int attr2)
{
	TupleTableSlot *slot;
	int i,j;

	MemoryContextSwitchTo((*hashtable)->hashCxt);

	(*hashtable)->indirectHashTable = (IndirectHashTable*)palloc(sizeof(IndirectHashTable)*((*hashtable)->nbatch));
	(*hashtable)->ihtWidthCols = (int)(INDIRECT_HASH_TABLE_SIZE/BIT_WIDTH)+2;
	(*hashtable)->ihtWidthBucketIDs = (int)((*hashtable)->nbuckets/BIT_WIDTH)+2;
	//printf("IHT WidthCols: %d - WidthBucketIDs: %d\n",(*hashtable)->ihtWidthCols, (*hashtable)->ihtWidthBucketIDs);
	for(i=0; i<(*hashtable)->nbatch; i++)
	{
		(*hashtable)->indirectHashTable[i].cols = (long*)palloc0(sizeof(long)*((*hashtable)->ihtWidthCols));
		//(*hashtable)->indirectHashTable[i].bucketids = (long**)palloc0(sizeof(long*)*IHT_BUCKET_PARTS);
		//for(j=0; j<IHT_BUCKET_PARTS; j++)
			//(*hashtable)->indirectHashTable[i].bucketids[j] = (long*)palloc0(sizeof(long)*((*hashtable)->ihtWidthBucketIDs));
	}

	for (;;)
	{
		Datum keyVal1, keyVal2;
		uint32	hashvalue1, hashvalue2;
		uint32  indirectHValue;
		int batchno, bucketno;
		List *newCell;

		slot = ExecProcNode(node);

		if (TupIsNull(slot))
			break;

		keyVal1 = slot->tts_values[attr1];
		keyVal2 = slot->tts_values[attr2];

		NWayGetHashValueSimple(keyVal1, hashfunction, &hashvalue1);
		//printf("KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", (unsigned long) keyVal1, (unsigned long) hashvalue1, (unsigned long) keyVal2);
		//batchno = NWayHashTableInsert2(hashtable, slot, hashvalue1);
		NWayHashTableInsert2(hashtable, slot, hashvalue1, &bucketno, &batchno);
		(*hashtable)->totalTuples += 1;

		//NWayGetHashValueSimple(keyVal2, hashfunction, &hashvalue2);
		NWayGetHashValueSimple(keyVal2, (*hashtable)->hashfunction, &hashvalue2);
		indirectHValue = hashvalue2%INDIRECT_HASH_TABLE_SIZE;
		UpdateIndirectHashTable(&((*hashtable)->indirectHashTable), batchno, indirectHValue);
/*
		if (batchno == 1)
		{
			printf("AAAAAAHashTable2Batch1 %llu %llu %llu %llu %llu\n",
			       (ullong)keyVal1, (ullong)hashvalue1, (ullong)keyVal2, (ullong)hashvalue2, (ullong) indirectHValue);
		}
*/

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

/*
	for(i=0; i<INDIRECT_HASH_TABLE_SIZE; i++)
	{
		int bitValue = GetIndirectHashTableColValue((*hashtable)->indirectHashTable, 1, (uint32)i);
		printf("BBBBBBIndirectHashTable 1 %d %d\n",i, bitValue);
	}	
*/
	//NWayPrintIndirectHashTable((*hashtable)->indirectHashTable, INDIRECT_HASH_TABLE_SIZE, (*hashtable)->nbatch);
	//NWayPrintIndirectHashTable2((*hashtable)->indirectHashTable, INDIRECT_HASH_TABLE_SIZE, (*hashtable)->nbatch);
}

void NWayBuildHashTable3(NWayHashTable **hashtable, PlanState *node, IndirectHashTable *indirectHashTable, FmgrInfo hashfunction, int attrNo)
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

		(*hashtable)->totalTuples += 1;
	}

}

void NWayHashTableInsert3(NWayHashTable **hashtable, TupleTableSlot *slot, Datum keyVal, 
			  IndirectHashTable *indirectHashTable, FmgrInfo hashfunction)
{

	uint32	hashvalue;
	uint32 idxNum;
	int i;

	NWayGetHashValueSimple(keyVal, hashfunction, &hashvalue);
	idxNum = hashvalue%INDIRECT_HASH_TABLE_SIZE;

	for(i=0; i<(*hashtable)->nbatch; i++)
	{
		int bitValue = GetIndirectHashTableColValue(indirectHashTable, i, idxNum);
		
		if (bitValue == 1) // batch number 'i' is valid
		{
			//printf("\tKeyVal3 - batchNum: %d - ", i);
			if (i==0)
			{
				int bucketno, batchno2;
				NWayGetBucketAndBatch(*hashtable, hashvalue, &bucketno, &batchno2);
				NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
				(*hashtable)->numBucketTuples += 1;
				//printf("BucketNo: %d\n", bucketno);
			}
			else
			{
				MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
				/*
				 * put the tuple into a temp file for later batches
				 */
				Assert(i > (*hashtable)->curbatch);
				ExecHashJoinSaveTuple(tuple, hashvalue, &(*hashtable)->batchFile[i]);
				//printf("\n");
			}
		}
		//else
			//printf("\tKeyVal3 - BitValue is 0\n");
	}
}

void NWayPrintBuckets3(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo)
{
	int i;
/*
	if(tupSlot == NULL)
	{
		printf("TupSlot is NULL\n");
		return;
	}
	else
		printf("TupSlot is not NULL\n");
*/

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
			//printf("Buckets[%d] - KeyVal: %lu - HashValue: %lu\n", i, (unsigned long) keyVal, (unsigned long) hashTuple->hashvalue);
			//fprintf(fp,"%d %llu %llu\n",i, (ullong)keyVal, (ullong)hashTuple->hashvalue);

			hashTuple = hashTuple->next;
		}
	}
	//fclose(fp);
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
	(*hashtable)->numBucketTuples = 0;

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

void GetIHTColumnOffset(uint32 valueForCol, int *colID, int *offset)
{
	uint32 div1 = valueForCol/(uint32)BIT_WIDTH;
	uint32 div2 = valueForCol - (div1*BIT_WIDTH);
	if (div2 == 0)
	{
		(*colID) = div1;
		(*offset) = BIT_WIDTH-1;
	}
	else
	{
		(*colID) = div1+1;
		(*offset) = div2-1;
	}
}

//void UpdateIndirectHashTable(IndirectHashTable **iht, int rowID, uint32 valueForCol, int bucketno)
void UpdateIndirectHashTable(IndirectHashTable **iht, int rowID, uint32 valueForCol)
{
	int colIndex, colOffset;

	GetIHTColumnOffset(valueForCol, &colIndex, &colOffset);
	(*iht)[rowID].cols[colIndex] |= 1 << colOffset;
}


int GetIndirectHashTableColValue(IndirectHashTable *iht, int rowID, uint32 valueForCol)
{
	int colID, offset;
	int bitValue;

	GetIHTColumnOffset(valueForCol, &colID, &offset);
	bitValue = (iht[rowID].cols[colID] >> offset) & 1;
	return bitValue;
}

#if 0
int GetIndirectHashTableBucketValue(IndirectHashTable *iht, int rowID, int firstColID, int bucketNo)
{
	int colID, offset;
	int bitValue;

	GetIHTColumnOffset((uint32)bucketNo, &colID, &offset);
	bitValue = (iht[rowID].bucketids[firstColID][colID] >> offset) & 1;
	return bitValue;
}
#endif

double NWayComputeActualJoinBatch(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
		          NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attr2No1, int attr2No2,
			  NWayHashTable *hashtable3, TupleTableSlot *tupSlot3, int attr3)
{
	int i, j;
	double count=0.0;
	Datum *keyVal3Arr;
	Datum *keyVal2Arr;
	int *tab2BucketArr;
	bool *probe2Suc;
	double timeval1 = 0.0, timeval2 = 0.0;
	int curIndex=0;
	int actualProbeBatchSize = PROBE_BATCH_SIZE;
	int currentArraySize;

	Assert (tupSlot1 != NULL);
	Assert (tupSlot2 != NULL);
	Assert (tupSlot3 != NULL);

	if (actualProbeBatchSize > hashtable3->numBucketTuples)
		actualProbeBatchSize = hashtable3->numBucketTuples;

	currentArraySize = actualProbeBatchSize;
	keyVal3Arr = (Datum*)palloc0(sizeof(Datum)*actualProbeBatchSize);
	keyVal2Arr = (Datum*)palloc0(sizeof(Datum)*actualProbeBatchSize);
	tab2BucketArr = (int*)palloc0(sizeof(int)*actualProbeBatchSize);
	probe2Suc = (bool*)palloc0(sizeof(bool)*actualProbeBatchSize);
	for(j=0; j<actualProbeBatchSize; j++)
		probe2Suc[j] = false;

	for(i=0; i<hashtable3->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable3->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal2, keyVal3;
			bool isNull;
			int tab2Bucket;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot3, false);	 /* do not pfree */

			keyVal3 = slot_getattr(inntuple, attr3+1, &isNull);
			keyVal3Arr[curIndex++] = keyVal3;
			if (curIndex >= actualProbeBatchSize)
			{
				count += NWayComputeActualJoinProbeTable2BatchDriver(hashtable1, econtext1, hashkeys1, tupSlot1, 
								      hashtable2, tupSlot2, attr2No1, attr2No2, keyVal3Arr, actualProbeBatchSize,
								      &probe2Suc, &keyVal2Arr, &tab2BucketArr, &currentArraySize);

#if 0
				//printf("Buckets[%d] - KeyVal: %lu - HashValue: %lu\n", i, (unsigned long) keyVal, (unsigned long) hashTuple->hashvalue);
				NWayComputeActualJoinProbeTable2Batch(hashtable2, tupSlot2, attr2No1, attr2No2, keyVal3Arr, &probe2Suc, 
						          	      &keyVal2Arr, &tab2BucketArr, PROBE_BATCH_SIZE);
				for(j=0; j<PROBE_BATCH_SIZE; j++)
				{
					if (probe2Suc[j] && NWayComputeActualJoinProbeTable1(hashtable1, econtext1, hashkeys1, tupSlot1, 
											     tab2BucketArr[j], keyVal2Arr[j]))
						count++;
					//timeval1 += (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
				}
#endif
				for(j=0; j<currentArraySize; j++)
					probe2Suc[j] = false;
				//timeval2 += (double) (tu2.tv_usec - tu1.tv_usec) / 1000000 + (double) (tu2.tv_sec - tu1.tv_sec);
				curIndex = 0;
			}
			hashTuple = hashTuple->next;
		}
	}
	if (curIndex > 0)
		count += NWayComputeActualJoinProbeTable2BatchDriver(hashtable1, econtext1, hashkeys1, tupSlot1, 
							      	     hashtable2, tupSlot2, attr2No1, attr2No2, keyVal3Arr, curIndex,
								     &probe2Suc, &keyVal2Arr, &tab2BucketArr, &currentArraySize);

	//printf("TimeVal1: %lf - TimeVal2: %lf\n",timeval1,timeval2);
	return count;
}

double NWayComputeActualJoinProbeTable2BatchDriver(NWayHashTable *hashtable1, ExprContext *econtext1, List *hashkeys1, TupleTableSlot *tupSlot1,
						NWayHashTable *hashtable2, TupleTableSlot *tupSlot2, int attrNo1, int attrNo2, 
				      	   	Datum *inKey, int inArrSize, bool **success, Datum **outKey, int **outBucket, int *outArrSize)
{
	int j;
	double count=0.0;
	
	NWayComputeActualJoinProbeTable2Batch(hashtable2, tupSlot2, attrNo1, attrNo2, inKey, inArrSize, success,
					      outKey, outBucket, outArrSize);
	for(j=0; j<(*outArrSize); j++)
	{
		//printf("Final Result - lineitem.l_orderkey: %lu - ",(unsigned long) inKey[j], (unsigned long)(*outKey)[j]);
		if ((*success)[j] && NWayComputeActualJoinProbeTable1(hashtable1, econtext1, hashkeys1, tupSlot1, 
								     (*outBucket)[j], (*outKey)[j]))
			count++;
	}
	return count;
}

void NWayComputeActualJoinProbeTable2Batch(NWayHashTable *hashtable, TupleTableSlot *tupSlot, int attrNo1, int attrNo2, 
				      	   Datum *inKey, int inArrSize, bool **success, Datum **outKey, int **outBucket, int *outArrSize)
{
	int i, j, k;
	int curPtr=0;

	for(i=0; i<hashtable->nbuckets; i++)
	{
		HashJoinTuple hashTuple = hashtable->buckets[i];

		while (hashTuple != NULL)
		{
			Datum keyVal1, keyVal2;
			bool isNull;
			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			TupleTableSlot *inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), tupSlot, false);	 /* do not pfree */

			keyVal2 = slot_getattr(inntuple, attrNo2+1, &isNull);
			for(j=0; j<inArrSize; j++)
			{
				//printf("Buckets[%d] - KeyVal1: %lu - HashValue1: %lu - KeyVal2: %lu\n", i, (unsigned long) keyVal1, (unsigned long) hashTuple->hashvalue, (unsigned long) keyVal2);
				if ((ullong)keyVal2 == (ullong)inKey[j])
				{
					keyVal1 = slot_getattr(inntuple, attrNo1+1, &isNull);
					(*outKey)[curPtr] = keyVal1;
					(*outBucket)[curPtr] = i;
					(*success)[curPtr++]=true;
					if (curPtr >= (*outArrSize))
					{
						(*outArrSize) = (*outArrSize)*2;
						(*success) = (bool*)repalloc((*success), sizeof(bool)*(*outArrSize));
						(*outBucket) = (int*)repalloc((*outBucket), sizeof(int)*(*outArrSize));
						(*outKey) = (Datum*)repalloc((*outKey), sizeof(Datum)*(*outArrSize));
						for(k=curPtr; k<(*outArrSize); k++)
							(*success)[k] = false;
					}
				}
			}

			hashTuple = hashTuple->next;
		}
	}

	//printf("ABCDEF - numBucketsProbed: %d\n",numBucketsProbed);
}

bool NWayLoadNextBatchFileLastTable(NWayHashTable **hashtable, TupleTableSlot *tupSlot)
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
	(*hashtable)->numBucketTuples = 0;

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
			int bucketno, batchno;
			NWayGetBucketAndBatch(*hashtable, hashvalue, &bucketno, &batchno);
			NWayHashTableInsertBucket(hashtable, slot, hashvalue, bucketno);
			(*hashtable)->numBucketTuples += 1;
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
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
