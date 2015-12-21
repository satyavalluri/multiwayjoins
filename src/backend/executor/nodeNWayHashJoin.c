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

TupleTableSlot * ExecNWayHashJoin(HashJoinState *node) /* return: a tuple or NULL */
{
	PlanState  *outerNode, *outerNode1, *outerNode2, *outerNode3;
	HashState  *hashNode, *hashNode2;
	HashJoinState *node2;
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
	/*
	//printf("node.type: %d - node2.type: %d\n", node->type, node2->type);
	printf("outerNode.type: %d - outerNode1.type: %d - outerNode2.type: %d - outerNode3.type: %d\n",outerNode->type, outerNode1->type, outerNode2->type, outerNode3->type);
*/
	exit(0);
}
