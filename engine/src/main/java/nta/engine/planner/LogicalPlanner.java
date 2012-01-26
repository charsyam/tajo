package nta.engine.planner;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import nta.catalog.Column;
import nta.catalog.ColumnBase;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.engine.Context;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryAnalyzer;
import nta.engine.parser.QueryBlock;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.SortKey;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.JoinNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalRootNode;
import nta.engine.planner.logical.ProjectionNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.planner.logical.SortNode;
import nta.engine.planner.logical.UnaryNode;
import nta.engine.query.exception.InvalidQueryException;
import nta.engine.query.exception.NotSupportQueryException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class creates a logical plan from a parse tree ({@link QueryBlock})
 * generated by {@link QueryAnalyzer}.
 * 
 * @author Hyunsik Choi
 *
 * @see QueryBlock
 */
public class LogicalPlanner {
  private Log LOG = LogFactory.getLog(LogicalPlanner.class);

  private LogicalPlanner() {
  }

  /**
   * This generates a logical plan.
   * 
   * @param query a parse tree
   * @return a initial logical plan
   */
  public static LogicalNode createPlan(Context ctx, QueryBlock query) {

    LogicalNode plan = null;    

    switch(query.getStatementType()) {    

    case SELECT:
      plan = createSelectPlan(ctx, query);
      break;

    default:;
    throw new NotSupportQueryException(query.toString());
    }
    
    LogicalRootNode root = new LogicalRootNode();
    root.setSubNode(plan);
    annotateInOutSchemas(ctx, root);
    
    return root;
  }
  
  /**
   * ^(SELECT from_clause? where_clause? groupby_clause? selectList)
   * 
   * @param query
   * @return
   */
  private static LogicalNode createSelectPlan(Context ctx, QueryBlock query) {
    LogicalNode subroot = null;
    if(query.hasFromClause()) {
       subroot = createJoinTree(ctx, query.getFromTables());
    }
    
    if(query.hasWhereClause()) {
      SelectionNode selNode = 
          new SelectionNode(query.getWhereCondition());
      selNode.setSubNode(subroot);
      subroot = selNode;
    }
    
    if(query.hasGroupbyClause()) {
      GroupbyNode groupbyNode = new GroupbyNode(query.getGroupFields());
      if(query.hasHavingCond())
        groupbyNode.setHavingCondition(query.getHavingCond());
      
      groupbyNode.setSubNode(subroot);
      subroot = groupbyNode;
    }
    
    if(query.hasOrderByClause()) {
      SortNode sortNode = new SortNode(query.getSortKeys());
      sortNode.setSubNode(subroot);
      subroot = sortNode;
    }
    
    if(query.getProjectAll()) {
    } else {
        ProjectionNode prjNode = new ProjectionNode(query.getTargetList());
        prjNode.setSubNode(subroot);
        subroot = prjNode;
    }
    
    return subroot;  
  }
  
  private static LogicalNode createJoinTree(Context ctx, FromTable [] tables) {
    LogicalNode subroot = null;
    
    subroot = new ScanNode(tables[0]);
    if(tables.length > 1) {    
      for(int i=1; i < tables.length; i++) {
        subroot = new JoinNode(new ScanNode(tables[i]), subroot);
      }
    }
    
    return subroot;
  }
  
  /**
   * This method determines the input and output schemas of all logical 
   * operators.
   * 
   * @param ctx
   * @param logicalPlan
   * @return a result target list 
   */
  public static void annotateInOutSchemas(Context ctx, 
      LogicalNode logicalPlan) {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    refineInOutSchama(ctx, logicalPlan, null, stack);
  }
  
  static void getTargetListFromEvalTree(TargetList inputSchema, 
      EvalNode evalTree, Set<ColumnBase> targetList) {
    
    switch(evalTree.getType()) {
    case FIELD:
      FieldEval fieldEval = (FieldEval) evalTree;
      ColumnBase col = inputSchema.getColumn(fieldEval.getName());
      targetList.add(col);
      
      break;
    
    case PLUS:
    case MINUS:
    case MULTIPLY:
    case DIVIDE:
    case AND:
    case OR:    
    case EQUAL:
    case NOT_EQUAL:
    case LTH:
    case LEQ:
    case GTH:   
    case GEQ:
      getTargetListFromEvalTree(inputSchema, evalTree.getLeftExpr(), targetList);
      getTargetListFromEvalTree(inputSchema, evalTree.getRightExpr(), targetList);
      
      break;
     case FUNCTION:
       FuncCallEval funcEval = (FuncCallEval) evalTree;
       for(EvalNode evalNode : funcEval.getGivenArgs()) {
         getTargetListFromEvalTree(inputSchema, evalNode, targetList);
       }
    default: return;
    }
  }
  
  static void refineInOutSchama(Context ctx,
      LogicalNode logicalNode, Set<ColumnBase> necessaryTargets, 
      Stack<LogicalNode> stack) {
    TargetList inputSchema = null;
    TargetList outputSchema = null;
    
    switch(logicalNode.getType()) {
    case ROOT:
      LogicalRootNode root = (LogicalRootNode) logicalNode;
      stack.push(root);
      refineInOutSchama(ctx, root.getSubNode(), necessaryTargets, stack);
      stack.pop();
      break;
    
    case PROJECTION:
      ProjectionNode projNode = ((ProjectionNode)logicalNode);
      if(necessaryTargets != null) {
        for(Target t : projNode.getTargetList()) {
          getTargetListFromEvalTree(projNode.getInputSchema(), t.getEvalTree(), 
              necessaryTargets);
        }
        
        stack.push(projNode);
        refineInOutSchama(ctx, projNode.getSubNode(), necessaryTargets, stack);
        stack.pop();
        
        LogicalNode parent = stack.peek();
        if(parent instanceof UnaryNode) {
          ((UnaryNode) parent).setSubNode(((UnaryNode) projNode).getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      } else {
        stack.push(projNode);
        refineInOutSchama(ctx, projNode.getSubNode(), necessaryTargets, stack);
        stack.pop();
      }
      
      projNode.setInputSchema(projNode.getSubNode().getOutputSchema());
      TargetList prjTargets = new TargetList();
      for(Target t : projNode.getTargetList()) {
        DataType type = t.getEvalTree().getValueType();
        String name = t.getEvalTree().getName();
        prjTargets.put(new ColumnBase(name,type));
      }
      projNode.setOutputSchema(prjTargets);
      
      break;
      
    case SELECTION:
      SelectionNode selNode = ((SelectionNode)logicalNode);
      if(necessaryTargets != null) {
        getTargetListFromEvalTree(selNode.getInputSchema(), selNode.getQual(), 
            necessaryTargets);
      }
      stack.push(selNode);
      refineInOutSchama(ctx, selNode.getSubNode(), necessaryTargets, stack);
      stack.pop();
      inputSchema = selNode.getSubNode().getOutputSchema();
      selNode.setInputSchema(inputSchema);
      selNode.setOutputSchema(inputSchema);
      
      break;
      
    case GROUP_BY:
      GroupbyNode groupByNode = ((GroupbyNode)logicalNode);
      if(necessaryTargets != null && groupByNode.hasHavingCondition()) {
        getTargetListFromEvalTree(groupByNode.getInputSchema(),
            groupByNode.getHavingCondition(), necessaryTargets);
        for(EvalNode grpField : groupByNode.getGroupingColumns()) {
          getTargetListFromEvalTree(groupByNode.getInputSchema(),
              grpField, necessaryTargets);
        }
      }
      stack.push(groupByNode);
      refineInOutSchama(ctx, groupByNode.getSubNode(), necessaryTargets, stack);
      stack.pop();
      groupByNode.setInputSchema(groupByNode.getSubNode().getOutputSchema());
      
      TargetList grpTargets = new TargetList();
      for(Target t : ctx.getTargetList()) {
        DataType type = t.getEvalTree().getValueType();
        String name = t.getEvalTree().getName();
        grpTargets.put(new ColumnBase(name,type));
      }
      
      groupByNode.setOutputSchema(grpTargets);
      
      break;
      
    case SCAN:
      ScanNode scanNode = ((ScanNode)logicalNode);
      Schema scanSchema = 
          ctx.getInputTable(scanNode.getTableId()).getMeta().getSchema();
      TargetList scanTargetList = new TargetList();
      scanTargetList.put(scanSchema);
      scanNode.setInputSchema(scanTargetList);
      if(necessaryTargets != null) {
        outputSchema = new TargetList();
        for(ColumnBase col : scanTargetList.targetList.values()) {
          if(necessaryTargets.contains(col)) {
            outputSchema.put(col);
          }
        }
        scanNode.setOutputSchema(outputSchema);
        
        TargetList projectedList = new TargetList();
        if(scanNode.hasQual()) {
          getTargetListFromEvalTree(scanTargetList, scanNode.getQual(), 
              necessaryTargets);
        }
        for(ColumnBase col : scanTargetList.targetList.values()) {
          if(necessaryTargets.contains(col)) {
            projectedList.put(col);
          }
        }
        
        scanNode.setTargetList(projectedList);
      } else {
        scanNode.setOutputSchema(scanTargetList); 
      }
      
      break;
      
    case SORT:
      SortNode sortNode = ((SortNode)logicalNode);
      // TODO - before this, should modify sort keys to eval trees.
      stack.push(sortNode);
      refineInOutSchama(ctx, sortNode.getSubNode(), necessaryTargets, stack);
      stack.pop();
      inputSchema = sortNode.getSubNode().getOutputSchema();
      sortNode.setInputSchema(inputSchema);
      sortNode.setOutputSchema(inputSchema);
      
      break;
    
    case JOIN:
      JoinNode joinNode = (JoinNode) logicalNode;
      stack.push(joinNode);
      refineInOutSchama(ctx, joinNode.getLeftSubNode(), necessaryTargets, 
          stack);
      refineInOutSchama(ctx, joinNode.getRightSubNode(), necessaryTargets,
          stack);
      stack.pop();
      
      inputSchema = merge(joinNode.getLeftSubNode().getInputSchema(), 
          joinNode.getRightSubNode().getInputSchema());
      joinNode.setInputSchema(inputSchema);
      joinNode.setOutputSchema(inputSchema);
            
      default: return;
    }
  }
  
  public static class TargetList {
    NavigableMap<Integer,ColumnBase> targetList = new TreeMap<Integer,ColumnBase>();
    Map<String,ColumnBase> targetListByName = new HashMap<String, ColumnBase>();
    private volatile int id = 0;
    
    public TargetList() {
    }
    
    public TargetList(TargetList targets) {
      this.targetList = new TreeMap<Integer, ColumnBase>(targets.targetList);
      this.targetListByName = new HashMap<String, ColumnBase>(targets.targetListByName);
      this.id = targets.id;
    }
    
    public void put(ColumnBase column) {
      int newId = id++;
      targetList.put(newId, column);
      targetListByName.put(column.getName(), column);
    }
    
    public void put(ColumnBase [] columns) {
      int newId;
      for(ColumnBase column : columns) {
        newId = id++;
        targetList.put(newId, column);
        targetListByName.put(column.getName(), column);
      }
    }
    
    public void put(Schema schema) {
      int newId;
      
      for(Column column : schema.getColumns()) {
        newId = id++;
        targetList.put(newId, column);
        targetListByName.put(column.getName(), column);
      }
    }
    
    public ColumnBase getColumn(String qualifiedName) {
      return targetListByName.get(qualifiedName);
    }    
    
    public String toString() {
      StringBuilder sb = new StringBuilder("[");
      ColumnBase col = null;
      int i=0;  
      for(Entry<Integer,ColumnBase> entry : targetList.entrySet()) {
        col = entry.getValue();
        sb.append("\"").append(col.getName());
        sb.append(" ").append(col.getDataType()).append("\"");
        if(i < targetList.size() - 1) {
          sb.append(",");
        }
        i++;
      }
      sb.append("]");
      return sb.toString();
    }
    
    public Object clone() {
      return new TargetList(this);
    }
  }
  
  public static TargetList merge(TargetList left, TargetList right) {
    TargetList merged = new TargetList();
    for(Entry<Integer,ColumnBase> entry : left.targetList.entrySet()) {
      merged.put(entry.getValue());
    }
    for(Entry<Integer,ColumnBase> entry : right.targetList.entrySet()) {
      merged.put(entry.getValue());
    }
    
    return merged;
  }
}