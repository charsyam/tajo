/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

public class MergeJoinExec extends CommonJoinExec {

  // temporal tuples and states for nested loop join
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outerNext = null;
  private final Tuple prevOuterTuple;
  private final Tuple prevInnerTuple;

  private TupleList outerTupleSlots;
  private TupleList innerTupleSlots;
  private Iterator<Tuple> outerIterator;
  private Iterator<Tuple> innerIterator;

  private JoinTupleComparator joincomparator = null;
  private TupleComparator [] tupleComparator = null;

  private boolean end = false;

  public MergeJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner, SortSpec[] outerSortKey, SortSpec[] innerSortKey) {
    super(context, plan, outer, inner);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");

    final int INITIAL_TUPLE_SLOT = context.getQueryContext().getInt(SessionVars.JOIN_HASH_TABLE_SIZE);

    this.outerTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new TupleList(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joincomparator = new JoinTupleComparator(outer.getSchema(),
        inner.getSchema(), sortSpecs);
    this.tupleComparator = PhysicalPlanUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), outer.getSchema(), inner.getSchema());
    this.outerIterator = outerTupleSlots.iterator();
    this.innerIterator = innerTupleSlots.iterator();

    prevOuterTuple = new VTuple(leftChild.getSchema().size());
    prevInnerTuple = new VTuple(rightChild.getSchema().size());
  }

  public Tuple next() throws IOException {

    while (!context.isStopped()) {
      if (!outerIterator.hasNext() && !innerIterator.hasNext()) {
        if(end){
          return null;
        }

        if(outerTuple == null){
          outerTuple = leftChild.next();
        }
        if(innerTuple == null){
          innerTuple = rightChild.next();
        }

        outerTupleSlots.clear();
        innerTupleSlots.clear();

        int cmp;
        while ((cmp = joincomparator.compare(outerTuple, innerTuple)) != 0) {
          if (cmp > 0) {
            innerTuple = rightChild.next();
          } else if (cmp < 0) {
            outerTuple = leftChild.next();
          }
          if (innerTuple == null || outerTuple == null) {
            return null;
          }
        }

        prevOuterTuple.put(outerTuple.getValues());
        do {
          outerTupleSlots.add(outerTuple);
          outerTuple = leftChild.next();
          if (outerTuple == null) {
            end = true;
            break;
          }
        } while (tupleComparator[0].compare(prevOuterTuple, outerTuple) == 0);
        outerIterator = outerTupleSlots.iterator();
        outerNext = outerIterator.next();

        prevInnerTuple.put(innerTuple.getValues());
        do {
          innerTupleSlots.add(innerTuple);
          innerTuple = rightChild.next();
          if (innerTuple == null) {
            end = true;
            break;
          }
        } while (tupleComparator[1].compare(prevInnerTuple, innerTuple) == 0);
        innerIterator = innerTupleSlots.iterator();
      }

      if(!innerIterator.hasNext()){
        outerNext = outerIterator.next();
        innerIterator = innerTupleSlots.iterator();
      }

      frameTuple.set(outerNext, innerIterator.next());

      if (joinQual.eval(frameTuple).isTrue()) {
        return projector.eval(frameTuple);
      }
    }
    return null;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    outerTupleSlots.clear();
    innerTupleSlots.clear();
    outerIterator = outerTupleSlots.iterator();
    innerIterator = innerTupleSlots.iterator();
  }

  @Override
  public void close() throws IOException {
    super.close();

    outerTupleSlots.clear();
    innerTupleSlots.clear();
    outerTupleSlots = null;
    innerTupleSlots = null;
    outerIterator = null;
    innerIterator = null;
  }
}
