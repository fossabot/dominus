package io.openaristos.dominus.core.graph.dsl.temporal;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.function.BiFunction;
import java.util.function.Predicate;

public class TraversalUtils {

  @SuppressWarnings("UnstableApiUsage")
  public static class TemporalRangeSetOverlap
      implements BiFunction<RangeSet<Long>, RangeSet<Long>, RangeSet<Long>> {
    @Override
    public RangeSet<Long> apply(RangeSet<Long> l1, RangeSet<Long> l2) {
      final TreeRangeSet<Long> n = TreeRangeSet.create(l1);
      n.removeAll(l2.complement());
      return n;
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  public static class EffectiveDatingFilter implements Predicate<Traverser<Edge>> {

    @Override
    public boolean test(Traverser<Edge> edgeTraverser) {
      try {
        final TreeRangeSet<Long> dating = edgeTraverser.sack();

        return dating.asRanges().size() > 0;

      } catch (Exception ex) {
        return false;
      }
    }
  }
}
