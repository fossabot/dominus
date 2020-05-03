package io.openaristos.dominus.core.graph.dsl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.apache.tinkerpop.gremlin.structure.Graph;

@SuppressWarnings("UnstableApiUsage")
public class GremlinUtils {

  public static DominusTraversalSource temporalTraversal(final Graph graph) {
    return temporalTraversal(graph, 0L, Long.MAX_VALUE);
  }

  public static DominusTraversalSource temporalTraversal(final Graph graph, long lower, long upper) {
    return graph.traversal(DominusTraversalSource.class).withSack(
        TreeRangeSet.create(ImmutableSet.of(Range.open(lower, upper))),
        TreeRangeSet::create,
        (a, a2) -> {
          final TreeRangeSet<Long> n = TreeRangeSet.create(a);
          n.addAll(a2);
          return n;
        });
  }

  public static DominusTraversalSource temporalTraversal(final DominusTraversalSource source, long lower, long upper) {
    return source.withSack(
        TreeRangeSet.create(ImmutableSet.of(Range.open(lower, upper))),
        TreeRangeSet::create,
        (a, a2) -> {
          final TreeRangeSet<Long> n = TreeRangeSet.create(a);
          n.addAll(a2);
          return n;
        });
  }

  public static DominusTraversalSource temporalTraversal(final DominusTraversalSource source) {
    return temporalTraversal(source, 0L, Long.MAX_VALUE);
  }
}
