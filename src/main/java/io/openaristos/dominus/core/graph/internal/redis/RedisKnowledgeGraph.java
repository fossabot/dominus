package io.openaristos.dominus.core.graph.internal.redis;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import io.openaristos.dominus.TraversalRequest;
import io.openaristos.dominus.TraversalResponse;
import io.openaristos.dominus.core.LocalEntityModel;
import io.openaristos.dominus.core.graph.KnowledgeEdge;
import io.openaristos.dominus.core.graph.KnowledgeGraph;
import io.openaristos.dominus.core.graph.KnowledgeNode;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class RedisKnowledgeGraph implements KnowledgeGraph {
  private static final String GRAPH_ID = "dominus";
  private static final String UID_PARAM = "uid";

  private final RedisGraph graph;

  public RedisKnowledgeGraph(final Properties properties) {
    this.graph = new RedisGraph();
  }

  @Override
  public boolean append(KnowledgeNode k) {

    final Set<String> clauses = Sets.newHashSet();

    // build a map with the properties
    final Map<String, Object> params = Maps.newHashMap();

    // for each property
    for (final Map.Entry<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> descriptor :
        k.getMasterEntity().getDescriptors().entrySet()) {

      final String key = descriptor.getKey().getName();

      // append the value
      params.put(key, descriptor.getValue().keySet().iterator().next());

      // add the clause
      clauses.add(String.format("%s:%s", key, "$" + key));
    }

    // append the uid
    params.put(UID_PARAM, k.getUid());

    // update the uid clause
    clauses.add(String.format("%s:%s", UID_PARAM, "$" + UID_PARAM));

    final String query =
        String.format(
            "MERGE (:%s{%s})", k.getMasterEntity().getEntityTypeCode(), String.join(",", clauses));

    // obtain the result set
    graph.query(GRAPH_ID, query, params);
    return true;
  }

  @Override
  public boolean relate(KnowledgeNode s, KnowledgeNode t, KnowledgeEdge e) {
    for (final Range<Long> r : e.getValidDts().asRanges()) {
      final String query =
          String.format(
              "MATCH (a:%s), (b:%s) WHERE (a.uid = $sourceUid AND b.uid = $targetUid) CREATE (a)-[:%s {effectiveStartDt: $effectiveStartDt, effectiveEndDt: $effectiveEndDt}]->(b)",
              s.getMasterEntity().getEntityTypeCode(),
              t.getMasterEntity().getEntityTypeCode(),
              e.getRelationship());

      final Map<String, Object> params = Maps.newHashMap();

      params.put("sourceUid", s.getUid());
      params.put("targetUid", t.getUid());
      params.put("effectiveStartDt", r.lowerEndpoint());
      params.put("effectiveEndDt", r.upperEndpoint());

      graph.query(GRAPH_ID, query, params);
    }

    return true;
  }

  @Override
  public void deprecate(KnowledgeNode s, KnowledgeNode t, long sequenceId) {}

  @Override
  public void lock() {}

  @Override
  public void unlock() {}

  @Override
  public void flush() {}

  @Override
  public TraversalResponse resolveTraversal(TraversalRequest request) {
    return null;
  }
}
