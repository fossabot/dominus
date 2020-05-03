package io.openaristos.dominus.core.graph.dsl;

import com.google.common.collect.Maps;
import io.openaristos.dominus.DominusApplication;
import io.openaristos.dominus.core.*;
import io.openaristos.dominus.core.graph.dsl.temporal.TraversalUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Set;

@GremlinDsl(traversalSource = "io.openaristos.dominus.core.graph.dsl.DominusTraversalSourceDsl")
public interface DominusTraversalDsl<S, E> extends GraphTraversal.Admin<S, E> {

  default GraphTraversal<S, Vertex> outT(String... edgeLabels) {
    return outE(edgeLabels)
        .sack(new TraversalUtils.TemporalRangeSetOverlap())
        .by("effectiveDating")
        .filter(new TraversalUtils.EffectiveDatingFilter())
        .property("temporality", __.sack())
        .inV();
  }

  default GraphTraversal<S, Edge> outET(String... edgeLabels) {
    return outE(edgeLabels)
        .sack(new TraversalUtils.TemporalRangeSetOverlap())
        .by("effectiveDating")
        .filter(new TraversalUtils.EffectiveDatingFilter())
        .property("temporality", __.sack());
  }

  default GraphTraversal<S, Vertex> inT(String... edgeLabels) {
    return inE(edgeLabels)
        .sack(new TraversalUtils.TemporalRangeSetOverlap())
        .by("effectiveDating")
        .filter(new TraversalUtils.EffectiveDatingFilter())
        .property("temporality", __.sack())
        .outV();
  }

  default GraphTraversal<S, Edge> inET(String... edgeLabels) {
    return inE(edgeLabels)
        .sack(new TraversalUtils.TemporalRangeSetOverlap())
        .by("effectiveDating")
        .filter(new TraversalUtils.EffectiveDatingFilter())
        .property("temporality", __.sack());
  }

  default GraphTraversal<S, Vertex> mHas(int direction, String... attributeList) {

    if (attributeList.length <= 0 || (attributeList.length - 1) % 2 != 0) {
      return null;
    }

    final String entityType = attributeList[0];
    final Map<String, String> attributes = Maps.newHashMap();

    for (int i = 1; i < attributeList.length; i += 2) {
      attributes.put(attributeList[i], attributeList[i + 1]);
    }

    if (!DominusApplication.universeMap.containsKey(entityType)
        || !DominusApplication.entityTypeMap.containsKey(entityType)) {
      throw new IllegalArgumentException("invalid entity type");
    }

    final EntityUniverse universe = DominusApplication.universeMap.get(entityType);

    final EntityType type = DominusApplication.entityTypeMap.get(entityType);

    final Map<LocalEntityModel.Attribute, String> query = Maps.newHashMap();

    for (Map.Entry<String, String> attrDef : attributes.entrySet()) {
      final LocalEntityModel.Attribute attr =
          type.getLocalEntityModel().getAttributeByName().getOrDefault(attrDef.getKey(), null);

      if (attr == null) {
        throw new IllegalArgumentException("invalid attribute");
      }

      query.put(attr, attrDef.getValue());
    }

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> memberships =
        universe.resolve(ResolveQuery.of(query));

    if (memberships == null || memberships.size() <= 0) {
      throw new IllegalArgumentException("could not resolve entity");
    }

    final LocalMasterEntity me = memberships.keySet().iterator().next();

    return has("uid", me.getUid()).out();
  }
}
