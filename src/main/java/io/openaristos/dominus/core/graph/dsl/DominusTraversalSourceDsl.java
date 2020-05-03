package io.openaristos.dominus.core.graph.dsl;

import com.google.common.collect.Maps;
import io.openaristos.dominus.DominusApplication;
import io.openaristos.dominus.core.*;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage")
public class DominusTraversalSourceDsl extends GraphTraversalSource {

  DominusTraversalSourceDsl(final Graph graph, final TraversalStrategies traversalStrategies) {
    super(graph, traversalStrategies);
  }

  DominusTraversalSourceDsl(final Graph graph) {
    super(graph);
  }

  DominusTraversalSourceDsl(final RemoteConnection connection) {
    super(connection);
  }

  public GraphTraversal<Vertex, Vertex> entity(String... attributeList) {

    if (attributeList.length <= 0 || (attributeList.length - 1) % 2 != 0) {
      throw new IllegalArgumentException("attribute list is invalid");
    }

    final String entityType = attributeList[0];
    final Map<String, String> attributes = Maps.newHashMap();

    for (int i = 1; i < attributeList.length; i += 2) {
      attributes.put(attributeList[i], attributeList[i + 1]);
    }

    if (!DominusApplication.universeMap.containsKey(entityType)
        || !DominusApplication.entityTypeMap.containsKey(entityType)) {
      throw new IllegalArgumentException("entity type is not found");
    }

    final EntityUniverse universe = DominusApplication.universeMap.get(entityType);

    final EntityType type = DominusApplication.entityTypeMap.get(entityType);

    final Map<LocalEntityModel.Attribute, String> query = Maps.newHashMap();

    for (Map.Entry<String, String> attrDef : attributes.entrySet()) {
      final LocalEntityModel.Attribute attr =
          type.getLocalEntityModel().getAttributeByName().getOrDefault(attrDef.getKey(), null);

      if (attr == null) {
        throw new IllegalArgumentException("attribute is invalid");
      }

      query.put(attr, attrDef.getValue());
    }

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> memberships =
        universe.resolve(ResolveQuery.of(query));

    if (memberships == null || memberships.size() <= 0) {
      throw new IllegalArgumentException("could not resolve entity");
    }

    GraphTraversal<Vertex, Vertex> traversal = this.clone().V();
    traversal = traversal.hasLabel("entity");

    final LocalMasterEntity me = memberships.keySet().iterator().next();

    traversal = traversal.has("uid", me.getUid());

    return traversal;
  }
}
