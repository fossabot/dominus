package io.openaristos.dominus.core.graph.internal.janus;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JanusSchemaApplier {
  private static final Logger LOG = LoggerFactory.getLogger(JanusSchemaApplier.class);

  public static void apply(final JanusGraph graph) {

    final JanusGraphManagement management = graph.openManagement();

    if (management.containsGraphIndex("byUid")) {
      LOG.info("detected existing indices, skipping creation of indices");
      return;
    }

    final PropertyKey uidProperty =
        management
            .makePropertyKey("uid")
            .dataType(String.class)
            .cardinality(Cardinality.SINGLE)
            .make();

    management
        .makePropertyKey("display")
        .dataType(String.class)
        .cardinality(Cardinality.SINGLE)
        .make();

    management.makeVertexLabel("entity").make();

    management.buildIndex("byUid", Vertex.class).addKey(uidProperty).unique().buildCompositeIndex();

    management.commit();
  }
}
