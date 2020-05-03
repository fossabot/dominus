package io.openaristos.dominus.core.graph.internal.janus;

import com.google.common.base.Stopwatch;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Monitor;
import groovy.util.Eval;
import io.openaristos.dominus.*;
import io.openaristos.dominus.core.LocalEntityModel;
import io.openaristos.dominus.core.errors.DominusException;
import io.openaristos.dominus.core.graph.KnowledgeEdge;
import io.openaristos.dominus.core.graph.KnowledgeGraph;
import io.openaristos.dominus.core.graph.KnowledgeNode;
import io.openaristos.dominus.core.graph.dsl.DominusTraversalSource;
import io.openaristos.dominus.core.graph.dsl.GremlinUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class JanusKnowledgeGraph implements KnowledgeGraph {
  private static final String ENTITY_LABEL = "entity";
  private static final String UID_PROPERTY = "uid";
  private static final String ENTITY_TYPE_PROPERTY = "entityType";
  private static final String EFFECTIVE_DATING_COLUMN = "effectiveDating";

  private static final Logger LOG = LoggerFactory.getLogger(JanusKnowledgeGraph.class);

  private final Graph graph;
  private final Map<String, Long> vertices = Maps.newHashMap();
  private final Map<String, Pair<String, RangeSet<Long>>> edges = Maps.newHashMap();
  private final Map<String, Integer> vertexState = Maps.newHashMap();

  private final Monitor mutex = new Monitor();

  public JanusKnowledgeGraph(final Properties properties) {
    LOG.info("Instantiating JanusGraph data store");

    final JanusGraphFactory.Builder config = JanusGraphFactory.build();

    // Loads configuration properties into JanusDB
    config.set("storage.backend", properties.getProperty("janus.storage.backend"));
    config.set("storage.directory", properties.getProperty("janus.storage.directory"));
    config.set("index.search.backend", properties.getProperty("janus.index.search.backend"));
    config.set("index.search.directory", properties.getProperty("janus.index.search.directory"));

    // Instantiates Apache TinkerPop knowledge graph traversal query language
    this.graph = TinkerGraph.open();
    LOG.info("Apache TinkerPop successfully initiated");

    LOG.info("JanusGraph successfully initiated");
  }

  @Override
  public boolean append(KnowledgeNode vertex) {
    assert mutex.isOccupiedByCurrentThread();

    if (vertices.containsKey(vertex.getUid())) {

      final int currentState = vertex.getMasterEntity().state();
      final int previousState = vertexState.get(vertex.getUid());

      if (currentState != previousState) {
        final Vertex v = graph.traversal().V(vertices.get(vertex.getUid())).next();

        while (true) {
          try {
            vertex
                .getMasterEntity()
                .getDescriptors()
                .forEach(
                    (attr, valueRange) -> {
                      v.property(attr.getName(), valueRange.keySet().iterator().next());
                    });

            break;
          } catch (ConcurrentModificationException ignored) {

          }
        }

        vertexState.put(vertex.getUid(), vertex.getMasterEntity().state());
      }
      return false;
    }

    // create a traversal
    final GraphTraversalSource g = graph.traversal();

    try {

      // add basic properties
      GraphTraversal<Vertex, Vertex> t =
          g.addV(ENTITY_LABEL)
              .property(UID_PROPERTY, vertex.getMasterEntity().getUid())
              .property(ENTITY_TYPE_PROPERTY, vertex.getMasterEntity().getEntityTypeCode());

      // enrich with entity properties
      for (Map.Entry<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> entry :
          vertex.getMasterEntity().getDescriptors().entrySet()) {

        if (entry.getValue().size() > 0) {
          t = t.property(entry.getKey().getName(), entry.getValue().keySet().iterator().next());
        }
      }

      // obtain the vertex
      final Vertex v = t.next();

      // commit
      // g.tx().commit();

      // store on the map
      vertices.put(vertex.getMasterEntity().getUid(), (long) v.id());
      vertexState.put(vertex.getMasterEntity().getUid(), vertex.getMasterEntity().state());

      return true;

    } catch (Exception ex) {
      // rollback
      // g.tx().rollback();

      throw new DominusException("failed to add vertex to graph", ex);
    }
  }

  @Override
  public boolean relate(KnowledgeNode s, KnowledgeNode t, KnowledgeEdge e) {
    return relate(s.getUid(), t.getUid(), e);
  }

  private boolean relate(String s, String t, KnowledgeEdge e) {
    assert mutex.isOccupiedByCurrentThread();

    assert vertices.containsKey(s);
    assert vertices.containsKey(t);

    assert !s.equalsIgnoreCase(t);

    long vs = vertices.get(s);
    long vt = vertices.get(t);

    final String edgeKey = makeEdgeKey(vs, vt, e.getRelationship());

    // if this edge already exists
    if (edges.containsKey(edgeKey)) {

      // obtain the current temporality view
      final int originalHashCode = edges.get(edgeKey).getRight().hashCode();

      // add new temporality
      edges.get(edgeKey).getRight().addAll(e.getValidDts());

      // obtain the current view
      final int newHashCode = edges.get(edgeKey).getRight().hashCode();

      // if it has changed, update the edge on the graph
      if (originalHashCode != newHashCode) {
        graph
            .traversal()
            .E(Long.parseLong(edges.get(edgeKey).getKey()))
            .property(EFFECTIVE_DATING_COLUMN, edges.get(edgeKey).getRight())
            .iterate();
      }

      return false;
    }

    final GraphTraversalSource g = graph.traversal();

    try {
      // build the edge
      final RangeSet<Long> tr = TreeRangeSet.create(e.getValidDts());

      final Edge ex =
          g.addE(e.getRelationship())
              .from(g.V(vs))
              .to(g.V(vt))
              .property(EFFECTIVE_DATING_COLUMN, tr)
              .next();

      // record on graph
      edges.put(edgeKey, Pair.of(ex.id().toString(), tr));
      return true;

    } catch (Exception ex) {
      throw new DominusException("failed to add edge to graph", ex);
    }
  }

  @Override
  public void deprecate(KnowledgeNode s, KnowledgeNode t, long sequenceId) {
    assert mutex.isOccupiedByCurrentThread();

    assert vertices.containsKey(s.getUid());
    assert vertices.containsKey(t.getUid());

    final GraphTraversalSource g = graph.traversal();

    try {
      final long oldVertexId = vertices.get(s.getUid());
      final long replacementVertexId = vertices.get(t.getUid());

      for (final Edge e : g.V(s).outE().toList()) {
        handleEdgeDeprecation(oldVertexId, replacementVertexId, e, Direction.OUT);
      }

      for (final Edge e : g.V(s).inE().toList()) {
        handleEdgeDeprecation(oldVertexId, replacementVertexId, e, Direction.IN);
      }

      // drop existing nodes and edges
      g.V(oldVertexId).drop().iterate();

      vertices.remove(s.getUid());

      assert vertices.size() == graph.traversal().V().count().next();
      assert edges.size() == graph.traversal().E().count().next();

    } catch (Exception ex) {
      throw new DominusException("failed to deprecate nodes", ex);
    }
  }

  @Override
  public void lock() {
    assert !mutex.isOccupiedByCurrentThread();
    mutex.enter();
  }

  @Override
  public void unlock() {
    assert mutex.isOccupiedByCurrentThread();
    mutex.leave();
  }

  @Override
  public void flush() {
    assert mutex.isOccupiedByCurrentThread();
  }

  @Override
  public TraversalResponse resolveTraversal(TraversalRequest request) {
    LOG.info(
        "resolving query `{}` against graph with `{}` vertices and `{}` edges for times `{}` to `{}`",
        request.getDefinition(),
        graph.traversal().V().count().next(),
        graph.traversal().E().count().next(),
        request.getEffectiveStartDt(),
        request.getEffectiveEndDt());

    final Stopwatch stopwatch = Stopwatch.createStarted();
    final GraphTraversalSource g = graph.traversal();

    final DominusTraversalSource baseTraversal =
        GremlinUtils.temporalTraversal(
            g.getGraph().traversal(DominusTraversalSource.class),
            Math.max(request.getEffectiveStartDt(), 0),
            Math.min(request.getEffectiveEndDt(), Long.MAX_VALUE));

    // obtain the results
    final List results = ((GraphTraversal) Eval.x(baseTraversal, request.getDefinition())).toList();

    // if they're empty, return none
    if (results.isEmpty()) {
      return TraversalResponse.newBuilder().build();
    }

    // create a builder for the response
    final TraversalResponse.Builder builder = TraversalResponse.newBuilder();

    // iterate over all results
    for (int i = 0; i < results.size(); ++i) {
      final Object current = results.get(i);

      // process aliased regular entry
      if (Map.class.isAssignableFrom(current.getClass())) {
        builder.addAllEntries(mapAliasedEntry((Map) current));
      }
    }

    LOG.info("query took `{}` seconds", stopwatch.stop().elapsed(TimeUnit.SECONDS));

    return builder.build();
  }

  private Set<TraversalResponseEntryGroup> mapAliasedEntry(Map current) {

    final Set<TraversalResponseEntryGroup> res = Sets.newHashSet();

    final TraversalResponseEntryGroup.Builder result = TraversalResponseEntryGroup.newBuilder();

    // build base with vertices
    for (Object key : current.keySet()) {
      final Object value = current.get(key);

      if (Vertex.class.isAssignableFrom(value.getClass())) {

        final TraversalResponseEntry.Builder builder = mapVertex((Vertex) value);

        builder.setAlias(key.toString());
        result.addEntries(builder.build());
      }
    }

    // embellish with temporal edges
    for (Object key : current.keySet()) {
      final Object value = current.get(key);

      if (Edge.class.isAssignableFrom(value.getClass())) {
        for (TraversalResponseEntry.Builder b : mapEdge((Edge) value)) {
          b.setAlias(key.toString());
          res.add(result.clone().addEntries(b).build());
        }
      }
    }

    return res;
  }

  private Set<TraversalResponseEntry.Builder> mapEdge(Edge current) {
    final Set<TraversalResponseEntry.Builder> res = Sets.newHashSet();

    final RangeSet<Long> rangeSet = current.<RangeSet<Long>>property("temporality").orElse(null);

    if (rangeSet == null) {
      throw new RuntimeException("one or more edges are missing temporality");
    }

    for (Range<Long> r : rangeSet.asRanges()) {
      final TraversalResponseEntry.Builder builder = TraversalResponseEntry.newBuilder();

      builder.setUid(current.id().toString());
      builder.setEffectiveDating(
          TimestampRange.newBuilder()
              .setStart(r.lowerEndpoint())
              .setEnd(r.upperEndpoint())
              .build());

      res.add(builder);
    }

    return res;
  }

  private TraversalResponseEntry.Builder mapVertex(final Vertex v) {

    final TraversalResponseEntry.Builder builder = TraversalResponseEntry.newBuilder();

    v.properties()
        .forEachRemaining(
            property -> {
              final String keyS = property.key();
              final String valS = property.value().toString();

              if ("uid".equals(keyS)) {
                builder.setUid(valS);
              } else {
                builder.putAttributes(keyS, valS);
              }
            });

    return builder;
  }

  private String makeEdgeKey(Long s, Long t, String e) {
    return String.format("%s:%s:%s", s, t, e);
  }

  private void handleEdgeDeprecation(
      long oldVertexId, long replacementVertexId, Edge e, Direction d) {
    // get the other vertex id
    final long otherV = d == Direction.OUT ? (long) e.inVertex().id() : (long) e.outVertex().id();

    // get the existing edge key
    final String existingEdgeKey =
        d == Direction.OUT
            ? makeEdgeKey(oldVertexId, otherV, e.label())
            : makeEdgeKey(otherV, oldVertexId, e.label());

    // make sure self loop is not allowed
    assert replacementVertexId != otherV;

    // make sure the existing edge, exists.
    assert edges.containsKey(existingEdgeKey);

    final String sourceUid =
        d == Direction.OUT
            ? e.outVertex().property(UID_PROPERTY).value().toString()
            : e.inVertex().property(UID_PROPERTY).value().toString();

    final String targetUid =
        d == Direction.OUT
            ? e.inVertex().property(UID_PROPERTY).value().toString()
            : e.outVertex().property(UID_PROPERTY).value().toString();

    assert vertices.containsKey(sourceUid);
    assert vertices.containsKey(targetUid);

    relate(
        sourceUid, targetUid, KnowledgeEdge.of(e.label(), edges.get(existingEdgeKey).getRight()));

    // remove the old edge
    edges.remove(existingEdgeKey);
  }
}
