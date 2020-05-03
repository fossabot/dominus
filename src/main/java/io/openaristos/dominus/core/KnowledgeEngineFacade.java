package io.openaristos.dominus.core;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.openaristos.dominus.core.graph.KnowledgeEdge;
import io.openaristos.dominus.core.graph.KnowledgeGraph;
import io.openaristos.dominus.core.graph.KnowledgeNode;
import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KnowledgeEngineFacade {
  private static final Logger LOG = LoggerFactory.getLogger(KnowledgeEngineFacade.class);

  private final Map<String, EntityType> entityTypeMap;
  private final Map<String, EntityUniverse> universeMap;
  private final KnowledgeGraph knowledgeGraph;

  public KnowledgeEngineFacade(
      Map<String, EntityType> entityTypeMap,
      Map<String, EntityUniverse> universeMap,
      KnowledgeGraph knowledgeGraph) {
    this.entityTypeMap = entityTypeMap;
    this.universeMap = universeMap;
    this.knowledgeGraph = knowledgeGraph;
  }

  public void acceptEntityAvroRecord(GenericRecord record) {
    final LocalMasterableEntity entity = getMasterableEntity(record);

    if (entity == null) {

      LOG.error("entity could not be built");
      return;
    }

    final EntityUniverse universe = universeMap.get(entity.getType().getName());
    final LocalMemoryEntityUniverse.AppendResult resolution = universe.append(entity);

    if (resolution == null || resolution.getMapping() == null) {
      LOG.error("entity could not be resolved");
      return;
    }

    try {
      knowledgeGraph.lock();

      resolution
          .getMapping()
          .forEach(
              (me, membership) -> {
                final KnowledgeNode v = KnowledgeNode.of(me);
                knowledgeGraph.append(v);
              });

      resolution
          .getDeprecations()
          .forEach(
              e -> {
                final KnowledgeNode srcV = KnowledgeNode.of(e.getOld());
                final KnowledgeNode newV = KnowledgeNode.of(e.getCurrent());

                assert !srcV.getUid().equals(newV.getUid());

                knowledgeGraph.append(srcV);
                knowledgeGraph.append(newV);

                knowledgeGraph.deprecate(srcV, newV, e.getSequenceId());
              });
    } finally {
      knowledgeGraph.unlock();
    }
  }

  private LocalMasterableEntity getMasterableEntity(GenericRecord record) {
    final String entityTypeCode = record.get("type").toString();

    if (!entityTypeMap.containsKey(entityTypeCode)) {
      LOG.error("failed to find entity model matching code `{}`", entityTypeCode);
      return null;
    }

    final EntityType entityType = entityTypeMap.get(entityTypeCode);

    if (!universeMap.containsKey(entityTypeCode)) {
      universeMap.put(entityTypeCode, new LocalMemoryEntityUniverse(entityType));
    }

    return LocalMasterableEntity.of(entityType, record);
  }

  public void acceptRelationshipAvroRecord(GenericRecord record) {
    final LocalMasterableEntity srcEntity =
        getMasterableEntity((GenericRecord) record.get("sourceEntity"));
    final LocalMasterableEntity tgtEntity =
        getMasterableEntity((GenericRecord) record.get("targetEntity"));

    if (srcEntity == null || tgtEntity == null) {
      LOG.error("one or more entities could not be built");
      return;
    }

    final EntityUniverse srcUniverse = universeMap.get(srcEntity.getType().getName());
    final EntityUniverse tgtUniverse = universeMap.get(tgtEntity.getType().getName());

    final LocalMemoryEntityUniverse.AppendResult srcResolution = srcUniverse.append(srcEntity);
    final LocalMemoryEntityUniverse.AppendResult tgtResolution = tgtUniverse.append(tgtEntity);

    if (srcResolution == null
        || tgtResolution == null
        || srcResolution.getMapping() == null
        || tgtResolution.getMapping() == null) {
      LOG.error("one or more entities could not be built");
      return;
    }

    try {
      // lock the graph
      knowledgeGraph.lock();

      srcResolution
          .getMapping()
          .forEach(
              (srcMe, srcMembership) -> {
                try {
                  final KnowledgeNode srcV = KnowledgeNode.of(srcMe);

                  knowledgeGraph.append(srcV);

                  tgtResolution
                      .getMapping()
                      .forEach(
                          (tgtMe, tgtMembership) -> {
                            try {
                              final KnowledgeNode tgtV = KnowledgeNode.of(tgtMe);

                              knowledgeGraph.append(tgtV);

                              final RangeSet<Long> tr = TreeRangeSet.create();

                              final GenericData.Array array =
                                  (GenericData.Array) record.get("effectiveDating");

                              if (array.size() <= 0) {
                                throw new RuntimeException("entity missing temporality");
                              }

                              array.forEach(
                                  k -> {
                                    final GenericData.Record r = (GenericData.Record) k;
                                    tr.add(Range.open((Long) r.get("key"), (Long) r.get("value")));
                                  });

                              knowledgeGraph.relate(
                                  srcV,
                                  tgtV,
                                  KnowledgeEdge.of(record.get("relationshipType").toString(), tr));

                            } catch (Exception ex) {
                              LOG.error("failed to handle resolution", ex);
                            }
                          });
                } catch (Exception ex) {
                  LOG.error("failed to handle resolutions", ex);
                }
              });

      // for each source deprecation
      srcResolution
          .getDeprecations()
          .forEach(
              e -> {
                try {
                  // obtain the old knowledge node
                  final KnowledgeNode srcV = KnowledgeNode.of(e.getOld());

                  // build the new knowledge node
                  final KnowledgeNode newV = KnowledgeNode.of(e.getCurrent());

                  // make sure they're of the same type
                  assert srcV.getMasterEntity()
                      .getEntityTypeCode()
                      .equals(newV.getMasterEntity().getEntityTypeCode());

                  knowledgeGraph.append(newV);
                  knowledgeGraph.deprecate(srcV, newV, e.getSequenceId());
                } catch (Exception ex) {
                  LOG.error("failed to handle deprecations", ex);
                }
              });

      // for each target deprecation
      tgtResolution
          .getDeprecations()
          .forEach(
              e -> {
                try {
                  final KnowledgeNode srcV = KnowledgeNode.of(e.getOld());
                  final KnowledgeNode newV = KnowledgeNode.of(e.getCurrent());

                  knowledgeGraph.append(newV);
                  knowledgeGraph.deprecate(srcV, newV, e.getSequenceId());

                } catch (Exception ex) {
                  LOG.error("failed to handle deprecations", ex);
                }
              });

    } finally {
      knowledgeGraph.unlock();
    }
  }

  public void flush() {
    knowledgeGraph.lock();
    knowledgeGraph.flush();
    knowledgeGraph.unlock();
  }
}
