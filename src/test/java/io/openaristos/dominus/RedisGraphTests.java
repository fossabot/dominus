package io.openaristos.dominus;

import com.google.common.collect.Lists;
import io.openaristos.dominus.core.LocalEntityModel;
import io.openaristos.dominus.core.LocalMasterEntity;
import io.openaristos.dominus.core.LocalMasterableEntity;
import io.openaristos.dominus.core.graph.KnowledgeEdge;
import io.openaristos.dominus.core.graph.KnowledgeNode;
import io.openaristos.dominus.core.graph.internal.redis.RedisKnowledgeGraph;
import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;
import io.openaristos.dominus.utils.OntologyUtils;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Ignore
public class RedisGraphTests {
  final int maxIterations = 100;

  @Test
  public void testCanInsertKnowledgeNode() {
    final RedisKnowledgeGraph knowledgeGraph = new RedisKnowledgeGraph(null);
    final OntologyUtils.Ontology ontology = OntologyUtils.getPersonOntology();
    final Map<String, LocalEntityModel.Attribute> attrs = ontology.entityModel.getAttributeByName();

    final LocalMasterableEntity masterableEntity =
        ontology.getRandomMasterableEntity(attrs.get("uid"));

    final LocalMemoryEntityUniverse.AppendResult appendResult =
        ontology.universe.append(masterableEntity);

    Assert.assertNotNull(appendResult);
    Assert.assertEquals(1, appendResult.getMapping().keySet().size());

    final LocalMasterEntity masterEntity = appendResult.getMapping().keySet().iterator().next();

    Assert.assertTrue(knowledgeGraph.append(KnowledgeNode.of(masterEntity)));
  }

  @Test
  public void testCanRelateKnowledgeNodes() {
    final RedisKnowledgeGraph knowledgeGraph = new RedisKnowledgeGraph(null);
    final OntologyUtils.Ontology ontology = OntologyUtils.getPersonOntology();
    final Map<String, LocalEntityModel.Attribute> attrs = ontology.entityModel.getAttributeByName();

    final List<KnowledgeNode> knowledgeNodeSet = Lists.newArrayList();

    for (int i = 0; i < maxIterations; ++i) {
      final LocalMasterableEntity masterableEntity =
          ontology.getRandomMasterableEntity(attrs.get("uid"));

      final LocalMemoryEntityUniverse.AppendResult appendResult =
          ontology.universe.append(masterableEntity);

      knowledgeNodeSet.add(KnowledgeNode.of(appendResult.getMapping().keySet().iterator().next()));
    }

    for (int i = 0; i < maxIterations - 1; ++i) {
      knowledgeGraph.relate(
          knowledgeNodeSet.get(i),
          knowledgeNodeSet.get(i + 1),
          KnowledgeEdge.of("relatedTo", OntologyUtils.Ontology.perpetualRangeSet()));
    }
  }
}
