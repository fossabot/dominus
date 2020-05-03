package io.openaristos.dominus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.openaristos.dominus.core.*;
import io.openaristos.dominus.core.errors.DominusException;
import io.openaristos.dominus.core.graph.KnowledgeEdge;
import io.openaristos.dominus.core.graph.KnowledgeGraph;
import io.openaristos.dominus.core.graph.KnowledgeNode;
import io.openaristos.dominus.core.graph.internal.janus.JanusKnowledgeGraph;
import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;
import io.openaristos.dominus.core.subscribers.ExternalSubscriber;
import io.openaristos.dominus.core.subscribers.SubscribersFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DominusApplication {

  public static final Map<String, EntityUniverse> universeMap = Maps.newConcurrentMap();
  public static final Map<String, EntityType> entityTypeMap = Maps.newConcurrentMap();
  public static final AtomicLong SEQUENCE_ID = new AtomicLong(0);
  private static final Logger LOG = LoggerFactory.getLogger(DominusApplication.class);

  private final List<ExternalSubscriber> subscriberList = Lists.newArrayList();
  private final Properties properties;
  private final JanusKnowledgeGraph knowledgeGraph;
  private Server server;

  public DominusApplication(Properties properties) {
    // Load configuration properties into Dominus
    this.properties = properties;

    // Load configuration properties into Knowledge Graph
    this.knowledgeGraph = new JanusKnowledgeGraph(properties);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    // Path where Dominus configuration properties live
    final String configPath = findConfigPath();

    // Checks that configuration file exists
    if (!Files.exists(Paths.get(configPath))) {
      LOG.error("Invalid configuration path `{}`.", configPath);
      return;
    }

    final Properties properties = new Properties();

    // Loads configuration properties into Dominus
    try (final InputStream input = new FileInputStream(configPath)) {
      properties.load(input);
    }

    LOG.info("{} configuration properties provided to Dominus.", properties.size());

    // Instantiates Dominus
    final DominusApplication server = new DominusApplication(properties);

    // Fires up the Dominus server
    server.start();
    server.blockUntilShutdown();
  }

  // Returns the path for where Dominus configuration properties live
  private static String findConfigPath() {
    return "/etc/dominus/dominus.properties";
  }

  // Responsible for starting Dominus
  private void start() throws IOException {
    // Adds all subscriptions from configuration
    subscriberList.addAll(SubscribersFactory.createFromProperties(properties));

    if (subscriberList.isEmpty()) {
      LOG.warn("No external subscribers were configured");
    } else {
      subscriberList.forEach(
          externalSubscriber ->
              externalSubscriber.setup(entityTypeMap, universeMap, knowledgeGraph));

      subscriberList.forEach(
          externalSubscriber ->
              LOG.info("Finished setup of external subscriber `{}`", externalSubscriber));
    }

    // Sets the port number that Dominus operates using
    final int port = Integer.parseInt(properties.getProperty("server.bind.port", "50051"));

    // Starts the service using defined parameters
    server =
        ServerBuilder.forPort(port)
            .addService(new DominusImpl(entityTypeMap, universeMap, knowledgeGraph, subscriberList))
            .build()
            .start();

    LOG.info("Started Dominus gRPC server on port `{}`", port);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    DominusApplication.this.stop();
                  } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                  }
                }));
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  static class DominusImpl extends DominusGrpc.DominusImplBase {

    private final Map<String, EntityType> entityTypeMap;
    private final Map<String, EntityUniverse> universeMap;
    private final KnowledgeGraph knowledgeGraph;
    private final List<ExternalSubscriber> subscriberList;

    DominusImpl(
        Map<String, EntityType> entityTypeMap,
        Map<String, EntityUniverse> universeMap,
        KnowledgeGraph knowledgeGraph,
        List<ExternalSubscriber> subscriberList) {
      this.entityTypeMap = entityTypeMap;
      this.universeMap = universeMap;
      this.knowledgeGraph = knowledgeGraph;
      this.subscriberList = subscriberList;
    }

    @Override
    public void appendEntityModel(
        EntityModel request, StreamObserver<EntityModelSummary> responseObserver) {

      if (entityTypeMap.containsKey(request.getEntityType())) {
        responseObserver.onNext(EntityModelSummary.newBuilder().build());
        responseObserver.onCompleted();

        return;
      }

      entityTypeMap.put(
          request.getEntityType(),
          EntityType.of(request.getEntityType(), LocalEntityModel.of(request)));

      responseObserver.onNext(EntityModelSummary.newBuilder().build());
      responseObserver.onCompleted();

      subscriberList.forEach(
          externalSubscriber -> {
            externalSubscriber.start();
            LOG.info("started external subscriber `{}`", externalSubscriber);
          });
    }

    @Override
    public void appendEntity(
        MasterableEntity masterableEntity, StreamObserver<EntityResolution> responseObserver) {
      final EntityType entityType =
          entityTypeMap.getOrDefault(masterableEntity.getEntityType(), null);

      if (entityType == null) {
        responseObserver.onError(new RuntimeException("entity type is not valid"));
        responseObserver.onCompleted();
        return;
      }

      EntityUniverse entityUniverse =
          universeMap.getOrDefault(masterableEntity.getEntityType(), null);
      if (entityUniverse == null) {
        entityUniverse = LocalMemoryEntityUniverse.of(entityType);
        universeMap.put(masterableEntity.getEntityType(), entityUniverse);
      }

      final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> results =
          entityUniverse
              .append(LocalMasterableEntity.of(entityType, masterableEntity))
              .getMapping();

      responseObserver.onNext(getEntityResolution(results));
      responseObserver.onCompleted();
    }

    @Override
    public void appendRelationship(
        AppendRelationshipRequest request,
        StreamObserver<AppendRelationshipResponse> responseObserver) {
      final EntityUniverse srcUniverse = universeMap.get(request.getSource().getEntityType());
      final EntityUniverse tgtUniverse = universeMap.get(request.getTarget().getEntityType());

      final EntityType srcEntityType = entityTypeMap.get(request.getSource().getEntityType());
      final EntityType tgtEntityType = entityTypeMap.get(request.getTarget().getEntityType());

      final LocalMemoryEntityUniverse.AppendResult srcResolution =
          srcUniverse.append(LocalMasterableEntity.of(srcEntityType, request.getSource()));
      final LocalMemoryEntityUniverse.AppendResult tgtResolution =
          tgtUniverse.append(LocalMasterableEntity.of(tgtEntityType, request.getTarget()));

      if (srcResolution == null || tgtResolution == null) {
        responseObserver.onError(
            new DominusException("invalid resolution for one or more given entities"));
        responseObserver.onCompleted();

        LOG.error("failed to resolve entity associated with request `{}`", request.toString());
        return;
      }

      srcResolution
          .getMapping()
          .forEach(
              (srcMe, srcMembership) -> {
                final KnowledgeNode srcV = KnowledgeNode.of(srcMe);
                knowledgeGraph.append(srcV);

                tgtResolution
                    .getMapping()
                    .forEach(
                        (tgtMe, tgtMembership) -> {
                          final KnowledgeNode tgtV = KnowledgeNode.of(tgtMe);

                          if (!srcV.equals(tgtV)) {
                            knowledgeGraph.append(tgtV);
                            knowledgeGraph.relate(
                                srcV,
                                tgtV,
                                KnowledgeEdge.of(
                                    request.getRelationship().getRelationship(),
                                    TreeRangeSet.create()));
                          }
                        });
              });

      responseObserver.onNext(AppendRelationshipResponse.newBuilder().setSuccess(true).build());
      responseObserver.onCompleted();
    }

    @Override
    public void resolveEntity(
        ResolveEntityRequest request, StreamObserver<EntityResolution> responseObserver) {
      final EntityUniverse universe = universeMap.getOrDefault(request.getEntityType(), null);

      if (universe == null) {
        responseObserver.onError(new RuntimeException("invalid universe"));
        responseObserver.onCompleted();
        return;
      }

      final EntityType entityType = entityTypeMap.getOrDefault(request.getEntityType(), null);
      if (entityType == null) {
        responseObserver.onError(new RuntimeException("entity type is not valid"));
        responseObserver.onCompleted();
        return;
      }

      final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> resolution =
          universe.resolve(ResolveQuery.of(entityType, request));

      responseObserver.onNext(getEntityResolution(resolution));
      responseObserver.onCompleted();
    }

    @Override
    public void resolveTraversal(
        TraversalRequest request, StreamObserver<TraversalResponse> responseObserver) {

      final TraversalResponse response = knowledgeGraph.resolveTraversal(request);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    private EntityResolution getEntityResolution(
        final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> results) {
      final EntityResolution.Builder builder = EntityResolution.newBuilder();

      results.forEach(
          (me, memberships) -> {
            final Set<TemporalDescriptor> descriptors = Sets.newHashSet();

            me.getDescriptors()
                .forEach(
                    (x, y) -> {
                      y.forEach(
                          (attributeValue, rangeSet) -> {
                            descriptors.add(
                                TemporalDescriptor.newBuilder()
                                    .setKey(x.getName())
                                    .setValue(attributeValue)
                                    .build());
                          });
                    });

            final MasterEntity masterEntity =
                MasterEntity.newBuilder()
                    .setUid(me.getUid())
                    .addAllDescriptors(descriptors)
                    .build();

            builder.addEntries(
                EntityResolutionEntry.newBuilder().setMasterEntity(masterEntity).build());
          });

      return builder.build();
    }
  }
}
