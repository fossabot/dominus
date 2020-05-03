package io.openaristos.dominus.core.subscribers.internal;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import io.openaristos.dominus.core.EntityType;
import io.openaristos.dominus.core.EntityUniverse;
import io.openaristos.dominus.core.KnowledgeEngineFacade;
import io.openaristos.dominus.core.graph.KnowledgeGraph;
import io.openaristos.dominus.core.subscribers.ExternalSubscriber;
import io.openaristos.models.Entity;
import io.openaristos.models.Relationship;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PubSubExternalSubscriber implements ExternalSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubExternalSubscriber.class);

  private final Properties properties;
  private final Lock lock = new ReentrantLock();
  private Subscriber relationshipSubscriber;
  private Subscriber entitySubscriber;

  public PubSubExternalSubscriber(final Properties properties) {
    this.properties = properties;
  }

  @Override
  public synchronized void start() {
    lock.lock();

    if (!relationshipSubscriber.isRunning()) {
      relationshipSubscriber.startAsync();
      relationshipSubscriber.awaitRunning();
      LOG.info("started relationship listener");
    }

    if (!entitySubscriber.isRunning()) {
      entitySubscriber.startAsync();
      entitySubscriber.awaitRunning();
      LOG.info("started entity listener");
    }

    lock.unlock();
  }

  @Override
  public synchronized void stop() {
    lock.lock();

    if (relationshipSubscriber.isRunning()) {
      relationshipSubscriber.stopAsync();
      LOG.info("stopped relationship listener");
    }

    if (entitySubscriber.isRunning()) {
      relationshipSubscriber.startAsync();
      LOG.info("stopped entity listener");
    }

    lock.unlock();
  }

  @Override
  public synchronized void setup(
      final Map<String, EntityType> entityTypeMap,
      final Map<String, EntityUniverse> universeMap,
      final KnowledgeGraph knowledgeGraph) {

    this.relationshipSubscriber =
        Subscriber.newBuilder(
                properties.getProperty("subscribers.pubsub.relationships.subscription"),
                new RelationshipReceiver(entityTypeMap, universeMap, knowledgeGraph))
            .build();

    this.entitySubscriber =
        Subscriber.newBuilder(
                properties.getProperty("subscribers.pubsub.entities.subscription"),
                new EntityReceiver(entityTypeMap, universeMap, knowledgeGraph))
            .build();
  }

  static class BaseReceiver {
    protected int totalReceived;

    public int getTotalReceived() {
      return totalReceived;
    }
  }

  static class EntityReceiver extends BaseReceiver implements MessageReceiver {
    private final Schema entitySchema = ReflectData.get().getSchema(Entity.class);
    private final KnowledgeEngineFacade facade;

    EntityReceiver(
        Map<String, EntityType> entityTypeMap,
        Map<String, EntityUniverse> universeMap,
        KnowledgeGraph knowledgeGraph) {
      this.totalReceived = 0;
      this.facade = new KnowledgeEngineFacade(entityTypeMap, universeMap, knowledgeGraph);
    }

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      try {
        ++totalReceived;

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(entitySchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(pubsubMessage.getData().newInput(), null);

        final GenericRecord record = datumReader.read(null, decoder);

        facade.acceptEntityAvroRecord(record);

        if (totalReceived % 1000 == 0) LOG.info("received a total of `{}` messages", totalReceived);

      } catch (IOException e) {
        LOG.error("failed to process incoming entity", e);
      } finally {
        ackReplyConsumer.ack();
      }
    }
  }

  static class RelationshipReceiver extends BaseReceiver implements MessageReceiver {
    private final Schema relationshipSchema = ReflectData.get().getSchema(Relationship.class);
    private final KnowledgeEngineFacade facade;

    RelationshipReceiver(
        Map<String, EntityType> entityTypeMap,
        Map<String, EntityUniverse> universeMap,
        KnowledgeGraph knowledgeGraph) {
      this.facade = new KnowledgeEngineFacade(entityTypeMap, universeMap, knowledgeGraph);
    }

    @Override
    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      try {
        ++totalReceived;

        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(relationshipSchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(pubsubMessage.getData().newInput(), null);

        final GenericRecord record = datumReader.read(null, decoder);

        facade.acceptRelationshipAvroRecord(record);

        if (totalReceived % 1000 == 0)
          LOG.info("received a total of `{}` relationships", totalReceived);

      } catch (IOException e) {
        LOG.error("failed to process incoming relationship", e);
      } finally {
        ackReplyConsumer.ack();
      }
    }
  }
}
