package io.openaristos.dominus.core;

import com.google.common.collect.*;
import io.openaristos.dominus.MasterableEntity;
import io.openaristos.dominus.TemporalDescriptor;
import io.openaristos.dominus.TimestampRange;
import lombok.EqualsAndHashCode;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

@EqualsAndHashCode
public class LocalMasterableEntity {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMasterableEntity.class);

  private EntityType type;
  private LocalMasterableEntityIdentity masterableEntityIdentity;
  private Map<LocalMasterableEntityDescriptor, RangeSet<Long>> descriptors;

  private LocalMasterableEntity(
      EntityType type,
      LocalMasterableEntityIdentity masterableEntityIdentity,
      Map<LocalMasterableEntityDescriptor, RangeSet<Long>> descriptors) {
    this.type = type;
    this.masterableEntityIdentity = masterableEntityIdentity;
    this.descriptors = descriptors;
  }

  public LocalMasterableEntity() {}

  public static LocalMasterableEntity of(EntityType type, MasterableEntity masterableEntity) {

    final Map<LocalMasterableEntityDescriptor, RangeSet<Long>> descriptors = Maps.newHashMap();

    for (TemporalDescriptor descriptor : masterableEntity.getDescriptorsList()) {

      final LocalMasterableEntityDescriptor localMasterableEntityDescriptor =
          LocalMasterableEntityDescriptor.of(
              type.getLocalEntityModel().getAttributeByName().get(descriptor.getKey()),
              descriptor.getValue());

      if (!descriptors.containsKey(localMasterableEntityDescriptor)) {
        descriptors.put(
            localMasterableEntityDescriptor,
            TreeRangeSet.create(ImmutableList.of(Range.open(Long.MIN_VALUE, Long.MAX_VALUE))));
      }

      for (TimestampRange current : descriptor.getRangesList()) {
        descriptors
            .get(localMasterableEntityDescriptor)
            .add(Range.open(current.getStart(), current.getEnd()));
      }
    }

    return new LocalMasterableEntity(
        type,
        LocalMasterableEntityIdentity.of(
            masterableEntity.getIdentity().getUid(),
            type,
            masterableEntity.getPerspective(),
            masterableEntity.getIdentity()),
        descriptors);
  }

  public static LocalMasterableEntity of(EntityType type, GenericRecord entity) {
    final GenericRecord identity = (GenericRecord) entity.get("identity");
    final Set<LocalMasterableEntityDescriptor> identityDescriptors = Sets.newHashSet();
    final Map<LocalMasterableEntityDescriptor, RangeSet<Long>> entityDescriptors =
        Maps.newHashMap();
    final Map<String, LocalEntityModel.Attribute> attributeMap =
        type.getLocalEntityModel().getAttributeByName();

    for (GenericRecord identityDescriptor : (Iterable<GenericRecord>) identity.get("descriptors")) {

      final LocalEntityModel.Attribute attribute =
          attributeMap.get(identityDescriptor.get("key").toString().toLowerCase());
      if (attribute == null) {
        LOG.error(
            "attribute `{}` does not exist in entity type `{}`",
            identityDescriptor.get("key"),
            type);
        return null;
      }

      identityDescriptors.add(
          LocalMasterableEntityDescriptor.of(
              attributeMap.get(identityDescriptor.get("key").toString()),
              identityDescriptor.get("value").toString()));
    }

    final LocalMasterableEntityIdentity id =
        LocalMasterableEntityIdentity.of(
            identity.get("identity").toString(),
            type,
            identity.get("perspective").toString(),
            identityDescriptors);

    for (GenericRecord entityDescriptor : (Iterable<GenericRecord>) entity.get("descriptors")) {
      final LocalMasterableEntityDescriptor key =
          LocalMasterableEntityDescriptor.of(
              attributeMap.get(entityDescriptor.get("key").toString()),
              entityDescriptor.get("value").toString());

      if (!entityDescriptors.containsKey(key)) {
        entityDescriptors.put(key, TreeRangeSet.create());
      }

      final GenericData.Array array =
          (GenericData.Array)entityDescriptor.get("effectiveDating");

      if(array.size() <= 0) {
        throw new RuntimeException("entity missing temporality");
      }

      array.forEach(k -> {
        final GenericData.Record r = (GenericData.Record)k;
        entityDescriptors
            .get(key)
            .add(Range.open((Long)r.get("key"), (Long)r.get("value")));
      });
    }

    return LocalMasterableEntity.of(type, id, entityDescriptors);
  }

  public static LocalMasterableEntity of(
      EntityType type,
      LocalMasterableEntityIdentity masterableEntityIdentity,
      Map<LocalMasterableEntityDescriptor, RangeSet<Long>> descriptors) {
    return new LocalMasterableEntity(type, masterableEntityIdentity, descriptors);
  }

  public EntityType getType() {
    return type;
  }

  public LocalMasterableEntityIdentity getMasterableEntityIdentity() {
    return masterableEntityIdentity;
  }

  public Map<LocalMasterableEntityDescriptor, RangeSet<Long>> getDescriptors() {
    return descriptors;
  }
}
