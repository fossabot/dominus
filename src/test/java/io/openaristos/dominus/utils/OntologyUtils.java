package io.openaristos.dominus.utils;

import com.google.common.collect.*;
import io.openaristos.dominus.core.*;
import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

public class OntologyUtils {
  static final LocalEntityModel.Matcher EQUALITY_MATCHER =
      LocalEntityModel.Matcher.of("equality", (x) -> x.getSource().equalsIgnoreCase(x.getTarget()));

  public static class Ontology {

    public LocalEntityModel entityModel;
    public EntityType entityType;
    public LocalMemoryEntityUniverse universe;

    public Ontology(
        LocalEntityModel entityModel, EntityType entityType, LocalMemoryEntityUniverse universe) {
      this.entityModel = entityModel;
      this.entityType = entityType;
      this.universe = universe;
    }

    public LocalMasterableEntityIdentity getRandomIdentity(LocalEntityModel.Attribute attr) {
      final String randomValue = UUID.randomUUID().toString();

      return LocalMasterableEntityIdentity.of(
          randomValue,
          entityType,
          RandomStringUtils.randomNumeric(10),
          LocalMasterableEntityDescriptor.of(attr, randomValue));
    }

    public static RangeSet<Long> perpetualRangeSet() {
      return TreeRangeSet.create(ImmutableSet.of(Range.openClosed(0L, Long.MAX_VALUE)));
    }

    public LocalMasterableEntity getRandomMasterableEntity(LocalEntityModel.Attribute attr) {

      final String randomValue = UUID.randomUUID().toString();

      final LocalMasterableEntityIdentity identity =
          LocalMasterableEntityIdentity.of(
              randomValue,
              entityType,
              RandomStringUtils.randomNumeric(10),
              LocalMasterableEntityDescriptor.of(attr, randomValue));

      return LocalMasterableEntity.of(
          entityType,
          identity,
          ImmutableMap.of(
              LocalMasterableEntityDescriptor.of(attr, randomValue), perpetualRangeSet()));
    }
  }

  public static Ontology getPersonOntology() {
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute email =
        LocalEntityModel.Attribute.of(
            "email", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute username =
        LocalEntityModel.Attribute.of(
            "username", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute publicKey =
        LocalEntityModel.Attribute.of(
            "publicKey", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // create entity model
    final LocalEntityModel personEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(uid, name, email, username, publicKey),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("uid", ImmutableSet.of(uid), 10),
                LocalEntityModel.Resolver.of("publicKey", ImmutableSet.of(publicKey), 9),
                LocalEntityModel.Resolver.of("email_username", ImmutableSet.of(username, email), 8),
                LocalEntityModel.Resolver.of("name_username", ImmutableSet.of(name, username), 7),
                LocalEntityModel.Resolver.of("name", ImmutableSet.of(name), 6),
                LocalEntityModel.Resolver.of("email", ImmutableSet.of(email), 5),
                LocalEntityModel.Resolver.of("username", ImmutableSet.of(email), 4)));

    // define entity type
    final EntityType personEntityType = EntityType.of("person", personEntityModel);

    // create entity universe and add entity
    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(personEntityType);

    return new Ontology(personEntityModel, personEntityType, universe);
  }
}
