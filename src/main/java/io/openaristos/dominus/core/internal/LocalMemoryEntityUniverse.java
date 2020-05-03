package io.openaristos.dominus.core.internal;

import com.google.common.collect.*;
import io.openaristos.dominus.DominusApplication;
import io.openaristos.dominus.core.*;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

// An entity universe is an isolated container of entities
@SuppressWarnings("UnstableApiUsage")
public class LocalMemoryEntityUniverse implements EntityUniverse {
  private static final Logger LOG = LoggerFactory.getLogger(DominusApplication.class);

  private final EntityType entityType;
  private final Map<
          LocalMasterableEntityIdentity,
          Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>>>
      entitySnapshot = Maps.newHashMap();
  private final Set<LocalMasterEntity> candidateForDeprecation = Sets.newHashSet();

  // A masterable entity is dirty when it is new, changed, or peer master entity member has changed
  private final Set<LocalMasterableEntityIdentity> dirtyIdentities =
      Sets.newTreeSet(
          (o1, o2) -> {
            final Iterator<LocalEntityModel.Resolver> srcResolvers =
                getMatchingResolvers(o1).iterator();

            if (!srcResolvers.hasNext()) {
              LOG.error("Could not find matcher for `{}`", o1);
              return 1;
            }

            final Iterator<LocalEntityModel.Resolver> tgtResolvers =
                getMatchingResolvers(o2).iterator();

            if (!tgtResolvers.hasNext()) {
              LOG.error("Could not find matcher for `{}`", o2);
              return -1;
            }

            final LocalEntityModel.Resolver topSrcResolver = srcResolvers.next();
            final LocalEntityModel.Resolver topTgtResolver = tgtResolvers.next();

            if (topSrcResolver.getWeight() == topTgtResolver.getWeight()) {
              return Integer.compare(o1.hashCode(), o2.hashCode());
            }

            return Integer.compare(-topSrcResolver.getWeight(), -topTgtResolver.getWeight());
          });
  private final Set<LocalMasterEntity> masterEntities = Sets.newHashSet();

  // Map from masterable entity identity to master entity and it's members
  private final Map<
          LocalMasterableEntityIdentity, Map<LocalMasterEntity, Set<LocalMasterEntityMembership>>>
      identityToMasterEntity = Maps.newConcurrentMap();

  // Set of masterable entities that are unresolved
  private final Set<LocalMasterableEntityIdentity> unresolved = Sets.newHashSet();

  // Map from entity model attribute to master entities
  private final Map<Pair<LocalEntityModel.Attribute, String>, Set<LocalMasterEntity>>
      masterEntityByAttribute = Maps.newHashMap();

  public LocalMemoryEntityUniverse(EntityType entityType) {
    this.entityType = entityType;
  }

  public static LocalMemoryEntityUniverse of(EntityType entityType) {
    return new LocalMemoryEntityUniverse(entityType);
  }

  @Override
  public synchronized AppendResult append(LocalMasterableEntity e) {

    // get the identity of the masterable entity
    final LocalMasterableEntityIdentity identity = e.getMasterableEntityIdentity();

    // flag to mark whether it is a new identity
    boolean isNew = false;

    // if we don't have it in our cache, it is new.
    if (!entitySnapshot.containsKey(identity)) {
      entitySnapshot.put(identity, Maps.newHashMap());
      isNew = true;
    }

    // apply the mutation and check if it modified the structure of the identity
    final boolean changed = applyMutation(e, identity);

    Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> memberships;

    // if masterable entity has changed or is new
    if (changed || isNew || !identityToMasterEntity.containsKey(identity)) {
      // Mark this entity as dirty
      dirtyIdentities.add(identity);

      // If masterable entity identity has a master entity
      if (identityToMasterEntity.containsKey(identity)) {
        // Dirty all master entity member identities
        dirtyIdentities.addAll(
            identityToMasterEntity.get(identity).keySet().stream()
                .flatMap(x -> x.getMemberIdentities().keySet().stream())
                .collect(Collectors.toSet()));
      }

      // Get all resolvers used by this masterable entity
      final List<LocalEntityModel.Resolver> matchingResolvers =
          Lists.newArrayList(getMatchingResolvers(identity));

      // Apply all resolvers to masterable entity and return its membership
      memberships = applyResolution(identity, entitySnapshot.get(identity), matchingResolvers);

      // If there are master entity members
      if (memberships != null) {
        // Deprecate every master entity for which this entity is a member of
        for (LocalMasterEntity m : memberships.keySet()) {
          deprecateMasterEntity(m);
        }
      }
    }

    // Force entity resolution for any unresolved masterable entities such as those which are dirty
    forcePendingResolutions();

    final Set<DeprecationEntry> resolvedDeprecations = resolveDeprecations();

    return new AppendResult(
        identityToMasterEntity.getOrDefault(identity, null), resolvedDeprecations);
  }

  @Override
  public Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> resolve(ResolveQuery query) {
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> resolution =
        applyResolution(
            null,
            query.getTemporalQueryAttributes(),
            getMatchingResolvers(query.getDescriptors().keySet(), true));

    if (resolution == null || resolution.size() <= 0) {
      return null;
    }

    return resolution;
  }

  // Returns a unique set of master entities
  @Override
  public Set<LocalMasterEntity> getMasterEntities() {
    return masterEntities;
  }

  private boolean applyMutation(
      LocalMasterableEntity masterableEntity,
      LocalMasterableEntityIdentity masterableEntityIdentity) {

    // get the current snapshot of this identity
    final Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> snapshot =
        entitySnapshot.get(masterableEntityIdentity);

    // by default, no changes
    boolean change = false;

    // loop over the descriptors on the masterable entity
    for (Map.Entry<LocalMasterableEntityDescriptor, RangeSet<Long>> entry :
        masterableEntity.getDescriptors().entrySet()) {
      // get the descriptor and the valid ranges
      final LocalMasterableEntityDescriptor masterableEntityDescriptor = entry.getKey();

      // Check if masterable entity descriptor is a valid entity model descriptor
      if(masterableEntityDescriptor.getKey() == null) {
        throw new RuntimeException("Invalid attribute, masterable entity descriptor has no entity model attribute");
      }

      // Get all time periods for when this descriptor effective
      final RangeSet<Long> timestamps = entry.getValue();

      // If the entity already contains a value for this attribute
      if (snapshot.containsKey(masterableEntityDescriptor.getKey())) {
        // if the snapshot entity, has the same descriptor and specific value, then append time periods
        if (snapshot
            .get(masterableEntityDescriptor.getKey())
            .containsKey(masterableEntityDescriptor.getValue())) {

          // get the time period current range set from the entity snapshot
          final RangeSet<Long> current =
              snapshot
                  .get(masterableEntityDescriptor.getKey())
                  .get(masterableEntityDescriptor.getValue());

          // get the original hash code for this descriptor before making any changes
          final int originalHashCode = current.hashCode();

          // add the new time ranges for this entity descriptor
          current.addAll(timestamps);

          // flag true/false whether the descriptor has changed by checking original/current hash codes
          change = change || originalHashCode != current.hashCode();
        } else {
          // if the entity has the descriptor, but not the same specific value, the we add it
          snapshot
              .get(masterableEntityDescriptor.getKey())
              .put(masterableEntityDescriptor.getValue(), timestamps);

          // flag true, because we know that this entity has changed with a changed descriptor value
          change = true;
        }
      } else {
        // if the entity does not have this descriptor at all, then let's create a new descriptor
        snapshot.put(masterableEntityDescriptor.getKey(), Maps.newHashMap());

        // Add the descriptor value and effective timestamps to this entity
        snapshot
            .get(masterableEntityDescriptor.getKey())
            .put(masterableEntityDescriptor.getValue(), timestamps);

        // flag true, because we know that this entity has changed with a new descriptor
        change = true;
      }
    }

    return change;
  }

  private Iterable<LocalEntityModel.Resolver> getMatchingResolvers(
      LocalMasterableEntityIdentity masterableEntityIdentity) {
    return getMatchingResolvers(entitySnapshot.get(masterableEntityIdentity).keySet(), false);
  }

  // Apply resolution to all masterable entities
  private Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> applyResolution(
      LocalMasterableEntityIdentity identity,
      Map<LocalEntityModel.Attribute, Map<String, RangeSet<Long>>> attrs,
      Iterable<LocalEntityModel.Resolver> matchingResolvers) {
    if (attrs.isEmpty()) {
      final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> resolution = Maps.newHashMap();
      final Set<LocalMasterEntity> allMasterEntities =
          masterEntityByAttribute
              .values()
              .parallelStream()
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());

      for (LocalMasterEntity localMasterEntity : allMasterEntities) {
        resolution.put(localMasterEntity, ImmutableSet.of());
      }

      return resolution;
    }

    // Start by iterating over every resolver
    for (LocalEntityModel.Resolver resolver : matchingResolvers) {

      // Store all master entities that have been matched using this resolver
      Set<LocalMasterEntity> currentMatches = null;
      boolean success = true;

      // Iterate over every resolver attribute
      for (LocalEntityModel.Attribute resolverAttribute : resolver.getAttributes()) {

        // If the masterable entity doesn't have this resolver attribute then break
        if (!attrs.containsKey(resolverAttribute)) {
          break;
        }

        // Create a set of candidate master entities
        final Set<LocalMasterEntity> possibleCandidates = Sets.newHashSet();

        // Iterate over every attribute of this masterable entity
        for (Map.Entry<String, RangeSet<Long>> entry : attrs.get(resolverAttribute).entrySet()) {
          // Get the attribute value of the masterable entity
          final Pair<LocalEntityModel.Attribute, String> key = Pair.of(resolverAttribute, entry.getKey());

          // Get master entity who has this exact attribute value and mark it as a candidate
          final Set<LocalMasterEntity> candidates = masterEntityByAttribute.get(key);

          // If any candidates exist
          if (candidates != null && !candidates.isEmpty()) {

            candidates.removeIf(x -> !masterEntities.contains(x));

            possibleCandidates.addAll(candidates);
          }
        }

        // If we cannot find any candidates then break
        if (possibleCandidates.isEmpty()) {
          break;
        }

        // Stores master entity candidates
        final Set<LocalMasterEntity> candidatesWithTemporalOverlap = Sets.newHashSet();

        // Iterate over every possible master entity candidate
        for (final LocalMasterEntity candidate : possibleCandidates) {
          // If descriptor value overlaps with master entity candidate then mark candidate with temporal overlap
          attrs
              .get(resolverAttribute)
              .forEach(
                  (tgtValue, tgtRangeset) -> {
                    if (candidate.getDescriptors().containsKey(resolverAttribute)) {
                      final RangeSet<Long> validRange =
                          candidate.getDescriptors().get(resolverAttribute).getOrDefault(tgtValue, null);
                      if (validRange != null
                          && validRange.asRanges().stream().anyMatch(tgtRangeset::intersects)) {
                        candidatesWithTemporalOverlap.add(candidate);
                      }
                    }
                  });
        }

        // Break if there are no master entity candidates with temporal overlap
        if (candidatesWithTemporalOverlap.size() <= 0) {
          break;
        }

        if (currentMatches == null) {
          currentMatches = candidatesWithTemporalOverlap;
          // If there are no current matches then simply set it equal to the master entity candidates
        } else {
          // Otherwise, update the current matches to the intersection between current matches found and new candidates
          currentMatches = Sets.intersection(currentMatches, candidatesWithTemporalOverlap);
        }

        // If we cannot find any match then set success to false and break
        if (currentMatches.size() <= 0) {
          success = false;
          break;
        }
      }

      // If we're successful and there were matches
      if (success && currentMatches != null) {
        final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> result = Maps.newHashMap();

        // Prepare a result of master entity, their members, and the resolver that got us the match
        for (LocalMasterEntity masterEntity : currentMatches) {
          result.put(
              masterEntity,
              ImmutableSet.of(LocalMasterEntityMembership.of(identity, resolver, false)));
        }

        return result;
      }
    }

    return null;
  }

  private void deprecateMasterEntity(LocalMasterEntity m) {

    // mark the master entity as candidate for deprecation
    candidateForDeprecation.add(m);

    // mark the identities as dirty
    for(final LocalMasterableEntityIdentity identity : m.getMemberIdentities().keySet()) {
      identityToMasterEntity.remove(identity);
      dirtyIdentities.add(identity);
    }

    // remove it from the current list of master entities
    masterEntities.remove(m);

  }

  /**
   *
   */
  private synchronized void forcePendingResolutions() {
    // Iterate over dirty identities
    for (LocalMasterableEntityIdentity dirty : dirtyIdentities) {
      // Apply entity resolution to dirtied masterable entity
      Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> memberships =
          applyResolution(dirty, entitySnapshot.get(dirty), getMatchingResolvers(dirty));

      // The dirtied masterable entity has no master entity so it becomes a new leader
      if (memberships == null) {
        // Create a new master entity from this dirtied entity
        memberships = applyResolutionByCreation(dirty);

        // If there are no members
        if (memberships == null || memberships.size() <= 0) {
          // Add this dirty entity as unresolved
          unresolved.add(dirty);
          continue;
        }
      }

      // Iterate over every master entity member
      memberships.forEach(
          (masterEntity, e) -> {
            // Add the dirty identity to the master entity if it's not currently there
            if (!masterEntity.getMemberIdentities().containsKey(dirty))
              masterEntity.getMemberIdentities().put(dirty, Sets.newHashSet());

            // Add all memberships
            masterEntity.getMemberIdentities().get(dirty).addAll(e);

            // Add the masterable entity identity if it doesn't exist in the identity to master entity pointer
            if (!identityToMasterEntity.containsKey(dirty))
              identityToMasterEntity.put(dirty, Maps.newHashMap());

            // Add the master entity to the pointer if the identity exists but master entity doesn't
            if (!identityToMasterEntity.get(dirty).containsKey(masterEntity))
              identityToMasterEntity.get(dirty).put(masterEntity, Sets.newHashSet());

            // Update the pointer from identity to master entity
            identityToMasterEntity.get(dirty).get(masterEntity).addAll(e);

            // Iterate over every masterable entity descriptor and add it to the master entity
            entitySnapshot
                .get(dirty)
                .forEach(
                    (x, y) -> {
                      if (!masterEntity.getDescriptors().containsKey(x)) {
                        masterEntity.getDescriptors().put(x, Maps.newHashMap());
                      }

                      y.forEach(
                          (key, range) -> {
                            if (!masterEntity.getDescriptors().get(x).containsKey(key)) {
                              masterEntity.getDescriptors().get(x).put(key, TreeRangeSet.create());
                            }
                            masterEntity.getDescriptors().get(x).get(key).addAll(range);

                            final Pair<LocalEntityModel.Attribute, String> k = Pair.of(x, key);
                            if (!masterEntityByAttribute.containsKey(k))
                              masterEntityByAttribute.put(k, Sets.newHashSet());

                            masterEntityByAttribute.get(k).add(masterEntity);
                          });
                    });

            dirty
                .getDescriptorMap()
                .forEach(
                    (x, y) -> {
                      final Pair<LocalEntityModel.Attribute, String> key = Pair.of(x, y);

                      if (!masterEntityByAttribute.containsKey(key))
                        masterEntityByAttribute.put(key, Sets.newHashSet());

                      masterEntityByAttribute.get(key).add(masterEntity);
                    });
          });
    }

    // Clear out all dirty identities
    dirtyIdentities.clear();
  }

  private Set<DeprecationEntry> resolveDeprecations() {

    final Set<DeprecationEntry> deprecations = Sets.newHashSet();

    // remove those candidates whose master entities survived.
    candidateForDeprecation.removeIf(masterEntities::contains);

    // for each candidate for deprecation
    for (final LocalMasterEntity candidate : candidateForDeprecation) {
      deprecations.add(
          new DeprecationEntry(
              candidate,
              findMasterEntityReplacement(candidate),
              DominusApplication.SEQUENCE_ID.getAndIncrement()));
    }

    candidateForDeprecation.clear();

    return deprecations;
  }

  private Iterable<LocalEntityModel.Resolver> getMatchingResolvers(
      Set<LocalEntityModel.Attribute> keys, boolean contains) {
    final List<LocalEntityModel.Resolver> results = new ArrayList<>();

    for (LocalEntityModel.Resolver resolver : entityType.getLocalEntityModel().getResolvers()) {
      if (contains) {
        if (resolver.getAttributes().containsAll(keys)) {
          results.add(resolver);
        }
      } else {
        final Set<LocalEntityModel.Attribute> overlappingAttributes =
            Sets.intersection(resolver.getAttributes(), keys);

        if (overlappingAttributes.size() == resolver.getAttributes().size()) {
          results.add(resolver);
        }
      }
    }

    return results;
  }

  // Apply entity resolution by creating a new master entity from a unresolved masterable entity
  private Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> applyResolutionByCreation(
      LocalMasterableEntityIdentity dirtyIdentity) {

    // Create a new master entity using the entity type and masterable entity identity
    final LocalMasterEntity masterEntity =
        new LocalMasterEntity(
            dirtyIdentity.getEntityType().getName(),
            entitySnapshot.get(dirtyIdentity));

    // Get all resolvers, from the entity model, for the dirty identity
    final List<LocalEntityModel.Resolver> candidateResolvers =
        Lists.newArrayList(getMatchingResolvers(dirtyIdentity));

    // No resolvers for masterable entity, this can happen when theres not enough characteristics for resolvers
    if (candidateResolvers.isEmpty()) {
      return null;
    }

    // Clone the masterable entity descriptor to the master entity
    masterEntity
        .getDescriptors()
        .forEach(
            (x, y) -> y.forEach(
                (attributeValue, rr) -> {
                  final Pair<LocalEntityModel.Attribute, String> key = Pair.of(x, attributeValue);

                  if (!masterEntityByAttribute.containsKey(key)) {
                    masterEntityByAttribute.put(key, Sets.newHashSet());
                  }

                  masterEntityByAttribute.get(key).add(masterEntity);
                }));

    // Add this new master entity to the list of masterable entities in this universe
    masterEntities.add(masterEntity);

    // Return a mapping of master entity and its members (itself)
    return ImmutableMap.of(
        masterEntity,
        ImmutableSet.of(
            LocalMasterEntityMembership.of(
                dirtyIdentity, candidateResolvers.get(0), true)));
  }

  private LocalMasterEntity findMasterEntityReplacement(LocalMasterEntity deprecated) {

    final Set<LocalMasterEntity> candidates =
        Sets.newTreeSet(
            (o1, o2) -> {
              if (o1.getUid().equalsIgnoreCase(o2.getUid())) {
                return 0;
              }

              final int sz1 = o1.getMemberIdentities().size() + o1.getDescriptors().size();
              final int sz2 = o2.getMemberIdentities().size() + o2.getDescriptors().size();

              if (sz1 == sz2) {
                return Integer.compare(o1.hashCode(), o2.hashCode());
              }

              return Integer.compare(-sz1, -sz2);
            });

    deprecated
        .getMemberIdentities()
        .forEach(
            (identity, membership) -> {
              if (identityToMasterEntity.containsKey(identity)) {
                candidates.addAll(identityToMasterEntity.get(identity).keySet());
              }
            });

    if (candidates.isEmpty()) {
      return null;
    }

    final LocalMasterEntity best = candidates.iterator().next();
    if (best.getUid().equalsIgnoreCase(deprecated.getUid())) {
      throw new RuntimeException("this should not happen");
    }

    return best;
  }

  @EqualsAndHashCode
  public static class DeprecationEntry {
    private final LocalMasterEntity old;
    private final LocalMasterEntity current;
    private final long sequenceId;

    DeprecationEntry(LocalMasterEntity old, LocalMasterEntity current, long sequenceId) {
      this.old = old;
      this.current = current;
      this.sequenceId = sequenceId;
    }

    public LocalMasterEntity getOld() {
      return old;
    }

    public LocalMasterEntity getCurrent() {
      return current;
    }

    public long getSequenceId() {
      return sequenceId;
    }
  }

  public static class AppendResult {
    private final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> mapping;
    private final Set<DeprecationEntry> deprecations;

    AppendResult(
        Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> mapping,
        Set<DeprecationEntry> deprecations) {
      this.mapping = mapping;
      this.deprecations = deprecations;
    }

    public Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> getMapping() {
      return mapping;
    }

    public Set<DeprecationEntry> getDeprecations() {
      return deprecations;
    }
  }
}
