package io.openaristos.dominus.core;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class LocalMasterEntityMembership {

  private final boolean resolvedByCreation;
  private LocalMasterableEntityIdentity identity;
  private LocalEntityModel.Resolver resolver;

  private LocalMasterEntityMembership(
      LocalMasterableEntityIdentity identity,
      LocalEntityModel.Resolver resolver,
      boolean resolvedByCreation) {
    this.identity = identity;
    this.resolver = resolver;
    this.resolvedByCreation = resolvedByCreation;
  }

  public static LocalMasterEntityMembership of(
      LocalMasterableEntityIdentity identity,
      LocalEntityModel.Resolver resolver,
      boolean resolvedByCreation) {
    return new LocalMasterEntityMembership(identity, resolver, resolvedByCreation);
  }

  public LocalMasterableEntityIdentity getIdentity() {
    return identity;
  }

  public void setIdentity(LocalMasterableEntityIdentity identity) {
    this.identity = identity;
  }

  public LocalEntityModel.Resolver getResolver() {
    return resolver;
  }

  public void setResolver(LocalEntityModel.Resolver resolver) {
    this.resolver = resolver;
  }

  public boolean isResolvedByCreation() {
    return resolvedByCreation;
  }
}
