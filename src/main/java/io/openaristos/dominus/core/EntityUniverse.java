package io.openaristos.dominus.core;

import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;

import java.util.Map;
import java.util.Set;

public interface EntityUniverse {
  LocalMemoryEntityUniverse.AppendResult append(LocalMasterableEntity masterableEntity);
  Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> resolve(ResolveQuery resolveQuery);
  Set<LocalMasterEntity> getMasterEntities();
}
