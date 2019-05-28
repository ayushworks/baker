package com.ing.baker.runtime.javadsl

import java.util.concurrent.CompletableFuture

import com.ing.baker.runtime.common.SensoryEventStatus
import com.ing.baker.runtime.common
import com.ing.baker.runtime.common.LanguageDataStructures.JavaApi

class SensoryEventReactions(
  override val received: CompletableFuture[SensoryEventStatus],
  override val completed: CompletableFuture[SensoryEventResult]
) extends common.SensoryEventReactions[CompletableFuture] with JavaApi {

  type Result = SensoryEventResult
}
