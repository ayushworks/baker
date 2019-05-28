package com.ing.baker.runtime.common

import com.ing.baker.runtime.common.LanguageDataStructures.LanguageApi
import com.ing.baker.types.{Type, Value}

/**
  * Interface used to provide an implementation for an interaction to a runtime.
  */
trait InteractionImplementation[F[_]] extends LanguageApi { self =>

  type Event <: RuntimeEvent { type Language = self.Language }

  def name: String

  def inputIngredients: language.Map[String, Type]

  /**
    * Executes the interaction.
    *
    * @param input ingredients required to compute the actual interaction
    * @return a runtime event with provided ingredients
    */
  def execute(input: language.Map[String, Value]): F[language.Option[Event]]
}
