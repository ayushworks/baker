package com.ing.baker.runtime

import com.ing.baker.recipe.scaladsl.{Event, Ingredient, Interaction}
import com.ing.baker.runtime.scaladsl.InteractionImplementation
import com.ing.baker.runtime.scaladsl.RuntimeEvent
import com.ing.baker.types.Converters.toJava
import com.ing.baker.types.{Converters, Value}

import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag

/**
  *  This class is for wiring the scala DSL to the runtime components (interaction implementations).
  *
  */
object ScalaDSLRuntime {

  object ScalaInteractionImplementation {
    def apply(i: Interaction, fn: Map[String, Value] => RuntimeEvent): InteractionImplementation = {
      InteractionImplementation(
        name = i.name,
        inputIngredients = i.inputIngredients.map(x => x.name -> x.ingredientType).toMap,
        run = input => Future.successful(Some(fn(input)))
      )
    }
  }

  // TODO use shapeless to abstract over function arity and add type safety
  implicit class InteractionOps(i: Interaction) {

    def implement[A : TypeTag](fn: A => RuntimeEvent): InteractionImplementation =
      ScalaInteractionImplementation(i, { input =>
        fn(toJava[A](input.values.head))
      })

    def implement[A : TypeTag, B : TypeTag](fn: (A, B) => RuntimeEvent): InteractionImplementation =
      ScalaInteractionImplementation(i, { input =>
        fn(toJava[A](input.values.head), toJava[B](input.values.toSeq(1)))
      })

    def implement[A : TypeTag, B : TypeTag, C : TypeTag](fn: (A, B, C) => RuntimeEvent): InteractionImplementation =
      ScalaInteractionImplementation(i, { input =>
        fn(toJava[A](input.values.head), toJava[B](input.values.toSeq(1)), toJava[C](input.values.toSeq(2)))
      })
  }

  implicit class EventOps(e: Event) {
    def instance(values: Any*): RuntimeEvent = {

      val providedIngredients: Map[String, Value] =
        e.providedIngredients.map(_.name).zip(values.toSeq.map(Converters.toValue)).toMap

      RuntimeEvent(e.name, providedIngredients)
    }
  }

  implicit object IngredientMap {

    def apply(values: (Ingredient[_], Any)*): Map[String, Value] = {
      values.map { case (key, value) => key.name -> Converters.toValue(value)
      }.toMap
    }
  }
}
