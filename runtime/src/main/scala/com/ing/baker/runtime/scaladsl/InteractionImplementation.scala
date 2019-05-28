package com.ing.baker.runtime.scaladsl

import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture

import com.ing.baker.runtime.javadsl
import com.ing.baker.runtime.common
import com.ing.baker.runtime.common.LanguageDataStructures.ScalaApi
import com.ing.baker.types.{Type, Value}

import scala.concurrent.Future

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters

case class InteractionImplementation(
    name: String,
    inputIngredients: Map[String, Type],
    run: Map[String, Value] => Future[Option[RuntimeEvent]]
  ) extends common.InteractionImplementation[Future] with ScalaApi { self =>

  override type Event = RuntimeEvent

  def execute(input: Map[String, Value]): Future[Option[Event]] =
    run(input)

  def asJava: javadsl.InteractionImplementation =
    new javadsl.InteractionImplementation {
      override val name: String =
        self.name
      override val inputIngredients: util.Map[String, Type] =
        self.inputIngredients.asJava
      override def execute(input: util.Map[String, Value]): CompletableFuture[Optional[javadsl.RuntimeEvent]] =
        FutureConverters
          .toJava(self.run(input.asScala.toMap))
          .toCompletableFuture
          .thenApply(_.fold(Optional.empty[javadsl.RuntimeEvent]())(e => Optional.of(e.asJava)))
    }
}

object InteractionImplementation {

  def unsafeFrom(implementation: AnyRef): InteractionImplementation = ???
}
