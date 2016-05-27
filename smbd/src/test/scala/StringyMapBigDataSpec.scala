// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
package s4m.exercise1

import org.scalatest._
import shapeless._
import s4m.smbd.api._
import s4m.smbd.impl._
import s4m.smbd.syntax._
import shapeless.labelled._

package api {
  sealed trait Receptacle
  case class Teapot(a: String, b: Int, c: Boolean) extends Receptacle
  case class Bottle(foo: String, bar: Double) extends Receptacle
  case object Glass extends Receptacle

  class Beans
  case class Coffee(z: Beans)
}

package object formats {
  import s4m.exercise1.api._

  implicit val TeapotF: BigDataFormat[Teapot] = cachedImplicit
  implicitly[BigDataFormat[Bottle]]
  implicitly[BigDataFormat[Glass.type]]
  implicitly[BigDataFormat[CNil]]
  val f1 = 'a
  type aT1 = f1.type

  val f2 = 'b
  type aT2 = f1.type

  type h1 = FieldType[aT1,String] :: HNil
  type h2 = FieldType[aT2,String] :: HNil
  type cp1 = FieldType[aT1,h1] :+: FieldType[aT2,h2] :+: CNil


  implicitly[BigDataFormat[cp1]]
  implicitly[LabelledGeneric[Bottle]]
  implicitly[LabelledGeneric[Receptacle]]

  // if you get compiler errors for ReceptacleF, uncomment these to
  // help find which implicit is not being found:
  //implicit val BottleF: BigDataFormat[Bottle] = cachedImplicit
  //implicit val GlassF: BigDataFormat[Glass.type] = cachedImplicit

  implicit val ReceptacleF: BigDataFormat[Receptacle] = cachedImplicit
}

package app {
  import s4m.exercise1.api._
  import s4m.exercise1.formats._

  class StringyMapBigDataSpec extends FlatSpec with Matchers {
    val teapot = Teapot("foo", 42, true)
    val bottle = Bottle("hello", 1.23)
    val glass = Glass

    "StringyMapBigData" should "marshall a case class" in {
      val stringyMap = teapot.toProperties
      stringyMap.get("a") shouldBe "foo"
      stringyMap.get("b") shouldBe 42
      stringyMap.get("c") shouldBe true
    }

    it should "unmarshall to a case class" in {
      val stringyMap = new StringyMap()
      stringyMap.put("a", "foo")
      stringyMap.put("b", new java.lang.Integer(42))
      stringyMap.put("c", new java.lang.Boolean(true))

      stringyMap.as[Teapot] shouldBe teapot
    }

    it should "correctly fail to unmarshall to a case class" in {
      val stringyMap = new StringyMap()
      stringyMap.put("z", "foo")

      TeapotF.fromProperties(stringyMap) shouldBe Left("missing key a")
    }

    it should "round trip sealed traits" in {
      (teapot: Receptacle).toProperties.as[Receptacle] shouldBe teapot
      (bottle: Receptacle).toProperties.as[Receptacle] shouldBe bottle
      (glass: Receptacle).toProperties.as[Receptacle] shouldBe glass
    }

    // uncomment this when the above succed
    it should "fail to marshall a case class with an unsupported value" in {
      // shapeless.test.illTyped(
      //   """implicitly[BigDataFormat[Coffee]]""",
      //   ".*could not find implicit value for evidence parameter of type.*"
      // )
    }

  }
}
