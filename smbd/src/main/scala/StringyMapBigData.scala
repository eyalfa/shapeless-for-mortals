// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, labelled.{ field, FieldType }

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, AnyRef]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): AnyRef
    def fromValue(v: AnyRef): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  // EXERCISE 1.1 goes here
  implicit def hNilBigDataFormat = new BigDataFormat[HNil] {
    override def label: String = "HNil"

    override def toProperties(t: HNil): StringyMap = new StringyMap

    override def fromProperties(m: StringyMap): BigResult[HNil] = Right(HNil)
  }
  implicit def hListBigDataFormat[Key <: Symbol, Value, Remaining <: HList](
    implicit
    witness : Witness.Aux[Key],
    lhd : Lazy[SPrimitive[Value]],
    lrm : Lazy[BigDataFormat[Remaining]])={
    val hd = lhd.value
    val rm = lrm.value
    new BigDataFormat[FieldType[Key,Value] :: Remaining]{
      override def label: String = s"${witness.value.name}::${rm.label}"

      override def toProperties(t: FieldType[Key, Value] :: Remaining ): StringyMap = {
        val res = rm.toProperties( t.tail )
        val headVal = hd.toValue(t.head)
        res.put(witness.value.name, headVal)
        res
      }

      override def fromProperties(m: StringyMap): BigResult[FieldType[Key, Value] :: Remaining] = {
        val hE = m.get(witness.value.name) match{
          case null => Left(s"missing key ${witness.value.name}")
          case v => Right(v)
        }
        hE.right.flatMap{ h =>
          val hh = hd.fromValue(h)
          rm.fromProperties(m).right.map{ tl =>
            val hf = field[Key]( hh )
            val res = hf :: tl
            res
          }
        }
      }
    }
  }
  implicit def cNilBigDataFormat = new BigDataFormat[CNil] {
    override def label: String = "CNil"

    override def toProperties(t: CNil): StringyMap = new StringyMap

    override def fromProperties(m: StringyMap): BigResult[CNil] = Left("CNil in abstract")
  }

  implicit def coproductBigDataFormat[Key <: Symbol, Value, Remaining <: Coproduct](
  implicit
  witness : Witness.Aux[Key],
  lhd : Lazy[SPrimitive[Value]],
  lrm : Lazy[BigDataFormat[Remaining]])={
    val hd = lhd.value
    val rm = lrm.value
    new BigDataFormat[FieldType[Key,Value] :+: Remaining]{
      override def label: String = s"${witness.value.name}:+:${rm.label}"

      override def toProperties(t: FieldType[Key, Value] :+: Remaining ): StringyMap = {
        t match{
          case Inl(x) =>
            val res = new StringyMap
            res.put( witness.value.name, hd.toValue(x))
            res
          case Inr(tl) => rm.toProperties(tl)
        }
      }

      override def fromProperties(m: StringyMap): BigResult[FieldType[Key, Value] :+: Remaining] = {
        m.get(witness.value.name) match{
          case null =>
            val res = rm.fromProperties(m)
            res.right.map( Inr.apply )
          case v =>
            val h1 = hd.fromValue(v)
            val h1f = field[Key](h1)
            Right(Inl(h1f))
        }
      }
    }
  }

  implicit def anyRefPrimitive[T <: AnyRef]( implicit bigDataFormat: Lazy[BigDataFormat[T]]) = new SPrimitive[T] {
    override def toValue(v: T): AnyRef = bigDataFormat.value.toProperties(v)

    override def fromValue(v: AnyRef): T = bigDataFormat.value.fromProperties(v.asInstanceOf[StringyMap]).right.get
  }

  implicit def familyBigDataFormat[T, Repr](
    implicit
    lg : LabelledGeneric.Aux[T,Repr],
    lrf : Lazy[BigDataFormat[Repr]],
    tp : Typeable[T]) = {
    val rf = lrf.value
    new BigDataFormat[T] {
      override def label: String = tp.describe

      override def toProperties(t: T): StringyMap = rf.toProperties( lg.to(t) )

      override def fromProperties(m: StringyMap): BigResult[T] = rf.fromProperties(m).right.map( lg.from )
    }
  }

  implicit object IntPrimitive extends SPrimitive[Int]{
    override def toValue(v: Int): AnyRef = java.lang.Integer.valueOf(v)

    override def fromValue(v: AnyRef): Int = v.asInstanceOf[java.lang.Integer].intValue()
  }

  implicit object StringPrimitive extends SPrimitive[String]{
    override def toValue(v: String): AnyRef = v

    override def fromValue(v: AnyRef): String = v.asInstanceOf[String]
  }

  implicit object BooleanPrimitive extends SPrimitive[Boolean] {
    override def toValue(v: Boolean): AnyRef = java.lang.Boolean.valueOf(v)

    override def fromValue(v: AnyRef): Boolean = v.asInstanceOf[java.lang.Boolean].booleanValue()
  }

  implicit object DoublePrimitive extends SPrimitive[Double]{
    override def toValue(v: Double): AnyRef = java.lang.Double.valueOf(v)

    override def fromValue(v: AnyRef): Double = v.asInstanceOf[java.lang.Double].doubleValue()
  }
}

package impl {
  //import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
