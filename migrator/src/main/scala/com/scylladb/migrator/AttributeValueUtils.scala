package com.scylladb.migrator

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import software.amazon.awssdk.core.SdkBytes.fromByteBuffer
import software.amazon.awssdk.services.dynamodb.model

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def binaryValue(bytes: Array[Byte]): AttributeValue =
    binaryValue(ByteBuffer.wrap(bytes))

  def binaryValue(byteBuffer: ByteBuffer): AttributeValue =
    new AttributeValue().withB(byteBuffer)

  def binaryValues(byteBuffers: ByteBuffer*): AttributeValue =
    new AttributeValue().withBS(byteBuffers: _*)

  def stringValue(value: String): AttributeValue =
    new AttributeValue().withS(value)

  def stringValues(values: String*): AttributeValue =
    new AttributeValue().withSS(values: _*)

  def numericalValue(value: String): AttributeValue =
    new AttributeValue().withN(value)

  def numericalValues(values: String*): AttributeValue =
    new AttributeValue().withNS(values: _*)

  def boolValue(value: Boolean): AttributeValue =
    new AttributeValue().withBOOL(value)

  def listValue(items: AttributeValue*): AttributeValue =
    new AttributeValue().withL(items: _*)

  def mapValue(items: (String, AttributeValue)*): AttributeValue =
    new AttributeValue().withM(items.toMap.asJava)

  def mapValue(items: Map[String, AttributeValue]): AttributeValue =
    new AttributeValue().withM(items.asJava)

  val nullValue: AttributeValue =
    new AttributeValue().withNULL(true)

  def mapV1AttributeValueToV2(v: AttributeValue): model.AttributeValue =
    model.AttributeValue.builder()
      .s(v.getS)
      .n(v.getN)
      .b(fromByteBuffer(v.getB))
      .ss(v.getSS)
      .ns(v.getNS)
      .bs(v.getBS.asScala.map(fromByteBuffer).asJava)
      .m(v.getM.asScala.toMap.transform((_, v) => mapV1AttributeValueToV2(v)).asJava)
      .l(v.getL.asScala.map(v => mapV1AttributeValueToV2(v)): _*)
      .bool(v.getBOOL)
      .nul(v.getNULL)
      .build()

  def mapV2AttributeValueToV1(v: model.AttributeValue): AttributeValue =
    new AttributeValue()
      .withS(v.s)
      .withN(v.n)
      .withB(v.b.asByteBuffer())
      .withSS(v.ss)
      .withNS(v.ns)
      .withBS(v.bs.asScala.map(_.asByteBuffer()).asJava)
      .withM(v.m.asScala.toMap.transform((_, v) => mapV2AttributeValueToV1(v)).asJava)
      .withL(v.l.asScala.map(v => mapV2AttributeValueToV1(v)): _*)
      .withBOOL(v.bool)
      .withNULL(v.nul)

  def boolValueV2(value: Boolean): model.AttributeValue =
    model.AttributeValue.builder().bool(value).build()

}
