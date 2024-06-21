package com.scylladb.migrator


import software.amazon.awssdk.core.SdkBytes.fromByteBuffer
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.nio.ByteBuffer
import scala.collection.JavaConverters._

/** Convenient factories to create `AttributeValue` objects */
object AttributeValueUtils {

  def binaryValue(bytes: Array[Byte]): AttributeValue =
    binaryValue(ByteBuffer.wrap(bytes))

  def binaryValue(byteBuffer: ByteBuffer): AttributeValue =
    new AttributeValue().toBuilder.b(fromByteBuffer(byteBuffer)).build()

  def binaryValues(byteBuffers: ByteBuffer*): AttributeValue = {
    new AttributeValue().toBuilder.bs(byteBuffers.map(fromByteBuffer).asJava).build()
  }

  def stringValue(value: String): AttributeValue =
    new AttributeValue().toBuilder.s(value).build()

  def stringValues(values: String*): AttributeValue =
    new AttributeValue().toBuilder.ss(values.asJava).build()

  def numericalValue(value: String): AttributeValue =
    new AttributeValue().toBuilder.n(value).build()

  def numericalValues(values: String*): AttributeValue =
    new AttributeValue().toBuilder.ns(values: _*).build()

  def boolValue(value: Boolean): AttributeValue =
    new AttributeValue().toBuilder.bool(value).build()

  def listValue(items: AttributeValue*): AttributeValue =
    new AttributeValue().toBuilder.l(items: _*).build()

  def mapValue(items: (String, AttributeValue)*): AttributeValue =
    new AttributeValue().toBuilder.m(items.toMap.asJava).build()

  def mapValue(items: Map[String, AttributeValue]): AttributeValue =
    new AttributeValue().toBuilder.m(items.asJava).build()

  val nullValue: AttributeValue =
    new AttributeValue().toBuilder.nul(true).build()

}
