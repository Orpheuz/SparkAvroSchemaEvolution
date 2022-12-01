package confluent.converter.avro

object MetadataKeys {
  val AVRO_TYPE: String = "avroType"
  val DOC: String = "doc"
  val DEFAULT: String = "default"
  val ENUM_DEFAULT: String = "enumDefault"
  val ENUM_SYMBOLS: String = "symbols"
  val NULLABLE: String = "nullable"

  val RESERVED_SCHEMA_KEYS: List[String] = List(
    AVRO_TYPE,
    DEFAULT,
    DOC,
    ENUM_DEFAULT,
    ENUM_SYMBOLS,
    NULLABLE,
    "fields",
    "items",
    "name",
    "namespace",
    "size",
    "values",
    "type",
    "aliases"
  )
}
