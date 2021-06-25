export enum MySQLType {
  DECIMAL = 0x00,
  TINY = 0x01,
  SHORT = 0x02,
  LONG = 0x03,
  FLOAT = 0x04,
  DOUBLE = 0x05,
  NULL = 0x06,
  TIMESTAMP = 0x07,
  LONGLONG = 0x08,
  INT24 = 0x09,
  DATE = 0x0a,
  TIME = 0x0b,
  DATETIME = 0x0c,
  YEAR = 0x0d,
  NEWDATE = 0x0e,
  VARCHAR = 0x0f,
  BIT = 0x10,
  JSON = 0xf5,
  NEWDECIMAL = 0xf6,
  ENUM = 0xf7,
  SET = 0xf8,
  TINY_BLOB = 0xf9,
  MEDIUM_BLOB = 0xfa,
  LONG_BLOB = 0xfb,
  BLOB = 0xfc,
  VAR_STRING = 0xfd,
  STRING = 0xfe,
  GEOMETRY = 0xff,
}

/**
 * MySQL Native types -> SQL type
 */
const MySqlNativeToMySqlType: Record<number, string> = {
  [MySQLType.DECIMAL]: 'decimal',
  [MySQLType.NEWDECIMAL]: 'decimal',
  [MySQLType.TINY]: 'tinyint',
  [MySQLType.SHORT]: 'smallint',
  [MySQLType.LONG]: 'int',
  [MySQLType.INT24]: 'mediumint',
  [MySQLType.LONGLONG]: 'bigint',
  [MySQLType.NEWDATE]: 'datetime',
  [MySQLType.TIMESTAMP]: 'timestamp',
  [MySQLType.DATETIME]: 'datetime',
  [MySQLType.TIME]: 'time',
  [MySQLType.TINY_BLOB]: 'tinytext',
  [MySQLType.MEDIUM_BLOB]: 'mediumtext',
  [MySQLType.LONG_BLOB]: 'longtext',
  [MySQLType.BLOB]: 'text',
  [MySQLType.VAR_STRING]: 'varchar',
  [MySQLType.STRING]: 'binary',
};

export function getNativeTypeName(typeId: MySQLType) {
  if (typeId in MySqlNativeToMySqlType) {
    return MySqlNativeToMySqlType[typeId];
  }

  throw new Error(
    `Unsupported mapping for data type: ${typeId}`
  );
}
