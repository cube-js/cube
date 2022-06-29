//! Meta layer information around pg_type

/// A Postgres type. Similar structure as pg_catalog.pg_type.
/// <https://www.postgresql.org/docs/14/catalog-pg-type.html>
#[derive(Debug)]
pub struct PgType<'a> {
    pub oid: u32,
    /// Data type name
    pub typname: &'a str,
    /// The OID of the namespace that contains this type. references pg_namespace.oid
    pub typnamespace: u32,
    /// Owner of the type. references pg_authid.oid
    pub typowner: u32,
    /// For a fixed-size type, typlen is the number of bytes in the internal representation of the type. But for a variable-length type, typlen is negative. -1 indicates a “varlena” type (one that has a length word), -2 indicates a null-terminated C string.
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: &'a str,
    pub typcategory: &'a str,
    pub typisprefered: bool,
    pub typisdefined: bool,
    pub typrelid: u32,
    pub typsubscript: &'static str,
    pub typelem: u32,
    pub typarray: u32,
    pub typalign: &'static str,
    pub typstorage: &'static str,
    pub typbasetype: u32,
    pub typreceive: &'static str,
    pub typreceive_oid: u32,
}

impl<'a> PgType<'a> {
    pub fn get_typinput(&self) -> String {
        if let Some(ty_id) = PgTypeId::from_oid(self.oid) {
            // TODO: It requires additional verification
            match ty_id {
                PgTypeId::ARRAYTEXT
                | PgTypeId::ARRAYINT2
                | PgTypeId::ARRAYINT4
                | PgTypeId::ARRAYINT8
                | PgTypeId::ARRAYFLOAT4
                | PgTypeId::ARRAYFLOAT8
                | PgTypeId::ARRAYBOOL
                | PgTypeId::ARRAYBYTEA => "array_in".to_string(),
                PgTypeId::TIMESTAMP
                | PgTypeId::TIMESTAMPTZ
                | PgTypeId::DATE
                | PgTypeId::TIME
                | PgTypeId::TIMETZ => self.typname.to_owned() + "_in",
                PgTypeId::TSMULTIRANGE
                | PgTypeId::NUMMULTIRANGE
                | PgTypeId::DATEMULTIRANGE
                | PgTypeId::INT4MULTIRANGE
                | PgTypeId::INT8MULTIRANGE => "multirange_in".to_string(),
                PgTypeId::MONEY => "cash_in".to_string(),
                _ => self.typname.to_owned() + "in",
            }
        } else {
            "record_in".to_string()
        }
    }

    pub fn is_binary_supported(&self) -> bool {
        // Right now, We assume that all types have binary encoding support
        true
    }
}

macro_rules! define_pg_types {
    ($($NAME:ident ($OID:expr) { $($KEY:ident: $VALUE:expr,)* },)*) => {
        #[derive(Debug, Clone, Copy)]
        #[repr(u32)]
        pub enum PgTypeId {
            UNSPECIFIED = 0,
            $($NAME = $OID,)*
        }

        impl PgTypeId {
            pub fn from_oid(oid: u32) -> Option<Self> {
                match oid {
                    0 => Some(Self::UNSPECIFIED),
                    $($OID => Some(Self::$NAME),)*
                    _ => None,
                }
            }
        }

        impl<'a> PgType<'a> {
            pub fn get_by_tid(oid: PgTypeId) -> &'static PgType<'static> {
                match oid {
                    PgTypeId::UNSPECIFIED => UNSPECIFIED,
                    $(PgTypeId::$NAME => $NAME,)*
                }
            }

            pub fn get_all() -> Vec<&'static PgType<'static>> {
                vec![
                    $($NAME,)*
                ]
            }
        }

        $(
            const $NAME: &PgType = &PgType {
                oid: PgTypeId::$NAME as u32,
                $($KEY: $VALUE,)*
            };
        )*
    }
}

const UNSPECIFIED: &PgType = &PgType {
    oid: 0,
    typname: "unspecified",
    typnamespace: 11,
    typowner: 10,
    typlen: 1,
    typbyval: true,
    typtype: "b",
    typcategory: "B",
    typisprefered: true,
    typisdefined: true,
    typrelid: 0,
    typsubscript: "-",
    typelem: 0,
    typarray: 0,
    typalign: "-",
    typstorage: "-",
    typbasetype: 0,
    typreceive: "",
    typreceive_oid: 0,
};

define_pg_types![
    BOOL (16) {
        typname: "bool",
        typnamespace: 11,
        typowner: 10,
        typlen: 1,
        typbyval: true,
        typtype: "b",
        typcategory: "B",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "c",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "boolrecv",
        typreceive_oid: 2436,
    },

    BYTEA (17) {
        typname: "bytea",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "U",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "bytearecv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    NAME (19) {
        typname: "name",
        typnamespace: 11,
        typowner: 10,
        typlen: 64,
        typbyval: false,
        typtype: "b",
        typcategory: "S",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "raw_array_subscript_handler",
        typelem: 0,
        typarray: 0,
        typalign: "c",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "namerecv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT8 (20) {
        typname: "int8",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "int8recv",
        typreceive_oid: 2408,
    },

    INT2 (21) {
        typname: "int2",
        typnamespace: 11,
        typowner: 10,
        typlen: 2,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "s",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "int2recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT4 (23) {
        typname: "int4",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "int4recv",
        typreceive_oid: 2406,
    },

    TEXT (25) {
        typname: "text",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "S",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "textrecv",
        typreceive_oid: 2414,
    },

    OID (26) {
        typname: "oid",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1028,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "oidrecv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TID (27) {
        typname: "tid",
        typnamespace: 11,
        typowner: 10,
        typlen: 6,
        typbyval: false,
        typtype: "b",
        typcategory: "U",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1010,
        typalign: "s",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "tidrecv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    PGCLASS (83) {
        typname: "pg_class",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "c",
        typcategory: "C",
        typisprefered: false,
        typisdefined: true,
        typrelid: 1259,
        typsubscript: "-",
        typelem: 0,
        typarray: 273,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "record_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    FLOAT4 (700) {
        typname: "float4",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1021,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "float4recv",
        typreceive_oid: 2424,
    },

    FLOAT8 (701) {
        typname: "float8",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1022,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "float8recv",
        typreceive_oid: 2426,
    },

    MONEY (790) {
        typname: "money",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 791,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "cash_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INET (869) {
        typname: "inet",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "I",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1041,
        typalign: "i",
        typstorage: "m",
        typbasetype: 0,
        typreceive: "inet_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYBOOL (1000) {
        typname: "_bool",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 16,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYBYTEA (1001) {
        typname: "_bytea",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 17,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYINT2 (1005) {
        typname: "_int2",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 21,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYINT4 (1007) {
        typname: "_int4",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 23,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYTEXT (1009) {
        typname: "_text",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 25,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYINT8 (1016) {
        typname: "_int8",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 20,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYFLOAT4 (1021) {
        typname: "_float4",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 700,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYFLOAT8 (1022) {
        typname: "_float8",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 701,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ACLITEM (1033) {
        typname: "aclitem",
        typnamespace: 11,
        typowner: 10,
        typlen: 12,
        typbyval: false,
        typtype: "b",
        typcategory: "U",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1034,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "-",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ARRAYACLITEM (1034) {
        typname: "_aclitem",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "A",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "array_subscript_handler",
        typelem: 1033,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "array_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    BPCHAR (1042) {
        typname: "bpchar",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "S",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1014,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "bpcharrecv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    VARCHAR (1043) {
        typname: "varchar",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "S",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "varcharrecv",
        typreceive_oid: 2432,
    },

    DATE (1082) {
        typname: "date",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "b",
        typcategory: "D",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "date_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TIME (1083) {
        typname: "time",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "D",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1183,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "time_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TIMESTAMP (1114) {
        typname: "timestamp",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "D",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "timestamp_recv",
        typreceive_oid: 2474,
    },

    TIMESTAMPTZ (1184) {
        typname: "timestamptz",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "D",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "timestamptz_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INTERVAL (1186) {
        typname: "interval",
        typnamespace: 11,
        typowner: 10,
        typlen: 16,
        typbyval: false,
        typtype: "b",
        typcategory: "T",
        typisprefered: true,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1187,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "interval_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TIMETZ (1266) {
        typname: "timetz",
        typnamespace: 11,
        typowner: 10,
        typlen: 12,
        typbyval: false,
        typtype: "b",
        typcategory: "D",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 1270,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "timetz_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    NUMERIC (1700) {
        typname: "numeric",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "b",
        typcategory: "N",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "m",
        typbasetype: 0,
        typreceive: "numeric_recv",
        typreceive_oid: 2460,
    },

    RECORD (2249) {
        typname: "record",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "p",
        typcategory: "P",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 2287,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "record_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ANYARRAY (2277) {
        typname: "anyarray",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "p",
        typcategory: "P",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "anyarray_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ANYELEMENT (2283) {
        typname: "anyelement",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "p",
        typcategory: "P",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "-",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT4RANGE (3904) {
        typname: "int4range",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    NUMRANGE (3906) {
        typname: "numrange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TSRANGE (3908) {
        typname: "tsrange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    PGLSN (3220) {
        typname: "pg_lsn",
        typnamespace: 11,
        typowner: 10,
        typlen: 8,
        typbyval: true,
        typtype: "b",
        typcategory: "U",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 3221,
        typalign: "d",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "pg_lsn_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ANYENUM (3500) {
        typname: "anyenum",
        typnamespace: 11,
        typowner: 10,
        typlen: 4,
        typbyval: true,
        typtype: "p",
        typcategory: "P",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "p",
        typbasetype: 0,
        typreceive: "-",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    ANYRANGE (3831) {
        typname: "anyrange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "p",
        typcategory: "P",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "-",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TSTZRANGE (3910) {
        typname: "tstzrange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    DATERANGE (3912) {
        typname: "daterange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT8RANGE (3926) {
        typname: "int8range",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "range_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    NUMMULTIRANGE (4532) {
        typname: "nummultirange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "m",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "multirange_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    TSMULTIRANGE (4533) {
        typname: "tsmultirange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "m",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "multirange_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    DATEMULTIRANGE (4535) {
        typname: "datemultirange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "m",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "multirange_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT8MULTIRANGE (4536) {
        typname: "int8multirange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "m",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "d",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "multirange_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    INT4MULTIRANGE (4451) {
        typname: "int4multirange",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "r",
        typcategory: "R",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 0,
        typreceive: "multirange_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    CHARACTERDATA (13408) {
        typname: "character_data",
        typnamespace: 13000,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "d",
        typcategory: "S",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "i",
        typstorage: "x",
        typbasetype: 1043,
        typreceive: "domain_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    PGNAMESPACE (12047) {
        typname: "pg_namespace",
        typnamespace: 11,
        typowner: 10,
        typlen: -1,
        typbyval: false,
        typtype: "c",
        typcategory: "C",
        typisprefered: false,
        typisdefined: true,
        typrelid: 2615,
        typsubscript: "-",
        typelem: 0,
        typarray: 12046,
        typalign: "c",
        typstorage: "p",
        typbasetype: 19,
        typreceive: "record_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },

    SQLIDENTIFIER (13410) {
        typname: "sql_identifier",
        typnamespace: 13000,
        typowner: 10,
        typlen: 64,
        typbyval: false,
        typtype: "d",
        typcategory: "S",
        typisprefered: false,
        typisdefined: true,
        typrelid: 0,
        typsubscript: "-",
        typelem: 0,
        typarray: 0,
        typalign: "c",
        typstorage: "p",
        typbasetype: 19,
        typreceive: "domain_recv",
        // TODO: Get from pg_proc
        typreceive_oid: 0,
    },
];

impl PgTypeId {
    pub fn to_type(self) -> &'static PgType<'static> {
        PgType::get_by_tid(self)
    }
}
