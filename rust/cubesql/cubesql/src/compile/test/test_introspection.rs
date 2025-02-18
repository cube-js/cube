use crate::{
    compile::{
        test::{execute_query, init_testing_logger},
        DatabaseProtocol,
    },
    CubeError,
};

#[tokio::test]
async fn test_tableau() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "tableau_table_name_column_name_query",
        execute_query(
            "SELECT `table_name`, `column_name`
                FROM `information_schema`.`columns`
                WHERE `data_type`='enum' AND `table_schema`='db'"
                .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "tableau_null_text_query",
        execute_query(
            "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'p' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    (pkic.relkind = 'i' OR pkic.relkind = 'I') AND
                    pkic.oid = con.conindid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'payment'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "tableau_table_cat_query",
        execute_query(
            "
                SELECT
                    result.TABLE_CAT,
                    result.TABLE_SCHEM,
                    result.TABLE_NAME,
                    result.COLUMN_NAME,
                    result.KEY_SEQ,
                    result.PK_NAME
                FROM
                    (
                        SELECT
                            NULL AS TABLE_CAT,
                            n.nspname AS TABLE_SCHEM,
                            ct.relname AS TABLE_NAME,
                            a.attname AS COLUMN_NAME,
                            (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ,
                            ci.relname AS PK_NAME,
                            information_schema._pg_expandarray(i.indkey) AS KEYS,
                            a.attnum AS A_ATTNUM
                        FROM pg_catalog.pg_class ct
                        JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                        JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                        JOIN pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                        JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE
                            true AND
                            n.nspname = 'public' AND
                            ct.relname = 'payment' AND
                            i.indisprimary
                    ) result
                    where result.A_ATTNUM = (result.KEYS).x
                ORDER BY
                    result.table_name,
                    result.pk_name,
                    result.key_seq;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_excel() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "excel_select_db_query",
        execute_query(
            "
                SELECT
                    'db' as Database,
                    ns.nspname as Schema,
                    relname as Name,
                    CASE
                        WHEN ns.nspname Like E'pg\\_catalog' then 'Catalog'
                        WHEN ns.nspname Like E'information\\_schema' then 'Information'
                        WHEN relkind = 'f' then 'Foreign'
                        ELSE 'User'
                    END as TableType,
                    pg_get_userbyid(relowner) AS definer,
                    rel.oid as Oid,
                    relacl as ACL,
                    true as HasOids,
                    relhassubclass as HasSubtables,
                    reltuples as RowNumber,
                    description as Comment,
                    relnatts as ColumnNumber,
                    relhastriggers as TriggersNumber,
                    conname as Constraint,
                    conkey as ColumnConstrainsIndexes
                FROM pg_class rel
                INNER JOIN pg_namespace ns ON relnamespace = ns.oid
                LEFT OUTER JOIN pg_description des ON
                    des.objoid = rel.oid AND
                    des.objsubid = 0
                LEFT OUTER JOIN pg_constraint c ON
                    c.conrelid = rel.oid AND
                    c.contype = 'p'
                WHERE
                    (
                        (relkind = 'r') OR
                        (relkind = 's') OR
                        (relkind = 'f')
                    ) AND
                    NOT ns.nspname LIKE E'pg\\_temp\\_%%' AND
                    NOT ns.nspname like E'pg\\_%' AND
                    NOT ns.nspname like E'information\\_schema' AND
                    ns.nspname::varchar like E'public' AND
                    relname::varchar like '%' AND
                    pg_get_userbyid(relowner)::varchar like '%'
                ORDER BY relname
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "excel_typname_big_query",
        execute_query(
            "
                SELECT
                    typname as name,
                    n.nspname as Schema,
                    pg_get_userbyid(typowner) as Definer,
                    typlen as Length,
                    t.oid as oid,
                    typbyval as IsReferenceType,
                    case
                        when typtype = 'b' then 'base'
                        when typtype = 'd' then 'domain'
                        when typtype = 'c' then 'composite'
                        when typtype = 'd' then 'pseudo'
                    end as Type,
                    case
                        when typalign = 'c' then 'char'
                        when typalign = 's' then 'short'
                        when typalign = 'i' then 'int'
                        else 'double'
                    end as alignment,
                    case
                        when typstorage = 'p' then 'plain'
                        when typstorage = 'e' then 'secondary'
                        when typstorage = 'm' then 'compressed inline'
                        else 'secondary or compressed inline'
                    end as ValueStorage,
                    typdefault as DefaultValue,
                    description as comment
                FROM pg_type t
                LEFT OUTER JOIN
                    pg_description des ON des.objoid = t.oid,
                    pg_namespace n
                WHERE
                    t.typnamespace = n.oid and
                    t.oid::varchar like E'1033' and
                    typname like E'%' and
                    n.nspname like E'%' and
                    pg_get_userbyid(typowner)::varchar like E'%' and
                    typtype::varchar like E'c'
                ORDER BY name
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "excel_typname_aclitem_query",
        execute_query(
            "
                SELECT
                    typname as name,
                    t.oid as oid,
                    typtype as Type,
                    typelem as TypeElement
                FROM pg_type t
                WHERE
                    t.oid::varchar like '1034' and
                    typtype::varchar like 'b' and
                    typelem != 0
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "excel_pg_constraint_query",
        execute_query(
            "
                SELECT
                    a.conname as Name,
                    ns.nspname as Schema,
                    mycl.relname as Table,
                    b.conname as ReferencedKey,
                    frns.nspname as ReferencedSchema,
                    frcl.relname as ReferencedTable,
                    a.oid as Oid,
                    a.conkey as ColumnIndexes,
                    a.confkey as ForeignColumnIndexes,
                    a.confupdtype as UpdateActionCode,
                    a.confdeltype as DeleteActionCode,
                    a.confmatchtype as ForeignKeyMatchType,
                    a.condeferrable as IsDeferrable,
                    a.condeferred as Iscondeferred
                FROM pg_constraint a
                inner join pg_constraint b on (
                    a.confrelid = b.conrelid AND
                    a.confkey = b.conkey
                )
                INNER JOIN pg_namespace ns ON a.connamespace = ns.oid
                INNER JOIN pg_class mycl ON a.conrelid = mycl.oid
                LEFT OUTER JOIN pg_class frcl ON a.confrelid = frcl.oid
                INNER JOIN pg_namespace frns ON frcl.relnamespace = frns.oid
                WHERE
                    a.contype = 'f' AND
                    (
                        b.contype = 'p' OR
                        b.contype = 'u'
                    ) AND
                    a.oid::varchar like '%' AND
                    a.conname like '%' AND
                    ns.nspname like E'public' AND
                    mycl.relname like E'KibanaSampleDataEcommerce' AND
                    frns.nspname like '%' AND
                    frcl.relname like '%'
                ORDER BY 1
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "excel_pg_attribute_query",
        execute_query(
            "
                SELECT DISTINCT
                    attname AS Name,
                    attnum
                FROM pg_attribute
                JOIN pg_class ON oid = attrelid
                INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    pg_namespace.nspname like 'public' AND
                    relname like 'KibanaSampleDataEcommerce' AND
                    attnum in (2)
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "excel_fkey_query",
        execute_query(
            "
                SELECT
                    nspname as Schema,
                    cl.relname as Table,
                    clr.relname as RefTableName,
                    conname as Name,
                    conkey as ColumnIndexes,
                    confkey as ColumnRefIndexes
                FROM pg_constraint
                INNER JOIN pg_namespace ON connamespace = pg_namespace.oid
                INNER JOIN pg_class cl ON conrelid = cl.oid
                INNER JOIN pg_class clr ON confrelid = clr.oid
                WHERE
                    contype = 'f' AND
                    conname like E'sample\\_fkey' AND
                    nspname like E'public' AND
                    cl.relname like E'KibanaSampleDataEcommerce'
                order by 1
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "excel_large_select_query",
            execute_query(
                "
                SELECT
                    na.nspname as Schema,
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN ty.typname Like 'interval' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(information_schema._pg_truetypid(att.*, ty.*), information_schema._pg_truetypmod(att.*, ty.*)))::information_schema.cardinal_number AS DatetimeLength,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'false'
                        ELSE 'true'
                    END as IsUnique,
                    att.atthasdef as HasDefaultValue,
                    att.attisdropped as IsDropped,
                    att.attinhcount as ancestorCount,
                    att.attndims as Dimension,
                    CASE
                        WHEN attndims > 0 THEN true
                        ELSE false
                    END AS isarray,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    tn.nspname as TypeSchema,
                    et.typname as elementaltypename,
                    description as Comment,
                    cs.relname AS sername,
                    ns.nspname AS serschema,
                    att.attidentity as IdentityMode,
                    CAST(pg_get_expr(def.adbin, def.adrelid) AS varchar) as DefaultValue,
                    (SELECT count(1) FROM pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid=atttypid
                JOIN pg_namespace tn ON tn.oid=ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid=attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid=cl.relnamespace
                LEFT OUTER JOIN pg_type et ON et.oid=ty.typelem
                LEFT OUTER JOIN pg_attrdef def ON
                    adrelid=attrelid AND
                    adnum=attnum
                LEFT OUTER JOIN pg_description des ON
                    des.objoid=attrelid AND
                    des.objsubid=attnum
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid=cs.oid AND
                        cs.relkind='S' AND
                        classid='pg_class'::regclass::oid
                ) ON
                    refobjid=attrelid AND
                    refobjsubid=attnum
                LEFT OUTER JOIN pg_namespace ns ON ns.oid=cs.relnamespace
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    cl.relname like E'KibanaSampleDataEcommerce' AND
                    na.nspname like E'public' AND
                    att.attname like '%'
                ORDER BY attnum
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "excel_exists_query",
        execute_query(
            "
                SELECT
                    a.attname as fieldname,
                    a.attnum  as fieldordinal,
                    a.atttypid as datatype,
                    a.atttypmod as fieldmod,
                    a.attnotnull as isnull,
                    c.relname as tablename,
                    n.nspname as schema,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'p' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as iskey,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c1
                            where
                                c1.conrelid = c.oid and
                                c1.contype = 'u' and
                                a.attnum = ANY (c1.conkey)
                        ) THEN true
                        ELSE false
                    END as isunique,
                    CAST(pg_get_expr(d.adbin, d.adrelid) AS varchar) as defvalue,
                    CASE
                        WHEN t.typtype = 'd' THEN t.typbasetype
                        ELSE a.atttypid
                    END as basetype,
                    CASE
                        WHEN a.attidentity = 'a' THEN true
                        ELSE false
                    END as IsAutoIncrement,
                    CASE
                        WHEN
                            t.typname Like 'bit' OR
                            t.typname Like 'varbit' and
                            a.atttypmod > 0
                        THEN a.atttypmod
                        WHEN
                            t.typname Like 'interval' OR
                            t.typname Like 'timestamp' OR
                            t.typname Like 'timestamptz' OR
                            t.typname Like 'time' OR
                            t.typname Like 'timetz'
                        THEN -1
                        WHEN a.atttypmod > 0 THEN a.atttypmod - 4
                        ELSE a.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(a .*, t.*),
                        information_schema._pg_truetypmod(a .*, t.*)
                    ))::information_schema.cardinal_number AS DatetimePrecision
                FROM pg_namespace n
                INNER JOIN pg_class c ON c.relnamespace = n.oid
                INNER JOIN pg_attribute a on c.oid = a.attrelid
                LEFT JOIN pg_attrdef d on
                    d.adrelid = a.attrelid and
                    d.adnum =a.attnum
                LEFT JOIN pg_type t on t.oid = a.atttypid
                WHERE
                    a.attisdropped = false AND
                    (
                        (c.relkind = 'r') OR
                        (c.relkind = 's') OR
                        (c.relkind = 'v') OR
                        (c.relkind = 'm') OR
                        (c.relkind = 'f')
                    ) AND
                    a.attnum > 0 AND
                    ((
                        c.relname LIKE 'KibanaSampleDataEcommerce' AND
                        n.nspname LIKE 'public'
                    ))
                ORDER BY
                    tablename,
                    fieldordinal
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn tableau_desktop_constraints() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "tableau_desktop_constraints",
        execute_query(
            "select	'test'::name as PKTABLE_CAT,
                n2.nspname as PKTABLE_SCHEM,
                c2.relname as PKTABLE_NAME,
                a2.attname as PKCOLUMN_NAME,
                'test'::name as FKTABLE_CAT,
                n1.nspname as FKTABLE_SCHEM,
                c1.relname as FKTABLE_NAME,
                a1.attname as FKCOLUMN_NAME,
                i::int2 as KEY_SEQ,
                case ref.confupdtype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as UPDATE_RULE,
                case ref.confdeltype
                    when 'c' then 0::int2
                    when 'n' then 2::int2
                    when 'd' then 4::int2
                    when 'r' then 1::int2
                    else 3::int2
                end as DELETE_RULE,
                ref.conname as FK_NAME,
                cn.conname as PK_NAME,
                case
                    when ref.condeferrable then
                        case
                        when ref.condeferred then 5::int2
                        else 6::int2
                        end
                    else 7::int2
                end as DEFERRABLITY
             from
             ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey,
                 generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) as i,
                 confupdtype, confdeltype, conname,
                 condeferrable, condeferred
              from pg_catalog.pg_constraint cn,
                pg_catalog.pg_class c,
                pg_catalog.pg_namespace n
              where contype = 'f'
               and  conrelid = c.oid
               and  relname = 'KibanaSampleDataEcommerce'
               and  n.oid = c.relnamespace
               and  n.nspname = 'public'
             ) ref
             inner join pg_catalog.pg_class c1
              on c1.oid = ref.conrelid)
             inner join pg_catalog.pg_namespace n1
              on  n1.oid = c1.relnamespace)
             inner join pg_catalog.pg_attribute a1
              on  a1.attrelid = c1.oid
              and  a1.attnum = conkey[i])
             inner join pg_catalog.pg_class c2
              on  c2.oid = ref.confrelid)
             inner join pg_catalog.pg_namespace n2
              on  n2.oid = c2.relnamespace)
             inner join pg_catalog.pg_attribute a2
              on  a2.attrelid = c2.oid
              and  a2.attnum = confkey[i])
             left outer join pg_catalog.pg_constraint cn
              on cn.conrelid = ref.confrelid
              and cn.contype = 'p')
              order by ref.oid, ref.i;"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn tableau_desktop_columns() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "tableau_desktop_table_columns",
            execute_query(
                "select
                    n.nspname,
                    c.relname,
                    a.attname,
                    a.atttypid,
                    t.typname,
                    a.attnum,
                    a.attlen,
                    a.atttypmod,
                    a.attnotnull,
                    c.relhasrules,
                    c.relkind,
                    c.oid,
                    pg_get_expr(d.adbin, d.adrelid),
                    case
                        t.typtype
                        when 'd' then t.typbasetype
                        else 0
                    end,
                    t.typtypmod,
                    c.relhasoids
                from
                    (
                        (
                            (
                                pg_catalog.pg_class c
                                inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                                and c.oid = 18000
                            )
                            inner join pg_catalog.pg_attribute a on (not a.attisdropped)
                            and a.attnum > 0
                            and a.attrelid = c.oid
                        )
                        inner join pg_catalog.pg_type t on t.oid = a.atttypid
                    )
                    /* Attention, We have hack for on a.atthasdef */
                    left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum
                order by
                    n.nspname,
                    c.relname,
                    attnum;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "tableau_desktop_indexes",
        execute_query(
            "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "tableau_desktop_pkeys",
        execute_query(
            "SELECT
                    ta.attname,
                    ia.attnum,
                    ic.relname,
                    n.nspname,
                    tc.relname
                FROM
                    pg_catalog.pg_attribute ta,
                    pg_catalog.pg_attribute ia,
                    pg_catalog.pg_class tc,
                    pg_catalog.pg_index i,
                    pg_catalog.pg_namespace n,
                    pg_catalog.pg_class ic
                WHERE
                    tc.relname = 'KibanaSampleDataEcommerce'
                    AND n.nspname = 'public'
                    AND tc.oid = i.indrelid
                    AND n.oid = tc.relnamespace
                    AND i.indisprimary = 't'
                    AND ia.attrelid = i.indexrelid
                    AND ta.attrelid = i.indrelid
                    AND ta.attnum = i.indkey [ia.attnum-1]
                    AND (NOT ta.attisdropped)
                    AND (NOT ia.attisdropped)
                    AND ic.oid = i.indexrelid
                ORDER BY
                    ia.attnum;"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "tableau_desktop_tables",
        execute_query(
            "select
                    relname,
                    nspname,
                    relkind
                from
                    pg_catalog.pg_class c,
                    pg_catalog.pg_namespace n
                where
                    relkind in ('r', 'v', 'm', 'f')
                    and nspname not in (
                        'pg_catalog',
                        'information_schema',
                        'pg_toast',
                        'pg_temp_1'
                    )
                    and n.oid = relnamespace
                order by
                    nspname,
                    relname"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn tableau_get_expr_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "tableau_get_expr_query",
            execute_query(
                "SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname, a.attnotnull OR ( t.typtype = 'd' AND t.typnotnull ), a.attidentity != '' OR pg_catalog.Pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%'
                FROM   pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n
                    ON ( c.relnamespace = n.oid )
                JOIN pg_catalog.pg_attribute a
                    ON ( c.oid = a.attrelid )
                JOIN pg_catalog.pg_type t
                    ON ( a.atttypid = t.oid )
                LEFT JOIN pg_catalog.pg_attrdef d
                    ON ( d.adrelid = a.attrelid AND d.adnum = a.attnum )
                JOIN (SELECT 2615 AS oid, 2 AS attnum UNION ALL SELECT 1259, 2 UNION ALL SELECT 2609, 4) vals
                ON ( c.oid = vals.oid AND a.attnum = vals.attnum );"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn datagrip_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "datagrip_introspection",
        execute_query(
            "select current_database(), current_schema(), current_user;".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn dbeaver_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "dbeaver_introspection_init",
        execute_query(
            "SELECT current_schema(), session_user;".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "dbeaver_introspection_databases",
        execute_query(
            "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname = 'cubedb'"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "dbeaver_introspection_namespaces",
            execute_query(
                "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n
                LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass
                ORDER BY nspname".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "dbeaver_introspection_types",
            execute_query(
                "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem
                LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid
                LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid
                WHERE t.typname IS NOT NULL
                AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')
                ORDER BY t.oid ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn postico1_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "postico1_schemas",
        execute_query(
            "SELECT
                    oid,
                    nspname,
                    nspname = ANY (current_schemas(true)) AS is_on_search_path,
                    oid = pg_my_temp_schema() AS is_my_temp_schema,
                    pg_is_other_temp_schema(oid) AS is_other_temp_schema
                FROM pg_namespace"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn tableau_regclass_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "tableau_regclass_query",
        execute_query(
            "SELECT NULL          AS TABLE_CAT,
                n.nspname     AS TABLE_SCHEM,
                c.relname     AS TABLE_NAME,
                CASE n.nspname ~ '^pg_'
                      OR n.nspname = 'information_schema'
                  WHEN true THEN
                    CASE
                      WHEN n.nspname = 'pg_catalog'
                            OR n.nspname = 'information_schema' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TABLE'
                          WHEN 'v' THEN 'SYSTEM VIEW'
                          WHEN 'i' THEN 'SYSTEM INDEX'
                          ELSE NULL
                        end
                      WHEN n.nspname = 'pg_toast' THEN
                        CASE c.relkind
                          WHEN 'r' THEN 'SYSTEM TOAST TABLE'
                          WHEN 'i' THEN 'SYSTEM TOAST INDEX'
                          ELSE NULL
                        end
                      ELSE
                        CASE c.relkind
                          WHEN 'r' THEN 'TEMPORARY TABLE'
                          WHEN 'p' THEN 'TEMPORARY TABLE'
                          WHEN 'i' THEN 'TEMPORARY INDEX'
                          WHEN 'S' THEN 'TEMPORARY SEQUENCE'
                          WHEN 'v' THEN 'TEMPORARY VIEW'
                          ELSE NULL
                        end
                    end
                  WHEN false THEN
                    CASE c.relkind
                      WHEN 'r' THEN 'TABLE'
                      WHEN 'p' THEN 'PARTITIONED TABLE'
                      WHEN 'i' THEN 'INDEX'
                      WHEN 'P' THEN 'PARTITIONED INDEX'
                      WHEN 'S' THEN 'SEQUENCE'
                      WHEN 'v' THEN 'VIEW'
                      WHEN 'c' THEN 'TYPE'
                      WHEN 'f' THEN 'FOREIGN TABLE'
                      WHEN 'm' THEN 'MATERIALIZED VIEW'
                      ELSE NULL
                    end
                  ELSE NULL
                end           AS TABLE_TYPE,
                d.description AS REMARKS,
                ''            AS TYPE_CAT,
                ''            AS TYPE_SCHEM,
                ''            AS TYPE_NAME,
                ''            AS SELF_REFERENCING_COL_NAME,
                ''            AS REF_GENERATION
            FROM   pg_catalog.pg_namespace n,
                pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_description d
                       ON ( c.oid = d.objoid
                            AND d.objsubid = 0
                            AND d.classoid = 'pg_class' :: regclass )
            WHERE  c.relnamespace = n.oid
                AND ( false
                       OR ( c.relkind = 'f' )
                       OR ( c.relkind = 'm' )
                       OR ( c.relkind = 'p'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'r'
                            AND n.nspname !~ '^pg_'
                            AND n.nspname <> 'information_schema' )
                       OR ( c.relkind = 'v'
                            AND n.nspname <> 'pg_catalog'
                            AND n.nspname <> 'information_schema' ) )
            ORDER BY TABLE_SCHEM ASC, TABLE_NAME ASC
            ;"
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn powerbi_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "powerbi_supported_types",
            execute_query(
                "/*** Load all supported types ***/
                SELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,
                CASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,
                CASE
                  WHEN pg_proc.proname='array_recv' THEN a.typelem
                  WHEN a.typtype='r' THEN rngsubtype
                  ELSE 0
                END AS elemoid,
                CASE
                  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */
                  WHEN a.typtype='r' THEN 2                                        /* Ranges before */
                  WHEN a.typtype='d' THEN 1                                        /* Domains before */
                  ELSE 0                                                           /* Base types first */
                END AS ord
                FROM pg_type AS a
                JOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)
                JOIN pg_proc ON pg_proc.oid = a.typreceive
                LEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)
                LEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)
                LEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)
                LEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid)
                WHERE
                  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */
                  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */
                  (pg_proc.proname='array_recv' AND (
                    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */
                    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */
                    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */
                  )) OR
                  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */
                /* changed for stable sort ORDER BY ord */
                ORDER BY a.typname"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "powerbi_composite_types",
        execute_query(
            "/*** Load field definitions for (free-standing) composite types ***/
                SELECT typ.oid, att.attname, att.atttypid
                FROM pg_type AS typ
                JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
                JOIN pg_class AS cls ON (cls.oid = typ.typrelid)
                JOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)
                WHERE
                    (typ.typtype = 'c' AND cls.relkind='c') AND
                attnum > 0 AND     /* Don't load system attributes */
                NOT attisdropped
                ORDER BY typ.oid, att.attnum"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "powerbi_enums",
        execute_query(
            "/*** Load enum fields ***/
                SELECT pg_type.oid, enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid=enumtypid
                ORDER BY oid, enumsortorder"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "powerbi_table_columns",
            execute_query(
                "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE
                from INFORMATION_SCHEMA.columns
                where TABLE_SCHEMA = 'public' and TABLE_NAME = 'KibanaSampleDataEcommerce'
                order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "powerbi_schemas",
        execute_query(
            "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
                from INFORMATION_SCHEMA.tables
                where TABLE_SCHEMA not in ('information_schema', 'pg_catalog')
                order by TABLE_SCHEMA, TABLE_NAME"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "powerbi_from_subquery",
            execute_query(
                "
                select
                    pkcol.COLUMN_NAME as PK_COLUMN_NAME,
                    fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA,
                    fkcol.TABLE_NAME AS FK_TABLE_NAME,
                    fkcol.COLUMN_NAME as FK_COLUMN_NAME,
                    fkcol.ORDINAL_POSITION as ORDINAL,
                    fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'users' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME
                from
                    (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, unique_constraint_name
                        from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol
                        on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA
                        and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME
                        inner join
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol
                        on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA
                        and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME
                where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'users'
                        and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION
                order by FK_NAME, fkcol.ORDINAL_POSITION
                ;
                "
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "powerbi_uppercase_alias",
        execute_query(
            "
                select
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME as INDEX_NAME,
                    ii.COLUMN_NAME,
                    ii.ORDINAL_POSITION,
                    case
                        when i.CONSTRAINT_TYPE = 'PRIMARY KEY' then 'Y'
                        else 'N'
                    end as PRIMARY_KEY
                from INFORMATION_SCHEMA.table_constraints i
                inner join INFORMATION_SCHEMA.key_column_usage ii on
                    i.CONSTRAINT_SCHEMA = ii.CONSTRAINT_SCHEMA and
                    i.CONSTRAINT_NAME = ii.CONSTRAINT_NAME and
                    i.TABLE_SCHEMA = ii.TABLE_SCHEMA and
                    i.TABLE_NAME = ii.TABLE_NAME
                where
                    i.TABLE_SCHEMA = 'public' and
                    i.TABLE_NAME = 'KibanaSampleDataEcommerce' and
                    i.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE')
                order by
                    i.CONSTRAINT_SCHEMA || '_' || i.CONSTRAINT_NAME,
                    ii.TABLE_SCHEMA,
                    ii.TABLE_NAME,
                    ii.ORDINAL_POSITION
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn superset_meta_queries() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "superset_attname_query",
        execute_query(
            r#"SELECT a.attname
                FROM pg_attribute a JOIN (
                SELECT unnest(ix.indkey) attnum,
                generate_subscripts(ix.indkey, 1) ord
                FROM pg_index ix
                WHERE ix.indrelid = 13449 AND ix.indisprimary
                ) k ON a.attnum=k.attnum
                WHERE a.attrelid = 13449
                ORDER BY k.ord
                "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    // TODO should be pg_get_expr instead of format_type
    insta::assert_snapshot!(
        "superset_subquery",
        execute_query(
            "
                SELECT
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    (
                        SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                        FROM pg_catalog.pg_attrdef d
                        WHERE
                            d.adrelid = a.attrelid AND
                            d.adnum = a.attnum AND
                            a.atthasdef
                    ) AS DEFAULT,
                    a.attnotnull,
                    a.attnum,
                    a.attrelid as table_oid,
                    pgd.description as comment,
                    a.attgenerated as generated
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_description pgd ON (
                    pgd.objoid = a.attrelid AND
                    pgd.objsubid = a.attnum
                )
                WHERE
                    a.attrelid = 18000
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY a.attnum
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "superset_visible_query",
        execute_query(
            r#"
                SELECT
                    t.typname as "name",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema",
                    e.enumlabel as "label"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
                WHERE t.typtype = 'e'
                ORDER BY
                    "schema",
                    "name",
                    e.oid
                ;
                "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "superset_attype_query",
        execute_query(
            r#"SELECT
                    t.typname as "name",
                    pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
                    not t.typnotnull as "nullable",
                    t.typdefault as "default",
                    pg_catalog.pg_type_is_visible(t.oid) as "visible",
                    n.nspname as "schema"
                FROM pg_catalog.pg_type t
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'd'
                ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "superset_indkey_varchar_query",
            execute_query(
                r#"SELECT
                    i.relname as relname,
                    ix.indisunique,
                    ix.indexprs,
                    a.attname,
                    a.attnum,
                    c.conrelid,
                    ix.indkey::varchar,
                    ix.indoption::varchar,
                    i.reloptions,
                    am.amname,
                    pg_get_expr(ix.indpred, ix.indrelid),
                    ix.indnkeyatts as indnkeyatts
                FROM pg_class t
                    join pg_index ix on t.oid = ix.indrelid
                    join pg_class i on i.oid = ix.indexrelid
                    left outer join pg_attribute a on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                    left outer join pg_constraint c on (ix.indrelid = c.conrelid and ix.indexrelid = c.conindid and c.contype in ('p', 'u', 'x'))
                    left outer join pg_am am on i.relam = am.oid
                WHERE t.relkind IN ('r', 'v', 'f', 'm', 'p') and t.oid = 18010 and ix.indisprimary = 'f'
                ORDER BY t.relname, i.relname
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn superset_conname_query() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "superset_conname_query",
        execute_query(
            r#"SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
                FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c
                WHERE r.conrelid = 13449 AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
                ORDER BY 1
                "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_insubquery_where_tables_spacing() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "grafana_insubquery_where_tables_spacing",
        execute_query(
            "select quote_ident(table_name) as \"table\" from information_schema.tables\
            \n    where quote_ident(table_schema) not in ('information_schema',\
            \n                             'pg_catalog',\
            \n                             '_timescaledb_cache',\
            \n                             '_timescaledb_catalog',\
            \n                             '_timescaledb_internal',\
            \n                             '_timescaledb_config',\
            \n                             'timescaledb_information',\
            \n                             'timescaledb_experimental')\
            \n      and \
            \n          quote_ident(table_schema) IN (\
            \n          SELECT\
            \n            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END\
            \n          FROM\
            \n            generate_series(\
            \n              array_lower(string_to_array(current_setting('search_path'),','),1),\
            \n              array_upper(string_to_array(current_setting('search_path'),','),1)\
            \n            ) as i,\
            \n            string_to_array(current_setting('search_path'),',') s\
            \n          )"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_grafana_pg_version_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "grafana_pg_version_introspection",
        execute_query(
            "SELECT current_setting('server_version_num')::int/100 as version".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_subquery_current_schema() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "microstrategy_subquery_current_schema",
            execute_query(
                "SELECT t.oid FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n ON t.typnamespace = n.oid WHERE t.typname = 'citext' AND (n.nspname = (SELECT current_schema()) OR n.nspname = 'public')".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_insubquery_where_tables() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "grafana_insubquery_where_tables",
            execute_query(
                r#"SELECT quote_ident(table_name) AS "table" FROM information_schema.tables WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', 'timescaledb_information', 'timescaledb_experimental') AND table_type = 'BASE TABLE' AND quote_ident(table_schema) IN (SELECT CASE WHEN TRIM(s[i]) = '"$user"' THEN user ELSE TRIM(s[i]) END FROM generate_series(array_lower(string_to_array(current_setting('search_path'), ','), 1), array_upper(string_to_array(current_setting('search_path'), ','), 1)) AS i, string_to_array(current_setting('search_path'), ',') AS s)"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_rust_client() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "rust_client_types",
            execute_query(
                r#"SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
                FROM pg_catalog.pg_type t
                LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
                INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = 25"#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_pg_get_expr_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_get_expr_1",
        execute_query(
            "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );
    insta::assert_snapshot!(
        "pg_get_expr_2",
        execute_query(
            "
                SELECT
                    attrelid,
                    attname,
                    pg_catalog.pg_get_expr(attname, attrelid, true) default
                FROM pg_catalog.pg_attribute
                ORDER BY
                    attrelid ASC,
                    attname ASC
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_truetyp() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_truetypid_truetypmod",
        execute_query(
            "
                SELECT
                    a.attrelid,
                    a.attname,
                    t.typname,
                    information_schema._pg_truetypid(a.*, t.*) typid,
                    information_schema._pg_truetypmod(a.*, t.*) typmod,
                    information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(a.*, t.*),
                        information_schema._pg_truetypmod(a.*, t.*)
                    ) as_arg
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                ORDER BY a.attrelid ASC, a.attnum ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

// https://github.com/sqlalchemy/sqlalchemy/blob/6104c163eb58e35e46b0bb6a237e824ec1ee1d15/lib/sqlalchemy/dialects/postgresql/base.py
#[tokio::test]
async fn sqlalchemy_new_conname_query() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "sqlalchemy_new_conname_query",
        execute_query(
            r#"SELECT
                a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod),
                (
                    SELECT
                        pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    FROM
                        pg_catalog.pg_attrdef AS d
                    WHERE
                        d.adrelid = a.attrelid
                        AND d.adnum = a.attnum
                        AND a.atthasdef
                ) AS DEFAULT,
                a.attnotnull,
                a.attrelid AS table_oid,
                pgd.description AS comment,
                a.attgenerated AS generated,
                (
                    SELECT
                        json_build_object(
                            'always',
                            a.attidentity = 'a',
                            'start',
                            s.seqstart,
                            'increment',
                            s.seqincrement,
                            'minvalue',
                            s.seqmin,
                            'maxvalue',
                            s.seqmax,
                            'cache',
                            s.seqcache,
                            'cycle',
                            s.seqcycle
                        )
                    FROM
                        pg_catalog.pg_sequence AS s
                        JOIN pg_catalog.pg_class AS c ON s.seqrelid = c."oid"
                    WHERE
                        c.relkind = 'S'
                        AND a.attidentity <> ''
                        AND s.seqrelid = CAST(
                            pg_catalog.pg_get_serial_sequence(
                                CAST(CAST(a.attrelid AS REGCLASS) AS TEXT),
                                a.attname
                            ) AS REGCLASS
                        )
                ) AS identity_options
            FROM
                pg_catalog.pg_attribute AS a
                LEFT JOIN pg_catalog.pg_description AS pgd ON (
                    pgd.objoid = a.attrelid
                    AND pgd.objsubid = a.attnum
                )
            WHERE
                a.attrelid = 18000
                AND a.attnum > 0
                AND NOT a.attisdropped
            ORDER BY
                a.attnum"#
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_sqlalchemy_regtype() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "sqlalchemy_regtype",
        execute_query(
            "SELECT
                    typname AS name,
                    oid,
                    typarray AS array_oid,
                    CAST(CAST(oid AS regtype) AS TEXT) AS regtype,
                    typdelim AS delimiter
                FROM
                    pg_type AS t
                WHERE
                    t.oid = to_regtype('boolean')
                ORDER BY
                    t.oid
                ;"
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn pgcli_queries() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
            "pgcli_queries_d",
            execute_query(
                r#"SELECT n.nspname as "Schema",
                    c.relname as "Name",
                    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                    pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
                    FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                    WHERE c.relkind IN ('r','p','v','m','S','f','')
                    AND n.nspname <> 'pg_catalog'
                    AND n.nspname !~ '^pg_toast'
                    AND n.nspname <> 'information_schema'
                    AND pg_catalog.pg_table_is_visible(c.oid)
                "#.to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_metabase() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            execute_query(
                "SELECT \
                    @@GLOBAL.time_zone AS global_tz, \
                    @@system_time_zone AS system_tz, time_format(   timediff(      now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')   ),   '%H:%i' ) AS 'offset'
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

    insta::assert_snapshot!(
            execute_query(
                "SELECT \
                TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, \
                CASE data_type WHEN 'bit' THEN -7 WHEN 'tinyblob' THEN -3 WHEN 'mediumblob' THEN -4 WHEN 'longblob' THEN -4 WHEN 'blob' THEN -4 WHEN 'tinytext' THEN 12 WHEN 'mediumtext' THEN -1 WHEN 'longtext' THEN -1 WHEN 'text' THEN -1 WHEN 'date' THEN 91 WHEN 'datetime' THEN 93 WHEN 'decimal' THEN 3 WHEN 'double' THEN 8 WHEN 'enum' THEN 12 WHEN 'float' THEN 7 WHEN 'int' THEN IF( COLUMN_TYPE like '%unsigned%', 4,4) WHEN 'bigint' THEN -5 WHEN 'mediumint' THEN 4 WHEN 'null' THEN 0 WHEN 'set' THEN 12 WHEN 'smallint' THEN IF( COLUMN_TYPE like '%unsigned%', 5,5) WHEN 'varchar' THEN 12 WHEN 'varbinary' THEN -3 WHEN 'char' THEN 1 WHEN 'binary' THEN -2 WHEN 'time' THEN 92 WHEN 'timestamp' THEN 93 WHEN 'tinyint' THEN IF(COLUMN_TYPE like 'tinyint(1)%',-7,-6)  WHEN 'year' THEN 91 ELSE 1111 END  DATA_TYPE, IF(COLUMN_TYPE like 'tinyint(1)%', 'BIT',  UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), COLUMN_TYPE))) TYPE_NAME,  CASE DATA_TYPE  WHEN 'time' THEN IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))  WHEN 'date' THEN 10  WHEN 'datetime' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  WHEN 'timestamp' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  ELSE   IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,2147483647), NUMERIC_PRECISION)  END COLUMN_SIZE, \
                65535 BUFFER_LENGTH, \
                CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS, 10 NUM_PREC_RADIX, \
                IF(IS_NULLABLE = 'yes',1,0) NULLABLE,
                COLUMN_COMMENT REMARKS, \
                COLUMN_DEFAULT COLUMN_DEF, \
                0 SQL_DATA_TYPE, \
                0 SQL_DATETIME_SUB, \
                LEAST(CHARACTER_OCTET_LENGTH,2147483647) CHAR_OCTET_LENGTH, \
                ORDINAL_POSITION, \
                IS_NULLABLE, \
                NULL SCOPE_CATALOG, \
                NULL SCOPE_SCHEMA, \
                NULL SCOPE_TABLE, \
                NULL SOURCE_DATA_TYPE, \
                IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, \
                IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ,'YES','NO') IS_GENERATEDCOLUMN \
                FROM INFORMATION_SCHEMA.COLUMNS  WHERE (ISNULL(database()) OR (TABLE_SCHEMA = database())) AND TABLE_NAME = 'KibanaSampleDataEcommerce' \
                ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

    insta::assert_snapshot!(
            execute_query(
                "SELECT
                    KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT,
                    NULL PKTABLE_SCHEM,
                    KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,
                    KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME,
                    KCU.TABLE_SCHEMA FKTABLE_CAT,
                    NULL FKTABLE_SCHEM,
                    KCU.TABLE_NAME FKTABLE_NAME,
                    KCU.COLUMN_NAME FKCOLUMN_NAME,
                    KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,
                    CASE update_rule    WHEN 'RESTRICT' THEN 1   WHEN 'NO ACTION' THEN 3   WHEN 'CASCADE' THEN 0   WHEN 'SET NULL' THEN 2   WHEN 'SET DEFAULT' THEN 4 END UPDATE_RULE,
                    CASE DELETE_RULE WHEN 'RESTRICT' THEN 1  WHEN 'NO ACTION' THEN 3  WHEN 'CASCADE' THEN 0  WHEN 'SET NULL' THEN 2  WHEN 'SET DEFAULT' THEN 4 END DELETE_RULE,
                    RC.CONSTRAINT_NAME FK_NAME,
                    NULL PK_NAME,
                    7 DEFERRABILITY
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
                WHERE (ISNULL(database()) OR (KCU.TABLE_SCHEMA = database())) AND  KCU.TABLE_NAME = 'SlackMessages' ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ
                ".to_string(), DatabaseProtocol::MySQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_metabase_table_cat_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "metabase_table_cat_query",
        execute_query(
            "
                SELECT  result.table_cat,
                        result.table_schem,
                        result.table_name,
                        result.column_name,
                        result.key_seq,
                        result.pk_name
                    FROM (
                        SELECT  NULL AS table_cat,
                                n.nspname AS table_schem,
                                ct.relname AS table_name,
                                a.attname AS column_name,
                                (information_schema._pg_expandarray(i.indkey)).n as key_seq,
                                ci.relname AS pk_name,
                                information_schema._pg_expandarray(i.indkey) AS keys,
                                a.attnum AS a_attnum
                            FROM   pg_catalog.pg_class ct
                            JOIN   pg_catalog.pg_attribute a ON(ct.oid = a.attrelid)
                            JOIN   pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                            JOIN   pg_catalog.pg_index i ON (a.attrelid = i.indrelid)
                            JOIN   pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                        WHERE true AND ct.relname = 'actor' AND i.indisprimary) result
                WHERE result.a_attnum = (result.keys).x
                ORDER BY result.table_name, result.pk_name, result.key_seq;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_metabase_pktable_cat_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "metabase_pktable_cat_query",
        execute_query(
            "
                SELECT  NULL::text  AS pktable_cat,
                        pkn.nspname AS pktable_schem,
                        pkc.relname AS pktable_name,
                        pka.attname AS pkcolumn_name,
                        NULL::text  AS fktable_cat,
                        fkn.nspname AS fktable_schem,
                        fkc.relname AS fktable_name,
                        fka.attname AS fkcolumn_name,
                        pos.n       AS key_seq,
                        CASE con.confupdtype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS update_rule,
                        CASE con.confdeltype
                            WHEN 'c' THEN 0
                            WHEN 'n' THEN 2
                            WHEN 'd' THEN 4
                            WHEN 'r' THEN 1
                            WHEN 'p' THEN 1
                            WHEN 'a' THEN 3
                            ELSE NULL
                        END AS delete_rule,
                        con.conname  AS fk_name,
                        pkic.relname AS pk_name,
                        CASE
                            WHEN con.condeferrable AND con.condeferred THEN 5
                            WHEN con.condeferrable THEN 6
                            ELSE 7
                        END AS deferrability
                    FROM    pg_catalog.pg_namespace pkn,
                            pg_catalog.pg_class pkc,
                            pg_catalog.pg_attribute pka,
                            pg_catalog.pg_namespace fkn,
                            pg_catalog.pg_class fkc,
                            pg_catalog.pg_attribute fka,
                            pg_catalog.pg_constraint con,
                            pg_catalog.generate_series(1, 32) pos(n),
                            pg_catalog.pg_class pkic
                WHERE   pkn.oid = pkc.relnamespace
                AND     pkc.oid = pka.attrelid
                AND     pka.attnum = con.confkey[pos.n]
                AND     con.confrelid = pkc.oid
                AND     fkn.oid = fkc.relnamespace
                AND     fkc.oid = fka.attrelid
                AND     fka.attnum = con.conkey[pos.n]
                AND     con.conrelid = fkc.oid
                AND     con.contype = 'f'
                AND     (pkic.relkind = 'i' OR pkic.relkind = 'I')
                AND     pkic.oid = con.conindid
                AND     fkn.nspname = 'public'
                AND     fkc.relname = 'actor'
                ORDER BY pkn.nspname, pkc.relname, con.conname, pos.n;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_metabase_type_in_subquery_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "metabase_type_in_subquery_query",
        execute_query(
            "
                SELECT nspname, typname
                FROM pg_type t
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE t.oid IN (SELECT DISTINCT enumtypid FROM pg_enum e);
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_sigma_computing_ilike_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "sigma_computing_ilike_query",
        execute_query(
            "
                select distinct table_schema
                from information_schema.tables
                where
                    table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                    table_schema NOT IN ('pg_catalog', 'information_schema') and
                    table_schema ilike '%'
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_sigma_computing_pg_matviews_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "sigma_computing_pg_matviews_query",
        execute_query(
            "
                SELECT table_name FROM (
                    select table_name
                    from information_schema.tables
                    where
                        table_type IN ('BASE TABLE', 'VIEW', 'FOREIGN', 'FOREIGN TABLE') and
                        table_schema = 'public'
                    UNION
                    select matviewname as table_name
                    from pg_catalog.pg_matviews
                    where schemaname = 'public'
                ) t
                ORDER BY table_name ASC
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_sigma_computing_array_subquery_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "sigma_computing_array_subquery_query",
            execute_query(
                r#"
                select
                    cl.relname as "source_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.conrelid and
                                attnum = con.conkey[i]
                        )
                        from generate_series(array_lower(con.conkey, 1), array_upper(con.conkey, 1)) i
                    ) as "source_keys",
                    (
                        select nspname
                        from pg_namespace ns2
                        join pg_class cl2 on ns2.oid = cl2.relnamespace
                        where cl2.oid = con.confrelid
                    ) as "target_schema",
                    (
                        select relname
                        from pg_class
                        where oid = con.confrelid
                    ) as "target_table",
                    array(
                        select (
                            select attname::text
                            from pg_attribute
                            where
                                attrelid = con.confrelid and
                                attnum = con.confkey[i]
                        )
                        from generate_series(array_lower(con.confkey, 1), array_upper(con.confkey, 1)) i
                    ) as "target_keys"
                from pg_class cl
                join pg_namespace ns on cl.relnamespace = ns.oid
                join pg_constraint con on con.conrelid = cl.oid
                where
                    ns.nspname = 'public' and
                    cl.relname >= 'A' and
                    cl.relname <= 'z' and
                    con.contype = 'f'
                order by
                    "source_table",
                    con.conname
                ;
                "#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_sigma_computing_with_subquery_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "sigma_computing_with_subquery_query",
        execute_query(
            "
                with
                    nsp as (
                        select oid
                        from pg_catalog.pg_namespace
                        where nspname = 'public'
                    ),
                    tbl as (
                        select oid
                        from pg_catalog.pg_class
                        where
                            relname = 'KibanaSampleDataEcommerce' and
                            relnamespace = (select oid from nsp)
                    )
                select
                    attname,
                    typname,
                    description
                from pg_attribute a
                join pg_type on atttypid = pg_type.oid
                left join pg_description on
                    attrelid = objoid and
                    attnum = objsubid
                where
                    attnum > 0 and
                    attrelid = (select oid from tbl)
                order by attnum
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_sigma_computing_attnames() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "sigma_computing_attnames",
        execute_query(
            "
                with
                    nsp as (
                        select oid as relnamespace
                        from pg_catalog.pg_namespace
                        where nspname = 'public'
                    ),
                    tbl as (
                        select
                            nsp.relnamespace as connamespace,
                            tbl.oid as conrelid
                        from pg_catalog.pg_class tbl
                        inner join nsp using (relnamespace)
                        where relname = 'emptytbl'
                    ),
                    con as (
                        select
                            conrelid,
                            conkey
                        from pg_catalog.pg_constraint
                        inner join tbl using (connamespace, conrelid)
                        where contype = 'p'
                    )
                select attname
                from pg_catalog.pg_attribute att
                inner join con on
                    conrelid = attrelid
                    and attnum = any(con.conkey)
                order by attnum
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_google_sheets_pg_database_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "google_sheets_pg_database_query",
        execute_query(
            "
                SELECT
                    cl.relname as Table,
                    att.attname AS Name,
                    att.attnum as Position,
                    CASE
                        WHEN att.attnotnull = 'f' THEN 'true'
                        ELSE 'false'
                    END as Nullable,
                    CASE
                        WHEN exists(
                            select null
                            from pg_constraint c
                            where
                                c.conrelid = cl.oid and
                                c.contype = 'p' and
                                att.attnum = ANY (c.conkey)
                        ) THEN true
                        ELSE false
                    END as IsKey,
                    CASE
                        WHEN cs.relname IS NULL THEN 'false'
                        ELSE 'true'
                    END as IsAutoIncrement,
                    CASE
                        WHEN ty.typname = 'bpchar' THEN 'char'
                        WHEN ty.typname = '_bpchar' THEN '_char'
                        ELSE ty.typname
                    END as TypeName,
                    CASE
                        WHEN
                            ty.typname Like 'bit' OR
                            ty.typname Like 'varbit' and
                            att.atttypmod > 0
                        THEN att.atttypmod
                        WHEN
                            ty.typname Like 'interval' OR
                            ty.typname Like 'timestamp' OR
                            ty.typname Like 'timestamptz' OR
                            ty.typname Like 'time' OR
                            ty.typname Like 'timetz' THEN -1
                        WHEN att.atttypmod > 0 THEN att.atttypmod - 4
                        ELSE att.atttypmod
                    END as Length,
                    (information_schema._pg_numeric_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Precision,
                    (information_schema._pg_numeric_scale(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS Scale,
                    (information_schema._pg_datetime_precision(
                        information_schema._pg_truetypid(att.*, ty.*),
                        information_schema._pg_truetypmod(att.*, ty.*)
                    ))::information_schema.cardinal_number AS DatetimeLength
                FROM pg_attribute att
                JOIN pg_type ty ON ty.oid = atttypid
                JOIN pg_namespace tn ON tn.oid = ty.typnamespace
                JOIN pg_class cl ON
                    cl.oid = attrelid AND
                    (
                        (cl.relkind = 'r') OR
                        (cl.relkind = 's') OR
                        (cl.relkind = 'v') OR
                        (cl.relkind = 'm') OR
                        (cl.relkind = 'f')
                    )
                JOIN pg_namespace na ON na.oid = cl.relnamespace
                LEFT OUTER JOIN (
                    pg_depend
                    JOIN pg_class cs ON
                        objid = cs.oid AND
                        cs.relkind = 'S' AND
                        classid = 'pg_class'::regclass::oid
                ) ON
                    refobjid = attrelid AND
                    refobjsubid = attnum
                LEFT JOIN pg_database db ON db.datname = current_database()
                WHERE
                    attnum > 0 AND
                    attisdropped IS FALSE AND
                    na.nspname = 'public' AND
                    cl.relname = 'KibanaSampleDataEcommerce'
                ORDER BY attnum
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_quicksight_has_schema_privilege_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "quicksight_has_schema_privilege_query",
        execute_query(
            "
                SELECT nspname AS schema_name
                FROM pg_namespace
                WHERE
                    (
                        has_schema_privilege('ovr', nspname, 'USAGE') = TRUE OR
                        has_schema_privilege('ovr', nspname, 'CREATE') = TRUE
                    ) AND
                    nspname NOT IN ('pg_catalog', 'information_schema') AND
                    nspname NOT LIKE 'pg_toast%' AND
                    nspname NOT LIKE 'pg_temp_%'
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_quicksight_pktable_cat_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "quicksight_pktable_cat_query",
        execute_query(
            "
                SELECT
                    NULL::text AS PKTABLE_CAT,
                    pkn.nspname AS PKTABLE_SCHEM,
                    pkc.relname AS PKTABLE_NAME,
                    pka.attname AS PKCOLUMN_NAME,
                    NULL::text AS FKTABLE_CAT,
                    fkn.nspname AS FKTABLE_SCHEM,
                    fkc.relname AS FKTABLE_NAME,
                    fka.attname AS FKCOLUMN_NAME,
                    pos.n AS KEY_SEQ,
                    CASE con.confupdtype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS UPDATE_RULE,
                    CASE con.confdeltype
                        WHEN 'c' THEN 0
                        WHEN 'n' THEN 2
                        WHEN 'd' THEN 4
                        WHEN 'r' THEN 1
                        WHEN 'a' THEN 3
                        ELSE NULL
                    END AS DELETE_RULE,
                    con.conname AS FK_NAME,
                    pkic.relname AS PK_NAME,
                    CASE
                        WHEN con.condeferrable AND con.condeferred THEN 5
                        WHEN con.condeferrable THEN 6
                        ELSE 7
                    END AS DEFERRABILITY
                FROM
                    pg_catalog.pg_namespace pkn,
                    pg_catalog.pg_class pkc,
                    pg_catalog.pg_attribute pka,
                    pg_catalog.pg_namespace fkn,
                    pg_catalog.pg_class fkc,
                    pg_catalog.pg_attribute fka,
                    pg_catalog.pg_constraint con,
                    pg_catalog.generate_series(1, 32) pos(n),
                    pg_catalog.pg_depend dep,
                    pg_catalog.pg_class pkic
                WHERE
                    pkn.oid = pkc.relnamespace AND
                    pkc.oid = pka.attrelid AND
                    pka.attnum = con.confkey[pos.n] AND
                    con.confrelid = pkc.oid AND
                    fkn.oid = fkc.relnamespace AND
                    fkc.oid = fka.attrelid AND
                    fka.attnum = con.conkey[pos.n] AND
                    con.conrelid = fkc.oid AND
                    con.contype = 'f' AND
                    con.oid = dep.objid AND
                    pkic.oid = dep.refobjid AND
                    pkic.relkind = 'i' AND
                    dep.classid = 'pg_constraint'::regclass::oid AND
                    dep.refclassid = 'pg_class'::regclass::oid AND
                    fkn.nspname = 'public' AND
                    fkc.relname = 'TABLENAME'
                ORDER BY
                    pkn.nspname,
                    pkc.relname,
                    con.conname,
                    pos.n
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_quicksight_table_cat_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "quicksight_table_cat_query",
        execute_query(
            "
                SELECT
                    NULL AS TABLE_CAT,
                    n.nspname AS TABLE_SCHEM,
                    ct.relname AS TABLE_NAME,
                    a.attname AS COLUMN_NAME,
                    (i.keys).n AS KEY_SEQ,
                    ci.relname AS PK_NAME
                FROM pg_catalog.pg_class ct
                JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)
                JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                JOIN (
                    SELECT
                        i.indexrelid,
                        i.indrelid,
                        i.indisprimary,
                        information_schema._pg_expandarray(i.indkey) AS keys
                    FROM pg_catalog.pg_index i
                ) i ON (
                    a.attnum = (i.keys).x AND
                    a.attrelid = i.indrelid
                )
                JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                WHERE
                    true AND
                    n.nspname = 'public' AND
                    ct.relname = 'KibanaSampleDataEcommerce' AND
                    i.indisprimary
                ORDER BY
                    table_name,
                    pk_name,
                    key_seq
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_thoughtspot_table_introspection() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "thoughtspot_table_introspection",
            execute_query(
                r#"
                SELECT *
                FROM (
                    SELECT
                        current_database() AS TABLE_CAT,
                        n.nspname AS TABLE_SCHEM,
                        c.relname AS TABLE_NAME,
                        a.attname AS COLUMN_NAME,
                        CAST(
                            CASE typname
                                WHEN 'text' THEN 12
                                WHEN 'bit' THEN - 7
                                WHEN 'bool' THEN - 7
                                WHEN 'boolean' THEN - 7
                                WHEN 'varchar' THEN 12
                                WHEN 'character varying' THEN 12
                                WHEN 'char' THEN 1
                                WHEN '"char"' THEN 1
                                WHEN 'character' THEN 1
                                WHEN 'nchar' THEN 12
                                WHEN 'bpchar' THEN 1
                                WHEN 'nvarchar' THEN 12
                                WHEN 'date' THEN 91
                                WHEN 'time' THEN 92
                                WHEN 'time without time zone' THEN 92
                                WHEN 'timetz' THEN 2013
                                WHEN 'time with time zone' THEN 2013
                                WHEN 'timestamp' THEN 93
                                WHEN 'timestamp without time zone' THEN 93
                                WHEN 'timestamptz' THEN 2014
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN 'smallint' THEN 5
                                WHEN 'int2' THEN 5
                                WHEN 'integer' THEN 4
                                WHEN 'int' THEN 4
                                WHEN 'int4' THEN 4
                                WHEN 'bigint' THEN - 5
                                WHEN 'int8' THEN - 5
                                WHEN 'decimal' THEN 3
                                WHEN 'real' THEN 7
                                WHEN 'float4' THEN 7
                                WHEN 'double precision' THEN 8
                                WHEN 'float8' THEN 8
                                WHEN 'float' THEN 6
                                WHEN 'numeric' THEN 2
                                WHEN '_float4' THEN 2003
                                WHEN '_aclitem' THEN 2003
                                WHEN '_text' THEN 2003
                                WHEN 'bytea' THEN - 2
                                WHEN 'oid' THEN - 5
                                WHEN 'name' THEN 12
                                WHEN '_int4' THEN 2003
                                WHEN '_int2' THEN 2003
                                WHEN 'ARRAY' THEN 2003
                                WHEN 'geometry' THEN - 4
                                WHEN 'super' THEN - 16
                                WHEN 'varbyte' THEN - 4
                                WHEN 'geography' THEN - 4
                                ELSE 1111
                            END
                            AS SMALLINT
                        ) AS DATA_TYPE,
                        t.typname AS TYPE_NAME,
                        CASE typname
                            WHEN 'int4' THEN 10
                            WHEN 'bit' THEN 1
                            WHEN 'bool' THEN 1
                            WHEN 'varchar' THEN atttypmod - 4
                            WHEN 'character varying' THEN atttypmod - 4
                            WHEN 'char' THEN atttypmod - 4
                            WHEN 'character' THEN atttypmod - 4
                            WHEN 'nchar' THEN atttypmod - 4
                            WHEN 'bpchar' THEN atttypmod - 4
                            WHEN 'nvarchar' THEN atttypmod - 4
                            WHEN 'date' THEN 13
                            WHEN 'time' THEN 15
                            WHEN 'time without time zone' THEN 15
                            WHEN 'timetz' THEN 21
                            WHEN 'time with time zone' THEN 21
                            WHEN 'timestamp' THEN 29
                            WHEN 'timestamp without time zone' THEN 29
                            WHEN 'timestamptz' THEN 35
                            WHEN 'timestamp with time zone' THEN 35
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 10
                            WHEN 'int' THEN 10
                            WHEN 'int4' THEN 10
                            WHEN 'bigint' THEN 19
                            WHEN 'int8' THEN 19
                            WHEN 'decimal' THEN (atttypmod - 4) >> 16
                            WHEN 'real' THEN 8
                            WHEN 'float4' THEN 8
                            WHEN 'double precision' THEN 17
                            WHEN 'float8' THEN 17
                            WHEN 'float' THEN 17
                            WHEN 'numeric' THEN (atttypmod - 4) >> 16
                            WHEN '_float4' THEN 8
                            WHEN 'oid' THEN 10
                            WHEN '_int4' THEN 10
                            WHEN '_int2' THEN 5
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 2147483647
                        END AS COLUMN_SIZE,
                        NULL AS BUFFER_LENGTH,
                        CASE typname
                            WHEN 'float4' THEN 8
                            WHEN 'float8' THEN 17
                            WHEN 'numeric' THEN (atttypmod - 4) & 65535
                            WHEN 'time without time zone' THEN 6
                            WHEN 'timetz' THEN 6
                            WHEN 'time with time zone' THEN 6
                            WHEN 'timestamp without time zone' THEN 6
                            WHEN 'timestamp' THEN 6
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 0
                        END AS DECIMAL_DIGITS,
                        CASE typname
                            WHEN 'varbyte' THEN 2
                            WHEN 'geography' THEN 2
                            ELSE 10
                        END AS NUM_PREC_RADIX,
                        CASE a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)
                            WHEN 'false' THEN 1
                            WHEN NULL THEN 2
                            ELSE 0
                        END AS NULLABLE,
                        dsc.description AS REMARKS,
                        pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS COLUMN_DEF,
                        CAST(
                            CASE typname
                                WHEN 'text' THEN 12
                                WHEN 'bit' THEN - 7
                                WHEN 'bool' THEN - 7
                                WHEN 'boolean' THEN - 7
                                WHEN 'varchar' THEN 12
                                WHEN 'character varying' THEN 12
                                WHEN '"char"' THEN 1
                                WHEN 'char' THEN 1
                                WHEN 'character' THEN 1
                                WHEN 'nchar' THEN 1
                                WHEN 'bpchar' THEN 1
                                WHEN 'nvarchar' THEN 12
                                WHEN 'date' THEN 91
                                WHEN 'time' THEN 92
                                WHEN 'time without time zone' THEN 92
                                WHEN 'timetz' THEN 2013
                                WHEN 'time with time zone' THEN 2013
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN 'timestamp' THEN 93
                                WHEN 'timestamp without time zone' THEN 93
                                WHEN 'smallint' THEN 5
                                WHEN 'int2' THEN 5
                                WHEN 'integer' THEN 4
                                WHEN 'int' THEN 4
                                WHEN 'int4' THEN 4
                                WHEN 'bigint' THEN - 5
                                WHEN 'int8' THEN - 5
                                WHEN 'decimal' THEN 3
                                WHEN 'real' THEN 7
                                WHEN 'float4' THEN 7
                                WHEN 'double precision' THEN 8
                                WHEN 'float8' THEN 8
                                WHEN 'float' THEN 6
                                WHEN 'numeric' THEN 2
                                WHEN '_float4' THEN 2003
                                WHEN 'timestamptz' THEN 2014
                                WHEN 'timestamp with time zone' THEN 2014
                                WHEN '_aclitem' THEN 2003
                                WHEN '_text' THEN 2003
                                WHEN 'bytea' THEN - 2
                                WHEN 'oid' THEN - 5
                                WHEN 'name' THEN 12
                                WHEN '_int4' THEN 2003
                                WHEN '_int2' THEN 2003
                                WHEN 'ARRAY' THEN 2003
                                WHEN 'geometry' THEN - 4
                                WHEN 'super' THEN - 16
                                WHEN 'varbyte' THEN - 4
                                WHEN 'geography' THEN - 4 ELSE 1111
                            END
                            AS SMALLINT
                        ) AS SQL_DATA_TYPE,
                        CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,
                        CASE typname
                            WHEN 'int4' THEN 10
                            WHEN 'bit' THEN 1
                            WHEN 'bool' THEN 1
                            WHEN 'varchar' THEN atttypmod - 4
                            WHEN 'character varying' THEN atttypmod - 4
                            WHEN 'char' THEN atttypmod - 4
                            WHEN 'character' THEN atttypmod - 4
                            WHEN 'nchar' THEN atttypmod - 4
                            WHEN 'bpchar' THEN atttypmod - 4
                            WHEN 'nvarchar' THEN atttypmod - 4
                            WHEN 'date' THEN 13
                            WHEN 'time' THEN 15
                            WHEN 'time without time zone' THEN 15
                            WHEN 'timetz' THEN 21
                            WHEN 'time with time zone' THEN 21
                            WHEN 'timestamp' THEN 29
                            WHEN 'timestamp without time zone' THEN 29
                            WHEN 'timestamptz' THEN 35
                            WHEN 'timestamp with time zone' THEN 35
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 10
                            WHEN 'int' THEN 10
                            WHEN 'int4' THEN 10
                            WHEN 'bigint' THEN 19
                            WHEN 'int8' THEN 19
                            WHEN 'decimal' THEN ((atttypmod - 4) >> 16) & 65535
                            WHEN 'real' THEN 8
                            WHEN 'float4' THEN 8
                            WHEN 'double precision' THEN 17
                            WHEN 'float8' THEN 17
                            WHEN 'float' THEN 17
                            WHEN 'numeric' THEN ((atttypmod - 4) >> 16) & 65535
                            WHEN '_float4' THEN 8
                            WHEN 'oid' THEN 10
                            WHEN '_int4' THEN 10
                            WHEN '_int2' THEN 5
                            WHEN 'geometry' THEN NULL
                            WHEN 'super' THEN NULL
                            WHEN 'varbyte' THEN NULL
                            WHEN 'geography' THEN NULL
                            ELSE 2147483647
                        END AS CHAR_OCTET_LENGTH,
                        a.attnum AS ORDINAL_POSITION,
                        CASE a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)
                            WHEN 'false' THEN 'YES'
                            WHEN NULL THEN ''
                            ELSE 'NO'
                        END AS IS_NULLABLE,
                        NULL AS SCOPE_CATALOG,
                        NULL AS SCOPE_SCHEMA,
                        NULL AS SCOPE_TABLE,
                        t.typbasetype AS SOURCE_DATA_TYPE,
                        CASE
                            WHEN left(pg_catalog.pg_get_expr(def.adbin, def.adrelid), 16) = 'default_identity' THEN 'YES'
                            ELSE 'NO'
                        END AS IS_AUTOINCREMENT,
                        false AS IS_GENERATEDCOLUMN
                    FROM pg_catalog.pg_namespace AS n
                    JOIN pg_catalog.pg_class AS c ON (c.relnamespace = n.oid)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid = c.oid)
                    JOIN pg_catalog.pg_type AS t ON (a.atttypid = t.oid)
                    LEFT JOIN pg_catalog.pg_attrdef AS def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum)
                    LEFT JOIN pg_catalog.pg_description AS dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid)
                    LEFT JOIN pg_catalog.pg_class AS dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class')
                    LEFT JOIN pg_catalog.pg_namespace AS dn ON (dc.relnamespace = dn.oid AND dn.nspname = 'pg_catalog')
                    WHERE
                        a.attnum > 0 AND
                        NOT a.attisdropped AND
                        current_database() = 'cubedb' AND
                        n.nspname LIKE 'public' AND
                        c.relname LIKE 'KibanaSampleDataEcommerce'
                    ORDER BY
                        TABLE_SCHEM,
                        c.relname,
                        attnum
                ) AS t
                UNION ALL
                SELECT
                    CAST(current_database() AS CHARACTER VARYING(128)) AS TABLE_CAT,
                    CAST(schemaname AS CHARACTER VARYING(128)) AS table_schem,
                    CAST(tablename AS CHARACTER VARYING(128)) AS table_name,
                    CAST(columnname AS CHARACTER VARYING(128)) AS column_name,
                    CAST(
                        CASE columntype_rep
                            WHEN 'text' THEN 12
                            WHEN 'bit' THEN - 7
                            WHEN 'bool' THEN - 7
                            WHEN 'boolean' THEN - 7
                            WHEN 'varchar' THEN 12
                            WHEN 'character varying' THEN 12
                            WHEN 'char' THEN 1
                            WHEN 'character' THEN 1
                            WHEN 'nchar' THEN 1
                            WHEN 'bpchar' THEN 1
                            WHEN 'nvarchar' THEN 12
                            WHEN '"char"' THEN 1
                            WHEN 'date' THEN 91
                            WHEN 'time' THEN 92
                            WHEN 'time without time zone' THEN 92
                            WHEN 'timetz' THEN 2013
                            WHEN 'time with time zone' THEN 2013
                            WHEN 'timestamp' THEN 93
                            WHEN 'timestamp without time zone' THEN 93
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'timestamp with time zone' THEN 2014
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 4
                            WHEN 'int' THEN 4
                            WHEN 'int4' THEN 4
                            WHEN 'bigint' THEN - 5
                            WHEN 'int8' THEN - 5
                            WHEN 'decimal' THEN 3
                            WHEN 'real' THEN 7
                            WHEN 'float4' THEN 7
                            WHEN 'double precision' THEN 8
                            WHEN 'float8' THEN 8
                            WHEN 'float' THEN 6
                            WHEN 'numeric' THEN 2
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'bytea' THEN - 2
                            WHEN 'oid' THEN - 5
                            WHEN 'name' THEN 12
                            WHEN 'ARRAY' THEN 2003
                            WHEN 'geometry' THEN - 4
                            WHEN 'super' THEN - 16
                            WHEN 'varbyte' THEN - 4
                            WHEN 'geography' THEN - 4
                            ELSE 1111
                        END
                        AS SMALLINT
                    ) AS DATA_TYPE,
                    COALESCE(
                        NULL,
                        CASE columntype
                            WHEN 'boolean' THEN 'bool'
                            WHEN 'character varying' THEN 'varchar'
                            WHEN '"char"' THEN 'char'
                            WHEN 'smallint' THEN 'int2'
                            WHEN 'integer' THEN 'int4'
                            WHEN 'bigint' THEN 'int8'
                            WHEN 'real' THEN 'float4'
                            WHEN 'double precision' THEN 'float8'
                            WHEN 'time without time zone' THEN 'time'
                            WHEN 'time with time zone' THEN 'timetz'
                            WHEN 'timestamp without time zone' THEN 'timestamp'
                            WHEN 'timestamp with time zone' THEN 'timestamptz'
                            ELSE columntype
                        END
                    ) AS TYPE_NAME,
                    CASE columntype_rep
                        WHEN 'int4' THEN 10
                        WHEN 'bit' THEN 1
                        WHEN 'bool' THEN 1
                        WHEN 'boolean' THEN 1
                        WHEN 'varchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'character varying' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'char' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN 'character' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN 'nchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'bpchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'nvarchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'date' THEN 13
                        WHEN 'time' THEN 15
                        WHEN 'time without time zone' THEN 15
                        WHEN 'timetz' THEN 21
                        WHEN 'timestamp' THEN 29
                        WHEN 'timestamp without time zone' THEN 29
                        WHEN 'time with time zone' THEN 21
                        WHEN 'timestamptz' THEN 35
                        WHEN 'timestamp with time zone' THEN 35
                        WHEN 'smallint' THEN 5
                        WHEN 'int2' THEN 5
                        WHEN 'integer' THEN 10
                        WHEN 'int' THEN 10
                        WHEN 'int4' THEN 10
                        WHEN 'bigint' THEN 19
                        WHEN 'int8' THEN 19
                        WHEN 'decimal' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN 'real' THEN 8
                        WHEN 'float4' THEN 8
                        WHEN 'double precision' THEN 17
                        WHEN 'float8' THEN 17
                        WHEN 'float' THEN 17
                        WHEN 'numeric' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN '_float4' THEN 8
                        WHEN 'oid' THEN 10
                        WHEN '_int4' THEN 10
                        WHEN '_int2' THEN 5
                        WHEN 'geometry' THEN NULL
                        WHEN 'super' THEN NULL
                        WHEN 'varbyte' THEN NULL
                        WHEN 'geography' THEN NULL
                        ELSE 2147483647
                    END AS COLUMN_SIZE,
                    NULL AS BUFFER_LENGTH,
                    CASE REGEXP_REPLACE(columntype, '[()0-9,]')
                        WHEN 'real' THEN 8
                        WHEN 'float4' THEN 8
                        WHEN 'double precision' THEN 17
                        WHEN 'float8' THEN 17
                        WHEN 'timestamp' THEN 6
                        WHEN 'timestamp without time zone' THEN 6
                        WHEN 'geometry' THEN NULL
                        WHEN 'super' THEN NULL
                        WHEN 'numeric' THEN CAST(regexp_substr(columntype, '[0-9]+', charindex(',', columntype)) AS INT)
                        WHEN 'varbyte' THEN NULL
                        WHEN 'geography' THEN NULL
                        ELSE 0
                    END AS DECIMAL_DIGITS,
                    CASE columntype
                        WHEN 'varbyte' THEN 2
                        WHEN 'geography' THEN 2
                        ELSE 10
                    END AS NUM_PREC_RADIX,
                    NULL AS NULLABLE,
                    NULL AS REMARKS,
                    NULL AS COLUMN_DEF,
                    CAST(
                        CASE columntype_rep
                            WHEN 'text' THEN 12
                            WHEN 'bit' THEN - 7
                            WHEN 'bool' THEN - 7
                            WHEN 'boolean' THEN - 7
                            WHEN 'varchar' THEN 12
                            WHEN 'character varying' THEN 12
                            WHEN 'char' THEN 1
                            WHEN 'character' THEN 1
                            WHEN 'nchar' THEN 12
                            WHEN 'bpchar' THEN 1
                            WHEN 'nvarchar' THEN 12
                            WHEN '"char"' THEN 1
                            WHEN 'date' THEN 91
                            WHEN 'time' THEN 92
                            WHEN 'time without time zone' THEN 92
                            WHEN 'timetz' THEN 2013
                            WHEN 'time with time zone' THEN 2013
                            WHEN 'timestamp' THEN 93
                            WHEN 'timestamp without time zone' THEN 93
                            WHEN 'timestamptz' THEN 2014
                            WHEN 'timestamp with time zone' THEN 2014
                            WHEN 'smallint' THEN 5
                            WHEN 'int2' THEN 5
                            WHEN 'integer' THEN 4
                            WHEN 'int' THEN 4
                            WHEN 'int4' THEN 4
                            WHEN 'bigint' THEN - 5
                            WHEN 'int8' THEN - 5
                            WHEN 'decimal' THEN 3
                            WHEN 'real' THEN 7
                            WHEN 'float4' THEN 7
                            WHEN 'double precision' THEN 8
                            WHEN 'float8' THEN 8
                            WHEN 'float' THEN 6
                            WHEN 'numeric' THEN 2
                            WHEN 'bytea' THEN - 2
                            WHEN 'oid' THEN - 5
                            WHEN 'name' THEN 12
                            WHEN 'ARRAY' THEN 2003
                            WHEN 'geometry' THEN - 4
                            WHEN 'super' THEN - 16
                            WHEN 'varbyte' THEN - 4
                            WHEN 'geography' THEN - 4
                            ELSE 1111
                        END
                        AS SMALLINT
                    ) AS SQL_DATA_TYPE,
                    CAST(NULL AS SMALLINT) AS SQL_DATETIME_SUB,
                    CASE
                        WHEN LEFT(columntype, 7) = 'varchar' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 7), ''), '0') AS INT)
                        WHEN LEFT(columntype, 4) = 'char' THEN CAST(isnull(nullif(regexp_substr(columntype, '[0-9]+', 4), ''), '0') AS INT)
                        WHEN columntype = 'string' THEN 16383
                        ELSE NULL
                    END AS CHAR_OCTET_LENGTH,
                    columnnum AS ORDINAL_POSITION,
                    NULL AS IS_NULLABLE,
                    NULL AS SCOPE_CATALOG,
                    NULL AS SCOPE_SCHEMA,
                    NULL AS SCOPE_TABLE,
                    NULL AS SOURCE_DATA_TYPE,
                    'NO' AS IS_AUTOINCREMENT,
                    'NO' AS IS_GENERATEDCOLUMN
                FROM (
                    SELECT
                        schemaname,
                        tablename,
                        columnname,
                        columntype AS columntype_rep,
                        columntype,
                        columnnum
                    FROM get_late_binding_view_cols_unpacked
                ) AS lbv_columns
                WHERE
                    true AND
                    current_database() = 'cubedb' AND
                    schemaname LIKE 'public' AND
                    tablename LIKE 'KibanaSampleDataEcommerce'
                ;"#
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_holistics_schema_privilege_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "holistics_schema_privilege_query",
            execute_query(
                "
                SELECT n.nspname AS schema_name
                FROM pg_namespace n
                WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema' AND has_schema_privilege(n.nspname, 'USAGE'::text);
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_holistics_left_join_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "holistics_left_join_query",
            execute_query(
                "
                SELECT
                    TRIM(c.conname) AS constraint_name,
                    CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY' END AS constraint_type,
                    TRIM(cn.nspname) AS constraint_schema,
                    TRIM(tn.nspname) AS schema_name,
                    TRIM(tc.relname) AS table_name,
                    TRIM(ta.attname) AS column_name,
                    TRIM(fn.nspname) AS referenced_schema_name,
                    TRIM(fc.relname) AS referenced_table_name,
                    TRIM(fa.attname) AS referenced_column_name,
                    o.ord AS ordinal_position
                FROM pg_constraint c
                    LEFT JOIN generate_series(1,1600) as o(ord) ON c.conkey[o.ord] IS NOT  NULL
                    LEFT JOIN pg_attribute ta ON c.conrelid=ta.attrelid AND ta.attnum=c.conkey[o.ord]
                    LEFT JOIN pg_attribute fa ON c.confrelid=fa.attrelid AND fa.attnum=c.confkey[o.ord]
                    LEFT JOIN pg_class tc ON ta.attrelid=tc.oid
                    LEFT JOIN pg_class fc ON fa.attrelid=fc.oid
                    LEFT JOIN pg_namespace cn ON c.connamespace=cn.oid
                    LEFT JOIN pg_namespace tn ON tc.relnamespace=tn.oid
                    LEFT JOIN pg_namespace fn ON fc.relnamespace=fn.oid
                WHERE
                    CASE c.contype WHEN 'p'
                    THEN 'PRIMARY KEY' WHEN 'u'
                    THEN 'UNIQUE' WHEN 'f'
                    THEN 'FOREIGN KEY'
                    END
                IN ('UNIQUE', 'PRIMARY KEY', 'FOREIGN KEY') AND tc.relkind = 'r'
                ".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_holistics_in_subquery_query() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "holistics_in_subquery_query",
            execute_query(
                "SELECT\n          n.nspname || '.' || c.relname AS \"table_name\",\n          a.attname AS \"column_name\",\n          format_type(a.atttypid, a.atttypmod) AS \"data_type\"\n        FROM pg_namespace n,\n             pg_class c,\n             pg_attribute a\n        WHERE n.oid = c.relnamespace\n          AND c.oid = a.attrelid\n          AND a.attnum > 0\n          AND NOT a.attisdropped\n          AND c.relname IN (SELECT table_name\nFROM information_schema.tables\nWHERE (table_type = 'BASE TABLE' OR table_type = 'VIEW')\n  AND table_schema NOT IN ('pg_catalog', 'information_schema')\n  AND has_schema_privilege(table_schema, 'USAGE'::text)\n)\n
                /* Added to avoid random output order and validate snapshot */
                order by table_name, column_name;"
                .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_langchain_pg_get_indexdef_and_in_realiasing() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "langchain_pg_get_indexdef_and_in_realiasing",
            execute_query(
                "
                SELECT
                    pg_catalog.pg_index.indrelid,
                    cls_idx.relname AS relname_index,
                    pg_catalog.pg_index.indisunique,
                    pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint,
                    pg_catalog.pg_index.indoption,
                    cls_idx.reloptions,
                    pg_catalog.pg_am.amname,
                    CASE
                        WHEN (pg_catalog.pg_index.indpred IS NOT NULL)
                            THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid)
                    END AS filter_definition,
                    pg_catalog.pg_index.indnkeyatts,
                    idx_cols.elements,
                    idx_cols.elements_is_expr
                FROM pg_catalog.pg_index
                JOIN pg_catalog.pg_class AS cls_idx ON pg_catalog.pg_index.indexrelid = cls_idx.oid
                JOIN pg_catalog.pg_am ON cls_idx.relam = pg_catalog.pg_am.oid
                LEFT OUTER JOIN (
                    SELECT
                        idx_attr.indexrelid AS indexrelid,
                        min(idx_attr.indrelid) AS min_1,
                        array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements,
                        array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr
                    FROM (
                        SELECT
                            idx.indexrelid AS indexrelid,
                            idx.indrelid AS indrelid,
                            idx.ord AS ord,
                            CASE
                                WHEN (idx.attnum = 0)
                                    THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true)
                                ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT)
                            END AS element,
                            idx.attnum = 0 AS is_expr
                        FROM (
                            SELECT
                                pg_catalog.pg_index.indexrelid AS indexrelid,
                                pg_catalog.pg_index.indrelid AS indrelid,
                                unnest(pg_catalog.pg_index.indkey) AS attnum,
                                generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord
                            FROM pg_catalog.pg_index
                            WHERE
                                NOT pg_catalog.pg_index.indisprimary
                                AND pg_catalog.pg_index.indrelid IN (18000)
                        ) AS idx
                        LEFT OUTER JOIN pg_catalog.pg_attribute ON
                            pg_catalog.pg_attribute.attnum = idx.attnum
                            AND pg_catalog.pg_attribute.attrelid = idx.indrelid
                        WHERE idx.indrelid IN (18000)
                    ) AS idx_attr
                    GROUP BY idx_attr.indexrelid
                ) AS idx_cols ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid
                LEFT OUTER JOIN pg_catalog.pg_constraint ON
                    pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid
                    AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid
                    AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x'])
                WHERE
                    pg_catalog.pg_index.indrelid IN (18000)
                    AND NOT pg_catalog.pg_index.indisprimary
                ORDER BY
                    pg_catalog.pg_index.indrelid,
                    cls_idx.relname
                ;".to_string(),
                DatabaseProtocol::PostgreSQL,
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_zoho_inet_server_addr() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "zoho_inet_server_addr",
        execute_query(
            "
                select
                    pg_backend_pid(),
                    coalesce(cast(inet_server_addr() as text ),'addr'),
                    current_database()
                ;"
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_extended_table_introspection() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
            "test_extended_table_introspection",
            execute_query(
                "SELECT current_database()        as TABLE_CAT,\
                \nCOALESCE(T.table_schema, MV.schemaname) as TABLE_SCHEM,\
                \nCOALESCE(T.table_name, MV.matviewname)  as TABLE_NAME,\
                \n(CASE\
                \n  WHEN c.reltuples < 0 THEN NULL\
                \n  WHEN c.relpages = 0 THEN float8 '0'\
                \n  ELSE c.reltuples / c.relpages END\
                \n  * (pg_relation_size(c.oid) / pg_catalog.current_setting('block_size')::int)\
                \n)::bigint                               as ROW_COUNT,\
                \nC.relnatts                              as COLUMN_COUNT,\
                \nC.relkind                               as TABLE_KIND,\
                \nC.relispartition                        as IS_PARTITION,\
                \nP.partstrat                             as PARTITION_STRATEGY,\
                \nPC.parition_count                       as PARTITION_COUNT,\
                \nPARTITION.parent_name                   as PARENT_TABLE_NAME,\
                \nPARTITION.parent_table_kind             as PARTITIONED_PARENT_TABLE,\
                \nPARTITION_RANGE.PARTITION_CONSTRAINT    as PARTITION_CONSTRAINT,\
                \nP.partnatts                             as NUMBER_COLUMNS_IN_PART_KEY,\
                \nP.partattrs                             as COLUMNS_PARTICIPATING_IN_PART_KEY,\
                \nCOALESCE(V.definition, MV.definition)   as VIEW_DEFINITION,\
                \nT.*\
                \nFROM pg_class C\
                \n        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)\
                \n        LEFT JOIN pg_stat_user_tables PSUT ON (C.oid = PSUT.relid)\
                \n        LEFT JOIN information_schema.tables T ON (C.relname = T.table_name AND N.nspname = T.table_schema)\
                \n        LEFT JOIN pg_views V ON (T.table_name = V.viewname)\
                \n        LEFT JOIN pg_matviews MV ON (C.relname = MV.matviewname)\
                \n        LEFT JOIN pg_partitioned_table P on C.oid = P.partrelid\
                \n        LEFT JOIN (SELECT parent.relname AS table_name,\
                \n                        COUNT(*)       as parition_count\
                \n                    FROM pg_inherits\
                \n                            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid\
                \n                            JOIN pg_class child ON pg_inherits.inhrelid = child.oid\
                \n                            JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace\
                \n                            JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace\
                \n                    GROUP BY table_name) AS PC ON (C.relname = PC.table_name)\
                \n        LEFT JOIN (SELECT child.relname  AS table_name,\
                \n                        parent.relname AS parent_name,\
                \n                        parent.relispartition AS parent_table_kind\
                \n                    FROM pg_inherits\
                \n                            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid\
                \n                            JOIN pg_class child ON pg_inherits.inhrelid = child.oid\
                \n                            JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace\
                \n                            JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace\
                \n                    WHERE parent.relkind = 'p') AS PARTITION ON (C.relname = PARTITION.table_name)\
                \n        LEFT JOIN (SELECT c.relname AS PARTITION_NAME, pg_get_expr(c.relpartbound, c.oid, true) AS PARTITION_CONSTRAINT\
                \n                    from pg_class c\
                \n                    where c.relispartition = 'true'\
                \n                    and c.relkind = 'r') AS PARTITION_RANGE ON (C.relname = PARTITION_RANGE.PARTITION_NAME)\
                \nWHERE N.nspname in (SELECT schema_name\
                \n                    FROM INFORMATION_SCHEMA.SCHEMATA\
                \n                    WHERE schema_name not like 'pg_%%'\
                \n                    and schema_name != 'information_schema')\
                \nAND C.relkind != 'i'\
                \nAND C.relkind != 'I'\
                \nORDER BY T.table_name ASC".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_metabase_introspection_indoption() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "metabase_introspection_indoption",
        execute_query(
            r#"
                SELECT
                  tmp.TABLE_CAT,
                  tmp.TABLE_SCHEM,
                  tmp.TABLE_NAME,
                  tmp.NON_UNIQUE,
                  tmp.INDEX_QUALIFIER,
                  tmp.INDEX_NAME,
                  tmp.TYPE,
                  tmp.ORDINAL_POSITION,
                  trim(
                    both '"'
                    from
                      pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)
                  ) AS COLUMN_NAME,
                  CASE
                    tmp.AM_NAME
                    WHEN 'btree' THEN CASE
                      tmp.I_INDOPTION [tmp.ORDINAL_POSITION - 1] & 1 :: smallint
                      WHEN 1 THEN 'D'
                      ELSE 'A'
                    END
                    ELSE NULL
                  END AS ASC_OR_DESC,
                  tmp.CARDINALITY,
                  tmp.PAGES,
                  tmp.FILTER_CONDITION
                FROM
                  (
                    SELECT
                      NULL AS TABLE_CAT,
                      n.nspname AS TABLE_SCHEM,
                      ct.relname AS TABLE_NAME,
                      NOT i.indisunique AS NON_UNIQUE,
                      NULL AS INDEX_QUALIFIER,
                      ci.relname AS INDEX_NAME,
                      CASE
                        i.indisclustered
                        WHEN true THEN 1
                        ELSE CASE
                          am.amname
                          WHEN 'hash' THEN 2
                          ELSE 3
                        END
                      END AS TYPE,
                      (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION,
                      ci.reltuples AS CARDINALITY,
                      ci.relpages AS PAGES,
                      pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION,
                      ci.oid AS CI_OID,
                      i.indoption AS I_INDOPTION,
                      am.amname AS AM_NAME
                    FROM
                      pg_catalog.pg_class ct
                      JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
                      JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)
                      JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
                      JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)
                    WHERE
                      true
                      AND n.nspname = 'public'
                      AND ct.relname = 'IT_Assistance_Needed'
                  ) AS tmp
                ORDER BY
                  NON_UNIQUE,
                  TYPE,
                  INDEX_NAME,
                  ORDINAL_POSITION
                "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn test_metabase_v0_51_2_introspection_field_indoption() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "test_metabase_v0_51_2_introspection_field_indoption",
        execute_query(
            // language=PostgreSQL
            r#"
            SELECT
            c.column_name AS name,
            c.udt_name AS database_type,
            c.ordinal_position - 1 AS database_position,
            c.table_schema AS table_schema,
            c.table_name AS table_name,
            pk.column_name IS NOT NULL AS pk,
            COL_DESCRIPTION(
                CAST(
                    CAST(
                        FORMAT(
                        '%I.%I',
                        CAST(c.table_schema AS TEXT),
                        CAST(c.table_name AS TEXT)
                        ) AS REGCLASS
                    ) AS OID
                ),
                c.ordinal_position
            ) AS field_comment,
            (
                (column_default IS NULL)
                OR (LOWER(column_default) = 'null')
            )
            AND (is_nullable = 'NO')
            AND NOT (
                (
                (column_default IS NOT NULL)
                AND (column_default LIKE '%nextval(%')
                )
                OR (is_identity <> 'NO')
            ) AS database_required,
            (
                (column_default IS NOT NULL)
                AND (column_default LIKE '%nextval(%')
            )
            OR (is_identity <> 'NO') AS database_is_auto_increment
            FROM
            information_schema.columns AS c
            LEFT JOIN (
                SELECT
                tc.table_schema,
                tc.table_name,
                kc.column_name
                FROM
                information_schema.table_constraints AS tc
                INNER JOIN information_schema.key_column_usage AS kc ON (tc.constraint_name = kc.constraint_name)
                AND (tc.table_schema = kc.table_schema)
                AND (tc.table_name = kc.table_name)
                WHERE
                tc.constraint_type = 'PRIMARY KEY'
            ) AS pk ON (c.table_schema = pk.table_schema)
            AND (c.table_name = pk.table_name)
            AND (c.column_name = pk.column_name)
            WHERE
            c.table_schema !~ '^information_schema|catalog_history|pg_'
            AND (c.table_schema IN ('public'))
            ORDER BY
            table_schema ASC,
              table_name ASC,
              database_position ASC
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );
    Ok(())
}

#[tokio::test]
async fn test_metabase_v0_51_8_introspection() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "test_metabase_v0_51_8_introspection",
        execute_query(
            // language=PostgreSQL
            r#"
select
    "c"."column_name" as "name",
    case
        when "c"."udt_schema" in ('public', 'pg_catalog')
        then format('%s', "c"."udt_name")
        else format('"%s"."%s"', "c"."udt_schema", "c"."udt_name")
    end as "database-type",
    "c"."ordinal_position" - 1 as "database-position",
    "c"."table_schema" as "table-schema",
    "c"."table_name" as "table-name",
    "pk"."column_name" is not null as "pk?",
    col_description(
        cast(
            cast(
                format(
                    '%I.%I',
                    cast("c"."table_schema" as text),
                    cast("c"."table_name" as text)
                ) as regclass
            ) as oid
        ),
        "c"."ordinal_position"
    ) as "field-comment",
    (("column_default" is null) or (lower("column_default") = 'null'))
    and ("is_nullable" = 'NO')
    and not (
        (("column_default" is not null) and ("column_default" like '%nextval(%'))
        or ("is_identity" <> 'NO')
    ) as "database-required",
    (("column_default" is not null) and ("column_default" like '%nextval(%'))
    or ("is_identity" <> 'NO') as "database-is-auto-increment"
from "information_schema"."columns" as "c"
left join
    (
        select "tc"."table_schema", "tc"."table_name", "kc"."column_name"
        from "information_schema"."table_constraints" as "tc"
        inner join
            "information_schema"."key_column_usage" as "kc"
            on ("tc"."constraint_name" = "kc"."constraint_name")
            and ("tc"."table_schema" = "kc"."table_schema")
            and ("tc"."table_name" = "kc"."table_name")
        where "tc"."constraint_type" = 'PRIMARY KEY'
    ) as "pk"
    on ("c"."table_schema" = "pk"."table_schema")
    and ("c"."table_name" = "pk"."table_name")
    and ("c"."column_name" = "pk"."column_name")
where
    c.table_schema !~ '^information_schema|catalog_history|pg_'
    and ("c"."table_schema" in ('public'))
union all
select
    "pa"."attname" as "name",
    case
        when "ptn"."nspname" in ('public', 'pg_catalog')
        then format('%s', "pt"."typname")
        else format('"%s"."%s"', "ptn"."nspname", "pt"."typname")
    end as "database-type",
    "pa"."attnum" - 1 as "database-position",
    "pn"."nspname" as "table-schema",
    "pc"."relname" as "table-name",
    false as "pk?",
    null as "field-comment",
    false as "database-required",
    false as "database-is-auto-increment"
from "pg_catalog"."pg_class" as "pc"
inner join "pg_catalog"."pg_namespace" as "pn" on "pn"."oid" = "pc"."relnamespace"
inner join "pg_catalog"."pg_attribute" as "pa" on "pa"."attrelid" = "pc"."oid"
inner join "pg_catalog"."pg_type" as "pt" on "pt"."oid" = "pa"."atttypid"
inner join "pg_catalog"."pg_namespace" as "ptn" on "ptn"."oid" = "pt"."typnamespace"
where ("pc"."relkind" = 'm') and ("pa"."attnum" >= 1) and ("pn"."nspname" in ('public'))
order by "table-schema" asc, "table-name" asc, "database-position" asc
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );
    Ok(())
}
