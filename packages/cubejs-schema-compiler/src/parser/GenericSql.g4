grammar GenericSql;

statement:
    query EOF | '(' query ')' EOF;

query:
    SELECT selectFields
    FROM from=fromTables
    (WHERE where=boolExp)?;

fromTables:
    aliasField;

selectFields:
    (field (',' field)*);

field:
    selectField | ASTERISK;

selectField:
    exp (AS? identifier)?;

aliasField:
    idPath (AS? identifier)?;

boolExp:
    exp |
    boolExp AND boolExp |
    boolExp OR boolExp |
    NOT boolExp
    ;

exp:
    exp binaryOperator exp |
    exp unaryOperator |
    idPath |
    identifier '(' (exp (',' exp)*) ')' |
    CAST '(' exp AS identifier ')' |
    REGEXP STRING |
    STRING |
    numeric |
    identifier |
    INDEXED_PARAM |
    PARAM_PLACEHOLDER |
    '(' exp ')'
    ;

numeric:
    DIGIT+ ('.' DIGIT+)? |
    '.' DIGIT+;

binaryOperator:
    LT | LTE | GT | GTE | EQUALS | NOT_EQUALS;

unaryOperator:
    IS NULL | IS NOT NULL;

idPath:
    identifier ('.' identifier)*;

identifier:
    ID |
    QUOTED_ID;

SELECT: 'SELECT';
ASTERISK: '*';
FROM: 'FROM';
WHERE: 'WHERE';
AND: 'AND';
OR: 'OR';
NOT: 'NOT';
AS: 'AS';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';
EQUALS: '=';
NOT_EQUALS: '<>' | '!=';
IS: 'IS';
NULL: 'NULL';
CAST: 'CAST';
REGEXP: 'REGEXP';

INDEXED_PARAM: '$' [0-9]+ '$';
PARAM_PLACEHOLDER: '?';
ID: [A-Z_@] [A-Z_@0-9]*;
DIGIT: [0-9];
QUOTED_ID: ('"' (~'"')* '"') | ('`' (~'`')* '`');
STRING: ('\'' (~ '\'' | '\'\'')* '\'');


WHITESPACE: [ \t\r\n] -> channel(HIDDEN);
COMMENT: ('--' (~[\r\n])* ('\r'? '\n' | EOF)) -> channel(HIDDEN);
MULTILINE_COMMENT: ('/*' .*? '*/') -> channel(HIDDEN);
