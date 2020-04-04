const antlr4 = require('antlr4');
const R = require('ramda');
const { GenericSqlLexer } = require('./GenericSqlLexer');
const { GenericSqlParser } = require('./GenericSqlParser');
const UserError = require('../compiler/UserError');

const {
  QueryContext, SelectFieldsContext, IdPathContext, AliasFieldContext
} = GenericSqlParser;

const nodeVisitor = (visitor) => ({
  visitChildren(ctx) {
    if (!ctx) {
      return;
    }

    visitor.visitNode(ctx);

    if (ctx.children) {
      ctx.children.forEach(child => {
        if (child.children && child.children.length) {
          child.accept(this);
        }
      });
    }
  }
});

class SqlParser {
  constructor(sql) {
    this.sql = sql;
    this.ast = this.parse();
  }

  static sqlUpperCase(sql) {
    let result = '';
    let openChar;
    for (let i = 0; i < sql.length; i++) {
      if (openChar) {
        if (openChar === '\'' && sql[i] === openChar && sql[i + 1] === openChar) {
          result += sql[i];
          i++;
        } else if (sql[i] === openChar) {
          openChar = null;
        }
        result += sql[i];
      } else {
        if (sql[i] === '\'' || sql[i] === '"' || sql[i] === '`') {
          openChar = sql[i];
        }
        result += sql[i].toUpperCase();
      }
    }
    if (openChar) {
      throw new Error(`Unterminated string: ${sql}`);
    }
    return result;
  }

  parse() {
    const { sql } = this;
    const chars = new antlr4.InputStream(SqlParser.sqlUpperCase(sql));
    chars.getText = (start, stop) => {
      // eslint-disable-next-line no-underscore-dangle
      if (stop >= this._size) {
        // eslint-disable-next-line no-underscore-dangle
        stop = this._size - 1;
      }
      // eslint-disable-next-line no-underscore-dangle
      if (start >= this._size) {
        return "";
      } else {
        return sql.slice(start, stop + 1);
      }
    };

    const errors = [];
    this.errors = errors;

    class ExprErrorListener extends antlr4.error.ErrorListener {
      syntaxError(recognizer, offendingSymbol, line, column, msg, err) {
        errors.push({
          msg, column, err, line, recognizer, offendingSymbol
        });
      }
    }

    const lexer = new GenericSqlLexer(chars);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ExprErrorListener());
    const tokens = new antlr4.CommonTokenStream(lexer);
    const parser = new GenericSqlParser(tokens);
    parser.buildParseTrees = true;
    parser.removeErrorListeners();
    parser.addErrorListener(new ExprErrorListener());

    return parser.statement();
  }

  canParse() {
    return !this.errors.length;
  }

  throwErrorsIfAny() {
    if (this.errors.length) {
      throw new UserError(`SQL Parsing Error:\n${this.errors.map(({ msg, column, line }) => `${line}:${column} ${msg}`).join('\n')}`);
    }
  }

  isSimpleAsteriskQuery() {
    if (!this.canParse()) {
      return false;
    }

    let result = false;

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext) {
          const selectItems = ctx.getTypedRuleContexts(SelectFieldsContext);
          if (selectItems.length === 1 && selectItems[0].getText() === '*') {
            result = true;
          }
        }
      }
    }));
    return result;
  }

  extractWhereConditions(tableAlias) {
    this.throwErrorsIfAny();
    let result = '';

    const { sql } = this;

    let cursor = 0;
    let end = 0;
    let originalAlias;

    const whereBuildingVisitor = nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof IdPathContext) {
          result += sql.substring(cursor, ctx.start.start);
          cursor = ctx.start.start;
          if (ctx.children[0].getText() === originalAlias) {
            const withoutFirst = R.drop(1, ctx.children);
            result += [tableAlias].concat(withoutFirst.map(c => c.getText())).join('');
            cursor = ctx.stop.stop + 1;
          } else if (ctx.children.length === 1) {
            result += [tableAlias, '.'].concat(ctx.children.map(c => c.getText())).join('');
            cursor = ctx.stop.stop + 1;
          } else {
            result += sql.substring(cursor, ctx.stop.stop);
            cursor = ctx.stop.stop;
          }
        }
      }
    });

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx.from && ctx.where) {
          const aliasField = ctx.from.getTypedRuleContexts(AliasFieldContext)[0];
          const lastNode = aliasField.children[aliasField.children.length - 1];
          if (lastNode instanceof IdPathContext) {
            originalAlias = lastNode.children[lastNode.children.length - 1].getText();
          } else {
            originalAlias = lastNode.getText();
          }
          cursor = ctx.where.start.start;
          end = ctx.where.stop.stop + 1;
          ctx.where.accept(whereBuildingVisitor);
        }
      }
    }));
    result += sql.substring(cursor, end);
    return result;
  }

  extractTableFrom() {
    this.throwErrorsIfAny();
    let result = null;

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx.from) {
          const aliasField = ctx.from.getTypedRuleContexts(AliasFieldContext)[0];
          result = aliasField.children[0].getText();
        }
      }
    }));
    return result;
  }
}

module.exports = SqlParser;
