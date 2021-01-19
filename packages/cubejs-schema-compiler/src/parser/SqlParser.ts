/* eslint-disable no-underscore-dangle */
import R from 'ramda';
import { ANTLRErrorListener, CommonTokenStream, CharStreams } from 'antlr4ts';
import { RuleNode, ParseTree } from 'antlr4ts/tree';

import { GenericSqlLexer } from './GenericSqlLexer';
import {
  GenericSqlParser,
  QueryContext,
  SelectFieldsContext,
  IdPathContext,
  AliasFieldContext,
  StatementContext,
} from './GenericSqlParser';
import { UserError } from '../compiler/UserError';
import { GenericSqlVisitor } from './GenericSqlVisitor';

const nodeVisitor = <Result = void>(visitor: { visitNode: (node: RuleNode) => void }): GenericSqlVisitor<void> => ({
  visit: () => {
    //
  },
  visitTerminal: () => {
    //
  },
  visitErrorNode: () => {
    //
  },
  visitChildren(node) {
    if (!node) {
      return;
    }

    visitor.visitNode(node);

    for (let i = 0; i < node.childCount; i++) {
      const child = node.getChild(i);
      if (child && child.childCount) {
        child.accept(this);
      }
    }
  }
});

interface SyntaxError {
  msg: string;
  column: number;
  err: any;
  line: number;
  recognizer: any;
  offendingSymbol: any;
}

export class SqlParser {
  protected readonly ast: StatementContext;

  protected errors: SyntaxError[] = [];

  public constructor(
    protected readonly sql: string
  ) {
    this.ast = this.parse();
  }

  protected static sqlUpperCase(sql) {
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

  protected parse() {
    const { sql } = this;

    const chars = CharStreams.fromString(SqlParser.sqlUpperCase(sql));
    chars.getText = (interval) => {
      const start = interval.a;
      let stop = interval.b;

      if (stop >= chars.size) {
        stop = chars.size - 1;
      }

      if (start >= chars.size) {
        return '';
      } else {
        return sql.slice(start, stop + 1);
      }
    };

    const { errors } = this;

    class ExprErrorListener implements ANTLRErrorListener<number> {
      public syntaxError(recognizer, offendingSymbol, line, column, msg, err) {
        errors.push({
          msg, column, err, line, recognizer, offendingSymbol
        });
      }
    }

    const lexer = new GenericSqlLexer(chars);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ExprErrorListener());

    const parser = new GenericSqlParser(
      new CommonTokenStream(lexer)
    );
    parser.buildParseTree = true;
    parser.removeErrorListeners();
    parser.addErrorListener(new ExprErrorListener());

    return parser.statement();
  }

  public canParse() {
    return !this.errors.length;
  }

  public throwErrorsIfAny() {
    if (this.errors.length) {
      throw new UserError(
        `SQL Parsing Error:\n${this.errors.map(({ msg, column, line }) => `${line}:${column} ${msg}`).join('\n')}`
      );
    }
  }

  public isSimpleAsteriskQuery(): boolean {
    if (!this.canParse()) {
      return false;
    }

    let result = false;

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext) {
          const selectItems = ctx.tryGetRuleContext(0, SelectFieldsContext);
          if (selectItems && selectItems.text === '*') {
            result = true;
          }
        }
      }
    }));

    return result;
  }

  public extractWhereConditions(tableAlias): string {
    this.throwErrorsIfAny();

    let result = '';

    const { sql } = this;

    let cursor = 0;
    let end = 0;
    let originalAlias;

    const whereBuildingVisitor = nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof IdPathContext) {
          result += sql.substring(cursor, ctx.start.startIndex);
          cursor = ctx.start.startIndex;

          const child = ctx.getChild(0);
          if (child && child.text === originalAlias) {
            const withoutFirst = R.drop(1, <ParseTree[]>ctx.children);
            result += [tableAlias].concat(withoutFirst.map(c => c.text)).join('');
            cursor = <number>ctx.stop?.stopIndex + 1;
          } else if (ctx.childCount === 1) {
            result += [tableAlias, '.'].concat(ctx.children?.map(c => c.text)).join('');
            cursor = <number>ctx.stop?.stopIndex + 1;
          } else {
            result += sql.substring(cursor, ctx.stop?.stopIndex);
            cursor = <number>ctx.stop?.stopIndex;
          }
        }
      }
    });

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx._from && ctx._where) {
          const aliasField = ctx._from.getRuleContext(0, AliasFieldContext);
          const lastNode = aliasField.getChild(aliasField.childCount - 1);
          if (lastNode instanceof IdPathContext) {
            originalAlias = lastNode.getChild(lastNode.childCount - 1).text;
          } else {
            originalAlias = lastNode.text;
          }

          cursor = ctx._where.start.startIndex;
          end = <number>ctx._where.stop?.stopIndex + 1;
          ctx._where.accept(whereBuildingVisitor);
        }
      }
    }));

    result += sql.substring(cursor, end);
    return result;
  }

  public extractTableFrom(): string|null {
    this.throwErrorsIfAny();

    let result: string|null = null;

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx._from) {
          const aliasField = ctx._from.getRuleContext(0, AliasFieldContext);
          result = aliasField.getChild(0).text;
        }
      }
    }));

    return result;
  }
}
