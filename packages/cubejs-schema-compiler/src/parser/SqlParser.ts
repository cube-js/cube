/* eslint-disable no-underscore-dangle */
import R from 'ramda';
import { ErrorListener, CommonTokenStream, CharStream, RuleNode, ParseTree } from 'antlr4';

import GenericSqlLexer from './GenericSqlLexer';
import GenericSqlParser, {
  QueryContext,
  SelectFieldsContext,
  IdPathContext,
  AliasFieldContext,
  StatementContext,
} from './GenericSqlParser';
import { UserError } from '../compiler/UserError';
import GenericSqlVisitor from './GenericSqlVisitor';

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

    if ((node as any).children) {
      for (let i = 0; i < (node as any).children.length; i++) {
        const child: any = (node as any).children[i];
        if (child && child.children && child.children.length > 0) {
          child.accept(this);
        }
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

    const chars = new CharStream(SqlParser.sqlUpperCase(sql));
    chars.getText = (start, stop) => {
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

    class ExprErrorListener implements ErrorListener<number> {
      public syntaxError(recognizer, offendingSymbol, line, column, msg, err) {
        errors.push({
          msg, column, err, line, recognizer, offendingSymbol
        });
      }

      public reportAmbiguity(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs) {
        // Optional: log ambiguity warnings if needed
      }

      public reportAttemptingFullContext(recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs) {
        // Optional: log full context attempts if needed
      }

      public reportContextSensitivity(recognizer, dfa, startIndex, stopIndex, prediction, configs) {
        // Optional: log context sensitivity if needed
      }
    }

    const lexer = new GenericSqlLexer(chars);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ExprErrorListener());

    const parser = new GenericSqlParser(
      new CommonTokenStream(lexer)
    );
    parser.buildParseTrees = true;
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
          const selectItems = ctx.getTypedRuleContext(SelectFieldsContext, 0);
          if (selectItems && selectItems.getText() === '*') {
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
          result += sql.substring(cursor, ctx.start.start);
          cursor = ctx.start.start;

          const { children } = ctx as any;
          const child = children ? children[0] : null;
          if (child && child.getText() === originalAlias) {
            const withoutFirst = R.drop(1, <ParseTree[]>ctx.children || []);
            result += [tableAlias].concat(withoutFirst.map((c: ParseTree) => c.getText())).join('');
            cursor = (ctx.stop?.stop || 0) + 1;
          } else if (children && children.length === 1) {
            result += [tableAlias, '.'].concat(children?.map((c: ParseTree) => c.getText())).join('');
            cursor = (ctx.stop?.stop || 0) + 1;
          } else {
            result += sql.substring(cursor, ctx.stop?.stop);
            cursor = <number>ctx.stop?.stop;
          }
        }
      }
    });

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx._from_ && ctx._where) {
          const aliasField = ctx._from_.getTypedRuleContext(AliasFieldContext, 0);
          const lastNode: any = (aliasField as any).children ? (aliasField as any).children[(aliasField as any).children.length - 1] : null;
          if (lastNode instanceof IdPathContext) {
            originalAlias = lastNode.children ? lastNode.children[lastNode.children.length - 1].getText() : '';
          } else {
            originalAlias = lastNode ? lastNode.getText() : '';
          }

          cursor = ctx._where.start.start;
          end = <number>(ctx._where.stop?.stop || 0) + 1;
          ctx._where.accept(whereBuildingVisitor);
        }
      }
    }));

    result += sql.substring(cursor, end);
    return result;
  }

  public extractTableFrom(): string | null {
    this.throwErrorsIfAny();

    let result: string | null = null;

    this.ast.accept(nodeVisitor({
      visitNode(ctx) {
        if (ctx instanceof QueryContext && ctx._from_) {
          const aliasField = ctx._from_.getTypedRuleContext(AliasFieldContext, 0);
          result = (aliasField as any).children ? (aliasField as any).children[0].getText() : null;
        }
      }
    }));

    return result;
  }
}
