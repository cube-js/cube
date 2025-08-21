/* eslint-disable no-underscore-dangle,camelcase */
import { ErrorListener, CommonTokenStream, CharStream, RuleNode } from 'antlr4';
import * as t from '@babel/types';

import Python3Lexer from './Python3Lexer';
import Python3Parser, {
  // eslint-disable-next-line camelcase
  File_inputContext,
  Double_string_template_atomContext,
  String_templateContext,
  AtomContext,
  Atom_exprContext,
  Expr_stmtContext,
  TrailerContext,
  VfpdefContext,
  VarargslistContext,
  LambdefContext,
  Single_string_template_atomContext,
  ArglistContext,
  CallArgumentsContext,
} from './Python3Parser';
import { UserError } from '../compiler/UserError';
import Python3ParserVisitor from './Python3ParserVisitor';

const nodeVisitor = <R>(visitor: { visitNode: (node: RuleNode, children: R[]) => R }): Python3ParserVisitor<R> => ({
  // TODO null -- note used?
  visit: () => <any>null,
  visitTerminal: <any>null,
  visitErrorNode: <any>null,
  visitChildren(node) {
    if (!node) {
      return <any>null;
    }

    const result: R[] = [];
    if ((node as any).children) {
      for (let i = 0; i < (node as any).children.length; i++) {
        const child: any = (node as any).children[i];
        if (child && child.children && child.children.length > 0) {
          result.push(child.accept(this));
        }
      }
    }
    return visitor.visitNode(node, result);
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

export class PythonParser {
  // eslint-disable-next-line camelcase
  protected readonly ast: File_inputContext;

  protected errors: SyntaxError[] = [];

  public constructor(
    protected readonly codeString: string
  ) {
    this.ast = this.parse();
  }

  protected parse() {
    const { codeString } = this;

    const chars = new CharStream(codeString);
    chars.getText = (start, stop) => {
      if (stop >= chars.size) {
        stop = chars.size - 1;
      }

      if (start >= chars.size) {
        return '';
      } else {
        return codeString.slice(start, stop + 1);
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

    const lexer = new Python3Lexer(chars);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new ExprErrorListener());

    const commonTokenStream = new CommonTokenStream(lexer);

    const parser = new Python3Parser(
      commonTokenStream
    );
    parser.buildParseTrees = true;
    parser.removeErrorListeners();
    parser.addErrorListener(new ExprErrorListener());

    return parser.file_input();
  }

  public transpileToJs(): t.Program {
    return this.ast.accept(nodeVisitor<any>({
      visitNode: (node, children) => {
        const singleNodeReturn = () => {
          if (children.length === 1) {
            return children[0];
          } else {
            throw new UserError(`Unsupported Python multiple children node: ${node.constructor.name}: ${node.getText()}`);
          }
        };

        if (node instanceof File_inputContext) {
          return t.program(children);
        } else if (node instanceof Expr_stmtContext) {
          if (children.length === 1) {
            return t.expressionStatement(children[0]);
          } else {
            throw new UserError(`Unsupported Python multiple children node: ${node.constructor.name}: ${node.getText()}`);
          }
        } else if (
          node instanceof Double_string_template_atomContext ||
          node instanceof Single_string_template_atomContext
        ) {
          if ((node.test() || node.star_expr()) && children.length === 1) {
            return children[0];
          }
          return t.templateElement({ raw: node.getText(), cooked: node.getText() });
        } else if (node instanceof String_templateContext) {
          if (children[children.length - 1].type === 'TemplateElement') {
            children[children.length - 1].tail = true;
          } else {
            children.push(t.templateElement({ raw: '', cooked: '' }));
          }
          if (children[0].type !== 'TemplateElement') {
            children.unshift(t.templateElement({ raw: '', cooked: '' }));
          }
          return t.templateLiteral(children.filter(c => c.type === 'TemplateElement'), children.filter(c => c.type !== 'TemplateElement'));
        } else if (node instanceof Atom_exprContext) {
          if (children.length === 1) {
            return children[0];
          } else if (children.length > 1) {
            let expr = children[0];
            for (let i = 1; i < children.length; i++) {
              if (children[i].call) {
                expr = t.callExpression(expr, children[i].call);
              } else if (children[i].identifier) {
                expr = t.memberExpression(expr, children[i].identifier);
              } else {
                throw new Error(`Unexpected trailer child: ${children[i]}`);
              }
            }
            return expr;
          } else {
            throw new UserError(`Empty Python atom_expr node: ${node.constructor.name}: ${node.getText()}`);
          }
        } else if (node instanceof AtomContext) {
          const name = node.NAME();
          const stringList = node.STRING_list();
          const number = node.NUMBER();

          if (name) {
            return t.identifier(name.getText());
          } else if (stringList && stringList.length) {
            return t.stringLiteral(stringList.map(s => this.stripQuotes(s.getText())).join(''));
          } else if (number) {
            const numText = number.getText();
            const numValue = parseFloat(numText);
            return t.numericLiteral(numValue);
          } else {
            return singleNodeReturn();
          }
        } else if (node instanceof CallArgumentsContext) {
          const argsList = node.arglist();
          if (argsList) {
            // arglist have a single child: arguments _list_
            const args = children[0];
            return { call: args };
          } else {
            return { call: [] };
          }
        } else if (node instanceof TrailerContext) {
          const name = node.NAME();
          const argsList = node.callArguments();
          if (argsList) {
            // trailer with callArguments have a single child: CallArgumentsContext
            // which was already processed (see other if branch)
            return children[0];
          } else if (name) {
            return { identifier: t.identifier(name.getText()) };
          } else {
            throw new UserError(`Unsupported Python Trailer children node: ${node.constructor.name}: ${node.getText()}`);
          }
        } else if (node instanceof VfpdefContext) {
          const name = node.NAME();
          if (name) {
            return t.identifier(name.getText());
          } else {
            throw new UserError(`Unsupported Python vfpdef children node: ${node.constructor.name}: ${node.getText()}`);
          }
        } else if (node instanceof VarargslistContext) {
          return { args: children };
        } else if (node instanceof LambdefContext) {
          return t.arrowFunctionExpression(children[0].args, children[1]);
        } else if (node instanceof ArglistContext) {
          return children;
        } else {
          return singleNodeReturn();
        }
      }
    }));
  }

  public stripQuotes(text: string): string {
    if (text[0] === '"' && text[text.length - 1] === '"' || text[0] === '\'' && text[text.length - 1] === '\'') {
      return text.slice(1, text.length - 1);
    } else {
      return text;
    }
  }

  public canParse() {
    return !this.errors.length;
  }

  public throwErrorsIfAny() {
    if (this.errors.length) {
      throw new UserError(
        `Python Parsing Error:\n${this.errors.map(({ msg, column, line }) => `${line}:${column} ${msg}`).join('\n')}`
      );
    }
  }
}
