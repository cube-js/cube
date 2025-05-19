/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 by Bart Kiers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Project : python3-parser; an ANTLR4 grammar for Python 3 https://github.com/bkiers/python3-parser
 * Developed by : Bart Kiers, bart@big-o.nl
 */
parser grammar Python3Parser;

// All comments that start with "///" are copy-pasted from The Python Language Reference:
// https://docs.python.org/3.3/reference/grammar.html

options {
    tokenVocab=Python3Lexer;
}

/*
 * parser rules
 */

file_input: (NEWLINE | stmt)* EOF;
single_input: NEWLINE | simple_stmt | compound_stmt NEWLINE;
eval_input: testlist NEWLINE* EOF;

decorator: AT dotted_name ( OPEN_PAREN (arglist)? CLOSE_PAREN)? NEWLINE;
decorators: decorator+;
decorated: decorators (classdef | funcdef | async_funcdef);

async_funcdef: ASYNC funcdef;
funcdef: DEF NAME parameters (ARROW test)? COLON suite;

parameters: OPEN_PAREN (typedargslist)? CLOSE_PAREN;
typedargslist: (
		tfpdef (ASSIGN test)? (COMMA tfpdef (ASSIGN test)?)* (
			COMMA (
				'*' (tfpdef)? (COMMA tfpdef ('=' test)?)* (
					COMMA ('**' tfpdef (COMMA)?)?
				)?
				| '**' tfpdef (COMMA)?
			)?
		)?
		| '*' (tfpdef)? (COMMA tfpdef ('=' test)?)* (
			COMMA ('**' tfpdef (COMMA)?)?
		)?
		| '**' tfpdef (COMMA)?
	);
tfpdef: NAME (':' test)?;
varargslist: (
		vfpdef ('=' test)? (COMMA vfpdef ('=' test)?)* (
			COMMA (
				'*' (vfpdef)? (COMMA vfpdef ('=' test)?)* (
					COMMA ('**' vfpdef (COMMA)?)?
				)?
				| '**' vfpdef (COMMA)?
			)?
		)?
		| '*' (vfpdef)? (COMMA vfpdef ('=' test)?)* (
			COMMA ('**' vfpdef (COMMA)?)?
		)?
		| '**' vfpdef (COMMA)?
	);
vfpdef: NAME;

stmt: simple_stmt | compound_stmt;
simple_stmt: small_stmt (';' small_stmt)* (';')? NEWLINE;
small_stmt: (
		expr_stmt
		| del_stmt
		| pass_stmt
		| flow_stmt
		| import_stmt
		| global_stmt
		| nonlocal_stmt
		| assert_stmt
	);
expr_stmt:
	testlist_star_expr (
		annassign
		| augassign (yield_expr | testlist)
		| ('=' (yield_expr | testlist_star_expr))*
	);
annassign: ':' test ('=' test)?;
testlist_star_expr: (test | star_expr) (COMMA (test | star_expr))* (
		COMMA
	)?;
augassign: (
		'+='
		| '-='
		| '*='
		| '@='
		| '/='
		| '%='
		| '&='
		| '|='
		| '^='
		| '<<='
		| '>>='
		| '**='
		| '//='
	);
// For normal and annotated assignments, additional restrictions enforced by the interpreter
del_stmt: DEL exprlist;
pass_stmt: 'pass';
flow_stmt:
	break_stmt
	| continue_stmt
	| return_stmt
	| raise_stmt
	| yield_stmt;
break_stmt: 'break';
continue_stmt: 'continue';
return_stmt: 'return' (testlist)?;
yield_stmt: yield_expr;
raise_stmt: 'raise' (test ('from' test)?)?;
import_stmt: import_name | import_from;
import_name: 'import' dotted_as_names;
// note below: the ('.' | '...') is necessary because '...' is tokenized as ELLIPSIS
import_from: (
		'from' (('.' | '...')* dotted_name | ('.' | '...')+) 'import' (
			'*'
			| '(' import_as_names ')'
			| import_as_names
		)
	);
import_as_name: NAME ('as' NAME)?;
dotted_as_name: dotted_name ('as' NAME)?;
import_as_names: import_as_name (COMMA import_as_name)* (COMMA)?;
dotted_as_names: dotted_as_name (COMMA dotted_as_name)*;
dotted_name: NAME ('.' NAME)*;
global_stmt: 'global' NAME (COMMA NAME)*;
nonlocal_stmt: 'nonlocal' NAME (COMMA NAME)*;
assert_stmt: 'assert' test (COMMA test)?;

compound_stmt:
	if_stmt
	| while_stmt
	| for_stmt
	| try_stmt
	| with_stmt
	| funcdef
	| classdef
	| decorated
	| async_stmt;
async_stmt: ASYNC (funcdef | with_stmt | for_stmt);
if_stmt:
	'if' test ':' suite ('elif' test ':' suite)* (
		'else' ':' suite
	)?;
while_stmt: 'while' test ':' suite ('else' ':' suite)?;
for_stmt:
	'for' exprlist 'in' testlist ':' suite ('else' ':' suite)?;
try_stmt: (
		'try' ':' suite (
			(except_clause ':' suite)+ ('else' ':' suite)? (
				'finally' ':' suite
			)?
			| 'finally' ':' suite
		)
	);
with_stmt: 'with' with_item (COMMA with_item)* ':' suite;
with_item: test ('as' expr)?;
// NB compile.c makes sure that the default except clause is last
except_clause: 'except' (test ('as' NAME)?)?;
suite: simple_stmt | NEWLINE INDENT stmt+ DEDENT;

test: or_test ('if' or_test 'else' test)? | lambdef;
test_nocond: or_test | lambdef_nocond;
lambdef: LAMBDA (varargslist)? COLON test;
lambdef_nocond: LAMBDA (varargslist)? COLON test_nocond;
or_test: and_test ('or' and_test)*;
and_test: not_test ('and' not_test)*;
not_test: 'not' not_test | comparison;
comparison: expr (comp_op expr)*;
// <> isn't actually a valid comparison operator in Python. It's here for the sake of a __future__
// import described in PEP 401 (which really works :-)
comp_op:
	'<'
	| '>'
	| '=='
	| '>='
	| '<='
	| '<>'
	| '!='
	| 'in'
	| 'not' 'in'
	| 'is'
	| 'is' 'not';
star_expr: '*' expr;
expr: xor_expr ('|' xor_expr)*;
xor_expr: and_expr ('^' and_expr)*;
and_expr: shift_expr ('&' shift_expr)*;
shift_expr: arith_expr (('<<' | '>>') arith_expr)*;
arith_expr: term (('+' | '-') term)*;
term: factor (('*' | '@' | '/' | '%' | '//') factor)*;
factor: ('+' | '-' | '~') factor | power;
power: atom_expr ('**' factor)?;
atom_expr: (AWAIT)? atom trailer*;
atom: (
		'(' (yield_expr | testlist_comp)? ')'
		| '[' (testlist_comp)? ']'
		| '{' (dictorsetmaker)? '}'
		| NAME
		| NUMBER
		| string_template+
		| STRING+
		| '...'
		| 'None'
		| 'True'
		| 'False'
	);
testlist_comp: (test | star_expr) (
		comp_for
		| (COMMA (test | star_expr))* (COMMA)?
	);
trailer: callArguments | '[' subscriptlist ']' | '.' NAME;
subscriptlist: subscript (COMMA subscript)* (COMMA)?;
subscript: test | (test)? ':' (test)? (sliceop)?;
sliceop: ':' (test)?;
exprlist: (expr | star_expr) (COMMA (expr | star_expr))* (COMMA)?;
testlist: test (COMMA test)* (COMMA)?;
dictorsetmaker: (
		(
			(test ':' test | '**' expr) (
				comp_for
				| (COMMA (test ':' test | '**' expr))* (COMMA)?
			)
		)
		| (
			(test | star_expr) (
				comp_for
				| (COMMA (test | star_expr))* (COMMA)?
			)
		)
	);

classdef: 'class' NAME ('(' (arglist)? ')')? ':' suite;

callArguments: '(' (arglist)? ')';

arglist: argument (COMMA argument)* (COMMA)?;

// The reason that keywords are test nodes instead of NAME is that using NAME results in an
// ambiguity. ast.c makes sure it's a NAME. "test '=' test" is really "keyword '=' test", but we
// have no such token. These need to be in a single rule to avoid grammar that is ambiguous to our
// LL(1) parser. Even though 'test' includes '*expr' in star_expr, we explicitly match '*' here,
// too, to give it proper precedence. Illegal combinations and orderings are blocked in ast.c:
// multiple (test comp_for) arguments are blocked; keyword unpackings that precede iterable
// unpackings are blocked; etc.
argument: (
		test (comp_for)?
		| test '=' test
		| '**' test
		| '*' test
	);

comp_iter: comp_for | comp_if;
comp_for: (ASYNC)? 'for' exprlist 'in' or_test (comp_iter)?;
comp_if: 'if' test_nocond (comp_iter)?;

// not used in grammar, but may appear in "node" passed from Parser to Compiler
encoding_decl: NAME;

yield_expr: 'yield' (yield_arg)?;
yield_arg: 'from' test | testlist;

string_template:
    SINGLE_QUOTE_SHORT_TEMPLATE_STRING_START single_string_template_atom* SINGLE_QUOTE_SHORT_TEMPLATE_STRING_END
    | SINGLE_QUOTE_LONG_TEMPLATE_STRING_START single_string_template_atom* SINGLE_QUOTE_LONG_TEMPLATE_STRING_END
    | DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_START double_string_template_atom* DOUBLE_QUOTE_SHORT_TEMPLATE_STRING_END
    | DOUBLE_QUOTE_LONG_TEMPLATE_STRING_START double_string_template_atom* DOUBLE_QUOTE_LONG_TEMPLATE_STRING_END;

single_string_template_atom:
    SINGLE_QUOTE_STRING_ATOM
    | OPEN_BRACE (test | star_expr) TEMPLATE_CLOSE_BRACE
    ;

double_string_template_atom
    : DOUBLE_QUOTE_STRING_ATOM
    | OPEN_BRACE (test | star_expr) TEMPLATE_CLOSE_BRACE
    ;

