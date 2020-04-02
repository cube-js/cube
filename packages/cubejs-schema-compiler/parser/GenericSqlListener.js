// Generated from GenericSql.g4 by ANTLR 4.8
// jshint ignore: start
var antlr4 = require('antlr4/index');

// This class defines a complete listener for a parse tree produced by GenericSqlParser.
function GenericSqlListener() {
	antlr4.tree.ParseTreeListener.call(this);
	return this;
}

GenericSqlListener.prototype = Object.create(antlr4.tree.ParseTreeListener.prototype);
GenericSqlListener.prototype.constructor = GenericSqlListener;

// Enter a parse tree produced by GenericSqlParser#statement.
GenericSqlListener.prototype.enterStatement = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#statement.
GenericSqlListener.prototype.exitStatement = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#query.
GenericSqlListener.prototype.enterQuery = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#query.
GenericSqlListener.prototype.exitQuery = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#fromTables.
GenericSqlListener.prototype.enterFromTables = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#fromTables.
GenericSqlListener.prototype.exitFromTables = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#selectFields.
GenericSqlListener.prototype.enterSelectFields = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#selectFields.
GenericSqlListener.prototype.exitSelectFields = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#field.
GenericSqlListener.prototype.enterField = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#field.
GenericSqlListener.prototype.exitField = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#aliasField.
GenericSqlListener.prototype.enterAliasField = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#aliasField.
GenericSqlListener.prototype.exitAliasField = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#boolExp.
GenericSqlListener.prototype.enterBoolExp = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#boolExp.
GenericSqlListener.prototype.exitBoolExp = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#exp.
GenericSqlListener.prototype.enterExp = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#exp.
GenericSqlListener.prototype.exitExp = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#numeric.
GenericSqlListener.prototype.enterNumeric = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#numeric.
GenericSqlListener.prototype.exitNumeric = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#binaryOperator.
GenericSqlListener.prototype.enterBinaryOperator = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#binaryOperator.
GenericSqlListener.prototype.exitBinaryOperator = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#unaryOperator.
GenericSqlListener.prototype.enterUnaryOperator = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#unaryOperator.
GenericSqlListener.prototype.exitUnaryOperator = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#idPath.
GenericSqlListener.prototype.enterIdPath = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#idPath.
GenericSqlListener.prototype.exitIdPath = function(ctx) {
};


// Enter a parse tree produced by GenericSqlParser#identifier.
GenericSqlListener.prototype.enterIdentifier = function(ctx) {
};

// Exit a parse tree produced by GenericSqlParser#identifier.
GenericSqlListener.prototype.exitIdentifier = function(ctx) {
};



exports.GenericSqlListener = GenericSqlListener;