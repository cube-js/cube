import {
  ArrayType,
  IntersectionType,
  IntrinsicType,
  ReferenceType,
  ReflectionType,
  StringLiteralType,
  TupleType,
  TypeOperatorType,
  UnionType,
} from 'typedoc/dist/lib/models/types';

import MarkdownTheme from '../../theme';
import { ReflectionKind } from 'typedoc';
import { LinkPlugin } from '../../plugins/LinkPlugin';

export function type(
  this:
    | ArrayType
    | IntersectionType
    | IntrinsicType
    | ReferenceType
    | StringLiteralType
    | TupleType
    | UnionType
    | TypeOperatorType
) {
  if (this instanceof ReferenceType && (this.reflection || (this.name && this.typeArguments))) {
    return getReferenceType(this);
  }

  if (this instanceof ArrayType && this.elementType) {
    return getArrayType(this);
  }

  if (this instanceof UnionType && this.types) {
    return getUnionType(this);
  }

  if (this instanceof IntersectionType && this.types) {
    return getIntersectionType(this);
  }

  if (this instanceof TupleType && this.elements) {
    return getTupleType(this);
  }

  if (this instanceof IntrinsicType && this.name) {
    return getIntrinsicType(this);
  }

  if (this instanceof StringLiteralType && this.value) {
    return getStringLiteralType(this);
  }

  if (this instanceof TypeOperatorType || this instanceof ReflectionType) {
    return this;
  }

  return this;
}

function getReferenceType(model: ReferenceType) {
  const md = [];

  // let parentName = '';
  // if (model.reflection?.kindOf(ReflectionKind.TypeAlias) && !(model.reflection as any).stickToParent) {
  //   parentName = 'Types';
  // }

  md.push(
    model.reflection ? LinkPlugin.toLink(model.name, model.reflection) : model.name
    // model.reflection
    //   ? `[${model.reflection.name}](${MarkdownTheme.handlebars.helpers.relativeURL(
    //       LinkPlugin.anchorName(parentName + model.reflection.name)
    //     )})`
    //   : model.name
  );

  if (model.typeArguments) {
    md.push(`‹${model.typeArguments.map((typeArgument) => `${type.call(typeArgument)}`).join(', ')}›`);
  }

  return md.join('');
}

function getArrayType(model: ArrayType) {
  return `${type.call(model.elementType)}[]`;
}

function getUnionType(model: UnionType) {
  return model.types.map((unionType) => type.call(unionType)).join(' | ');
}

function getIntersectionType(model: IntersectionType) {
  return model.types.map((intersectionType) => type.call(intersectionType)).join(' & ');
}

function getTupleType(model: TupleType) {
  return `[${model.elements.map((element) => type.call(element)).join(', ')}]`;
}

function getIntrinsicType(model: IntrinsicType) {
  return model.name;
}

function getStringLiteralType(model: StringLiteralType) {
  return `"${model.value}"`;
}
