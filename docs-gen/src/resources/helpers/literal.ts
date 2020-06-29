import { DeclarationReflection } from 'typedoc';
import { ReflectionKind, ReflectionType } from 'typedoc/dist/lib/models';
import { declarationTitle } from './declaration-title';
import { signatureTitle } from './signature-title';
import { spaces } from './spaces';

export function literal(this: DeclarationReflection) {
  const md = [];

  if (this.children) {
    this.children.forEach(child => {
      md.push(objectProperty(md, 0, child));
    });
  }
  return md.join('') + '\n';
}

function objectProperty(md: any[], spaceLength: number, property: DeclarationReflection) {
  if (property.type instanceof ReflectionType) {
    md.push(`${spaces(spaceLength)}* ${signatureTitle.call(property, false)}\n\n`);
    if (property.type.declaration) {
      md.push(objectProperty(md, spaceLength + 2, property.type.declaration));
    }
    if (property.type.declaration && property.type.declaration.signatures) {
      property.type.declaration.signatures.forEach(signature => {
        if (signature.kind !== ReflectionKind.CallSignature) {
          md.push(`${spaces(spaceLength)}* ${signatureTitle.call(signature, false)}\n\n`);
          if (signature.type instanceof ReflectionType) {
            md.push(objectProperty(md, spaceLength + 2, signature.type.declaration));
          }
        }
      });
    }
  } else {
    if (property.signatures) {
      property.signatures.forEach(signature => {
        md.push(`${spaces(spaceLength)}* ${signatureTitle.call(signature, false)}\n\n`);
        if (signature.type instanceof ReflectionType) {
          md.push(objectProperty(md, spaceLength + 2, signature.type.declaration));
        }
      });
    } else {
      if (property.kind !== ReflectionKind.TypeLiteral) {
        md.push(`${spaces(spaceLength)}* ${declarationTitle.call(property, false)}\n\n`);
      }
    }
  }
  if (property.children) {
    property.children.forEach(child => {
      md.push(objectProperty(md, property.kind === ReflectionKind.TypeLiteral ? spaceLength : spaceLength + 2, child));
    });
  }
}
