import { ParameterReflection } from 'typedoc';
import { ReflectionType, UnionType } from 'typedoc/dist/lib/models';

import { signatureTitle } from './signature-title';
import { type } from './type';

export default function paramTypeToString(parameter: ParameterReflection) {
  let typeOut;
  
  if (parameter.type instanceof ReflectionType && parameter.type.toString() === 'function') {
    const declarations = parameter.type.declaration.signatures?.map((sig) => signatureTitle.call(sig, false, true));
    typeOut = declarations.join(' | ').replace(/\n/, '');
  } else if (parameter.type instanceof UnionType) {
    typeOut = parameter.type.types
      .map((currentType) => {
        if (currentType instanceof ReflectionType) {
          const declarations = currentType.declaration.signatures?.map((sig) => signatureTitle.call(sig, false, true));
          return declarations.join(' | ').replace(/\n/, '');
        }
        return type.call(currentType);
      })
      .join(' | ');
  } else {
    typeOut = type.call(parameter.type);
  }
  
  return typeOut;
}
