export type MemberType = Record<string, 'time' | 'number' | 'string'>;

export function metaConfigToTypes(metaConfig: any[]) {
  const types: MemberType = {};
  
  metaConfig.forEach(({ config }) => {
    Object.entries<any>(config).forEach(([key, members]) => {
      if (['measures', 'dimensions', 'segments'].includes(key)) {
        members.forEach(({ name, type }) => {
          types[name] = type;
        });
      }
    });
  });
  
  return types;
}

export function unCapitalize(name: string) {
  return `${name[0].toLowerCase()}${name.slice(1)}`;
}
