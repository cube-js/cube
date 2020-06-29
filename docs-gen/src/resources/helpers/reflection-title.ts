import { PageEvent } from 'typedoc/dist/lib/output/events';

export function reflectionTitle(this: PageEvent) {
  const title = [];
  if (this.model.kindString) {
    title.push(`${this.model.kindString}:`);
  }
  title.push(this.model.name);
  if (this.model.typeParameters) {
    const typeParameters = this.model.typeParameters.map((typeParameter) => typeParameter.name).join(', ');
    title.push(`‹**${typeParameters}**›`);
  }
  return title.join(' ');
}
