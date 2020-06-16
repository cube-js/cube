export function reorder(list, sourceIndex, destinationIndex) {
  const result = [...list];
  const [removed] = result.splice(sourceIndex, 1);
  result.splice(destinationIndex, 0, removed);

  return result;
}
