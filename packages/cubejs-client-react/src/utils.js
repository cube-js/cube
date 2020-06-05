export function moveKeyAtIndex(object, key, atIndex) {
  const keys = Object.keys(object);

  const entries = [];
  let index = 0;
  let j = 0;

  while (j < keys.length) {
    if (entries.length === atIndex) {
      entries.push([key, object[key]]);
      j++;
    } else {
      if (keys[index] !== key) {
        entries.push([keys[index], object[keys[index]]]);
        j++;
      }

      index++;
    }
  }
}
