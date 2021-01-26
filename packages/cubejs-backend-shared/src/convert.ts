export function formatDuration(time: number) {
  const hours = Math.floor(time / 60 / 60);
  const minutes = Math.floor((time - (hours * 60 * 60)) / 60);
  const seconds = Math.floor(time - (hours * 60 * 60 + minutes * 60));

  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
}
