export function downloadFile(
  fileName: string,
  content: string,
  mimeType: string = 'text/plain'
): void {
  // Create a Blob with the content and the specified MIME type
  const blob = new Blob([content], { type: mimeType });

  // Create a URL for the blob object
  const url = window.URL.createObjectURL(blob);

  // Create an anchor (`<a>`) element
  const downloadLink = document.createElement('a');

  // Set the download attribute of the anchor to the filename
  downloadLink.download = fileName;

  // Set the href of the link to the blob URL
  downloadLink.href = url;

  // Append the anchor to the document
  document.body.appendChild(downloadLink);

  // Programmatically click the anchor to trigger the download
  downloadLink.click();

  // Remove the anchor from the document
  document.body.removeChild(downloadLink);

  // Release the created blob URL
  window.URL.revokeObjectURL(url);
}
