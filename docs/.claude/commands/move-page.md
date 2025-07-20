# Move documentation page

I need to move a documentation page from its current location to a new one within our documentation site. Please help me with the full process including:

1. Moving the source file to the destination directory:
  - If there's a file with the same name in the destination directory, ask for the new name and rename the source file before moving it
  - Use the `mv` command to move the file to the new location

2. Updating relevant _meta.js files to maintain proper navigation:
  - Add the page to the destination directory's _meta.js file
  - Remove the page from the source directory's _meta.js file
  - If the source _meta.js file becomes empty (just contains `module.exports = {}`)
      - Delete it
      - Delete the directory where that _meta.js file was from the _meta.js file in its parent directory

3. Finding and updating all internal references/links to the moved page:
  - Search for references to the old URL path in all files
  - Pay special attention to link references at the bottom of files
  - Check plugins that might construct URLs programmatically

4. Adding a redirect from the old URL to the new one:
  - Add a new entry at the top of the redirects.json file
  - Format should follow existing entries with "permanent": true

Before starting, ask for:
- Source page path (in URL format, e.g., /reference/configuration/environment-variables)
- Destination page path (in URL format, e.g., /product/configuration/reference/environment-variables)
