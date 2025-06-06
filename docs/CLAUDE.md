# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains the documentation site for Cube, a semantic layer for building data applications. The documentation is built using Next.js with the Nextra documentation theme and MDX for content authoring.

## Project Structure

- `/pages`: Contains all the documentation content in MDX format
- `/components`: React components used throughout the documentation site
  - `/common`: General UI components like buttons, logos, etc.
  - `/layouts`: Page layout components
  - `/mdx`: Custom components for use within MDX content
  - `/overrides`: Components that override Nextra defaults
- `/public`: Static assets (images, icons)
- `/styles`: Global CSS styles
- `/scripts`: Utility scripts for managing content

## Development Commands

```bash
# Start development server
npm run dev

# Build for production
npm run build

# Start production server
npm run start

# Utility commands for content management
npm run create-redirects     # Create redirect entries
npm run migrate-content      # Run content migration scripts
npm run update-links         # Update links in content files
```

## Technology Stack

- **Framework**: Next.js with Nextra theme for documentation
- **Content**: MDX (Markdown with JSX)
- **Styling**: CSS/SCSS modules with Tailwind CSS
- **Deployment**: Vercel

## Content Organization

The documentation follows a hierarchical structure:

1. Main sections are defined in `/pages/_meta.js`
2. Each section has its own `_meta.js` file that defines the order and titles of pages
3. Content is written in MDX files with frontmatter

## Component Usage in MDX

The documentation uses custom MDX components for specialized content presentation:

- `<AlertBox>` - For highlighting important information
- `<CodeTabs>` - For code examples in multiple languages
- `<Grid>` and `<GridItem>` - For creating responsive grids
- `<YouTubeVideo>` - For embedding videos
- `<Screenshot>` - For displaying screenshots

## Best Practices

1. **MDX Content**:
   - Use the appropriate custom components for different content types
   - Follow the existing section structure when adding new pages
   - Include proper frontmatter with title and description

2. **Component Development**:
   - Use CSS/SCSS modules for component styling
   - Follow the existing folder structure for new components
   - Export components through index files for cleaner imports

3. **Navigation**:
   - Update `_meta.js` files when adding new pages to ensure proper navigation

## Deployment

The site is deployed to Vercel. The deployment process is automated via GitHub integration.
