#!/usr/bin/env node

/**
 * Link Audit Script for docs_v4
 * Crawls all pages, checks internal and external links, generates a report
 */

import { writeFileSync } from 'fs'

const BASE_URL = 'http://localhost:3000'
const CONCURRENCY = 3  // Reduced to avoid overwhelming dev server
const EXTERNAL_CONCURRENCY = 3
const TIMEOUT = 30000  // 30s timeout for slow dev server page compiles

// Track state
const visitedPages = new Set()
const pagesToCrawl = []
const queuedPages = new Set() // Track what's already in queue to avoid duplicates
const allLinks = new Map() // url -> { sources: Set, status, redirectTo, isExternal }
const results = {
  pagesChecked: 0,
  totalLinks: 0,
  brokenLinks: [],
  redirects: [],
  errors: []
}

// Extract links from HTML
function extractLinks(html, pageUrl) {
  const links = []
  // Match href attributes in anchor tags
  const hrefRegex = /<a[^>]+href=["']([^"']+)["']/gi
  let match
  while ((match = hrefRegex.exec(html)) !== null) {
    const href = match[1]
    // Skip javascript:, mailto:, tel:, #anchors-only
    if (href.startsWith('javascript:') ||
        href.startsWith('mailto:') ||
        href.startsWith('tel:') ||
        href.startsWith('#')) {
      continue
    }

    try {
      // Resolve relative URLs
      const absoluteUrl = new URL(href, pageUrl).href
      links.push(absoluteUrl)
    } catch (e) {
      // Invalid URL, skip
    }
  }
  return links
}

// Check if URL is internal
function isInternalUrl(url) {
  try {
    const parsed = new URL(url)
    return parsed.origin === BASE_URL
  } catch {
    return false
  }
}

// Normalize URL (remove trailing slash, fragment)
function normalizeUrl(url) {
  try {
    const parsed = new URL(url)
    // Remove fragment
    parsed.hash = ''
    // Remove trailing slash for paths (but keep root /)
    if (parsed.pathname !== '/' && parsed.pathname.endsWith('/')) {
      parsed.pathname = parsed.pathname.slice(0, -1)
    }
    return parsed.href
  } catch {
    return url
  }
}

// Fetch with timeout
async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT)

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      redirect: 'manual' // Don't follow redirects automatically
    })
    clearTimeout(timeoutId)
    return response
  } catch (error) {
    clearTimeout(timeoutId)
    throw error
  }
}

// Crawl a single page
async function crawlPage(url) {
  const normalizedUrl = normalizeUrl(url)
  if (visitedPages.has(normalizedUrl)) return
  visitedPages.add(normalizedUrl)

  try {
    const response = await fetchWithTimeout(url)
    results.pagesChecked++

    // Handle redirects
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get('location')
      if (location) {
        const redirectTo = new URL(location, url).href
        const normalizedRedirect = normalizeUrl(redirectTo)
        if (isInternalUrl(redirectTo) && !visitedPages.has(normalizedRedirect) && !queuedPages.has(normalizedRedirect)) {
          // Follow internal redirects
          pagesToCrawl.push(redirectTo)
          queuedPages.add(normalizedRedirect)
        }
      }
      return
    }

    if (!response.ok) {
      console.log(`  [${response.status}] ${url}`)
      return
    }

    const contentType = response.headers.get('content-type') || ''
    if (!contentType.includes('text/html')) return

    const html = await response.text()
    const links = extractLinks(html, url)

    for (const link of links) {
      const normalizedLink = normalizeUrl(link)

      if (!allLinks.has(normalizedLink)) {
        allLinks.set(normalizedLink, {
          sources: new Set(),
          status: null,
          redirectTo: null,
          isExternal: !isInternalUrl(link)
        })
      }
      allLinks.get(normalizedLink).sources.add(url)

      // Queue internal links for crawling (check both visited and queued)
      if (isInternalUrl(link) && !visitedPages.has(normalizedLink) && !queuedPages.has(normalizedLink)) {
        pagesToCrawl.push(link)
        queuedPages.add(normalizedLink)
      }
    }
  } catch (error) {
    results.errors.push({ url, error: error.message })
  }
}

// Check a single link
async function checkLink(url, linkInfo) {
  try {
    // For external links, use HEAD first, fallback to GET
    const isExternal = linkInfo.isExternal

    let response
    try {
      response = await fetchWithTimeout(url, { method: 'HEAD' })
    } catch {
      // Some servers don't support HEAD, try GET
      response = await fetchWithTimeout(url, { method: 'GET' })
    }

    linkInfo.status = response.status

    // Handle redirects
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get('location')
      if (location) {
        linkInfo.redirectTo = new URL(location, url).href
      }
    }

    return response.status
  } catch (error) {
    linkInfo.status = 'ERROR'
    linkInfo.error = error.message
    return 'ERROR'
  }
}

// Process links in batches with concurrency control
async function processWithConcurrency(items, fn, concurrency) {
  const results = []
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency)
    const batchResults = await Promise.all(batch.map(fn))
    results.push(...batchResults)

    // Progress indicator
    process.stdout.write(`\r  Checked ${Math.min(i + concurrency, items.length)}/${items.length} links`)
  }
  console.log('')
  return results
}

// Generate report
function generateReport() {
  console.log('\n' + '='.repeat(60))
  console.log('LINK AUDIT REPORT')
  console.log('='.repeat(60))

  console.log(`\nSummary:`)
  console.log(`  Pages crawled: ${results.pagesChecked}`)
  console.log(`  Total unique links: ${allLinks.size}`)

  // Categorize links
  const brokenInternal = []
  const brokenExternal = []
  const redirectsInternal = []
  const redirectsExternal = []

  for (const [url, info] of allLinks) {
    const sources = Array.from(info.sources).slice(0, 3)
    const sourceStr = sources.map(s => s.replace(BASE_URL, '')).join(', ')

    if (info.status === 404 || info.status === 'ERROR' || (typeof info.status === 'number' && info.status >= 400)) {
      const entry = {
        url: info.isExternal ? url : url.replace(BASE_URL, ''),
        status: info.status,
        error: info.error,
        sources: sourceStr,
        sourceCount: info.sources.size
      }
      if (info.isExternal) {
        brokenExternal.push(entry)
      } else {
        brokenInternal.push(entry)
      }
    }

    if (info.status >= 300 && info.status < 400 && info.redirectTo) {
      const entry = {
        url: info.isExternal ? url : url.replace(BASE_URL, ''),
        status: info.status,
        redirectTo: info.redirectTo.replace(BASE_URL, ''),
        sources: sourceStr,
        sourceCount: info.sources.size
      }
      if (info.isExternal) {
        redirectsExternal.push(entry)
      } else {
        redirectsInternal.push(entry)
      }
    }
  }

  // Sort by source count (most referenced first)
  brokenInternal.sort((a, b) => b.sourceCount - a.sourceCount)
  brokenExternal.sort((a, b) => b.sourceCount - a.sourceCount)

  console.log(`\n  Broken internal links: ${brokenInternal.length}`)
  console.log(`  Broken external links: ${brokenExternal.length}`)
  console.log(`  Internal redirects: ${redirectsInternal.length}`)
  console.log(`  External redirects: ${redirectsExternal.length}`)

  if (brokenInternal.length > 0) {
    console.log('\n' + '-'.repeat(60))
    console.log('BROKEN INTERNAL LINKS')
    console.log('-'.repeat(60))
    for (const link of brokenInternal) {
      console.log(`\n[${link.status}] ${link.url}`)
      console.log(`  Found on: ${link.sources}${link.sourceCount > 3 ? ` (+${link.sourceCount - 3} more)` : ''}`)
      if (link.error) console.log(`  Error: ${link.error}`)
    }
  }

  if (brokenExternal.length > 0) {
    console.log('\n' + '-'.repeat(60))
    console.log('BROKEN EXTERNAL LINKS')
    console.log('-'.repeat(60))
    for (const link of brokenExternal) {
      console.log(`\n[${link.status}] ${link.url}`)
      console.log(`  Found on: ${link.sources}${link.sourceCount > 3 ? ` (+${link.sourceCount - 3} more)` : ''}`)
      if (link.error) console.log(`  Error: ${link.error}`)
    }
  }

  if (redirectsInternal.length > 0) {
    console.log('\n' + '-'.repeat(60))
    console.log('INTERNAL REDIRECTS (links that should be updated)')
    console.log('-'.repeat(60))
    for (const link of redirectsInternal) {
      console.log(`\n[${link.status}] ${link.url} → ${link.redirectTo}`)
      console.log(`  Found on: ${link.sources}${link.sourceCount > 3 ? ` (+${link.sourceCount - 3} more)` : ''}`)
    }
  }

  if (results.errors.length > 0) {
    console.log('\n' + '-'.repeat(60))
    console.log('CRAWL ERRORS')
    console.log('-'.repeat(60))
    for (const err of results.errors) {
      console.log(`  ${err.url}: ${err.error}`)
    }
  }

  // Output JSON for further processing
  const jsonOutput = {
    summary: {
      pagesChecked: results.pagesChecked,
      totalLinks: allLinks.size,
      brokenInternal: brokenInternal.length,
      brokenExternal: brokenExternal.length,
      redirectsInternal: redirectsInternal.length,
      redirectsExternal: redirectsExternal.length
    },
    brokenInternal,
    brokenExternal,
    redirectsInternal,
    redirectsExternal
  }

  writeFileSync('link-audit-results.json', JSON.stringify(jsonOutput, null, 2))
  console.log('\n\nDetailed results saved to: link-audit-results.json')
}

// Main function
async function main() {
  console.log('Link Audit Script')
  console.log('=================')
  console.log(`Base URL: ${BASE_URL}`)
  console.log('')

  // Start crawling from root
  console.log('Phase 1: Crawling internal pages...')
  const startUrl = BASE_URL + '/product/introduction'
  pagesToCrawl.push(startUrl)
  queuedPages.add(normalizeUrl(startUrl))

  while (pagesToCrawl.length > 0) {
    const batch = pagesToCrawl.splice(0, CONCURRENCY)
    await Promise.all(batch.map(url => crawlPage(url)))
    process.stdout.write(`\r  Crawled ${visitedPages.size} pages, ${pagesToCrawl.length} in queue, ${allLinks.size} links found`)
    // Small delay between batches to avoid overwhelming the server
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  console.log('')

  // Check all links
  console.log('\nPhase 2: Checking internal links...')
  const internalLinks = Array.from(allLinks.entries()).filter(([_, info]) => !info.isExternal)
  await processWithConcurrency(
    internalLinks,
    ([url, info]) => checkLink(url, info),
    CONCURRENCY
  )

  console.log('\nPhase 3: Checking external links...')
  const externalLinks = Array.from(allLinks.entries()).filter(([_, info]) => info.isExternal)
  await processWithConcurrency(
    externalLinks,
    ([url, info]) => checkLink(url, info),
    EXTERNAL_CONCURRENCY
  )

  // Generate report
  generateReport()
}

main().catch(console.error)
