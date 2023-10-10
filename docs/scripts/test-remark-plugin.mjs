import fs from 'fs'
import remark from 'remark'
import reporter from 'vfile-reporter'
import linkEnvVars from '../src/remark/plugins/link-environment-variables.js'
import remarkGfm from 'remark-gfm';
import remarkParse from 'remark-parse'


const buffer = fs.readFileSync('content/Configuration/Databases/AWS-Athena.mdx')

remark()
  .use(remarkParse)
  .use(remarkGfm)
  .use(linkEnvVars)
  .process(buffer)
  .then((file) => {
    console.error(reporter(file))
  })
