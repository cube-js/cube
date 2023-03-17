import { getEmoji } from '../emoji'
import * as classes from './ArticleList.module.css'

function ArticleList({ articles }) {
  return <ul className={classes.root}>
    {articles.map((article, i) => (
      <li key={i} className={classes.item}>
        {getEmoji(article.region)}
        <a className={classes.link} href={article.url} target='_blank'>
          {article.title.replaceAll('_', ' ')}
        </a>
        <sup className={classes.index}>{i + 1}</sup>
      </li>
    ))}
  </ul>
}

export default ArticleList