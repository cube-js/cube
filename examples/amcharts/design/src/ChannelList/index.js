import React from 'react';
import PropTypes from 'prop-types';
import styles from './ChannelList.module.css';

export default function ChannelList(props) {
  const { data } = props

  return (
    <div className={styles.root}>
      <h2>Most Active Channels</h2>
      <ul className={styles.list}>
        {data.map(channel => (
          <li key={channel.id} className={styles.item}>
            <div className={styles.avatar}>&nbsp;</div>
            <div title={channel.purpose}>
              <div className={styles.name}>{channel.name}</div>
              {channel.purpose && <div className={styles.title}>{channel.purpose}</div>}
            </div>
            <div>
              <div className={styles.reactions} title={'Top 3 reactions in #' + channel.name}>
                <span role='img' aria-label=''>{channel.reactions}</span>
              </div>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

ChannelList.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired
}