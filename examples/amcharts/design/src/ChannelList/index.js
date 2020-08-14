import React from 'react';
import PropTypes from 'prop-types';
import styles from './styles.module.css';

export default function ChannelList(props) {
  const { data, limit, channel: chosenChannel, onShow, onSelect } = props;

  const channels = limit
    ? data.slice(0, limit)
    : data.slice().sort((a, b) => a.name.localeCompare(b.name));

  if (!channels.length) return null;

  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <h2>{limit ? 'Most Active' : 'All'} Channels</h2>
        <div className={styles.controls}>
          <button onClick={onShow}>Show {limit ? 'All' : 'Active'}</button>
        </div>
      </div>
      <ul className={styles.list}>
        {channels.map(channel => (
          <li
            key={channel.id}
            className={styles.item + ' ' + (chosenChannel === channel.name ? styles.selected : '')}
            onClick={() => onSelect(chosenChannel !== channel.name ? channel.name : null)}
          >
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
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  limit: PropTypes.number,
  channel: PropTypes.string,
  onShow: PropTypes.func.isRequired,
  onSelect: PropTypes.func.isRequired,
}