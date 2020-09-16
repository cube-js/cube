import React from 'react';
import PropTypes from 'prop-types';
import styles from './styles.module.css';

export default function MemberList(props) {
  const { data, limit, member: chosenMember, onShow, onSelect } = props;

  const members = limit
    ? data.slice(0, limit)
    : data.slice().sort((a, b) => a.name.localeCompare(b.name));

  if (!members.length) return null;

  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <h2>{limit ? 'Most Active' : 'All'} Members</h2>
        <div className={styles.controls}>
          <button onClick={onShow}>Show {limit ? 'All' : 'Active'}</button>
        </div>
      </div>
      <ul className={styles.list}>
        {members.map(member => (
          <li
            key={member.id}
            className={styles.item + ' ' + (chosenMember === member.name ? styles.selected : '')}
            onClick={() => onSelect(chosenMember !== member.name ? member.name : null)}
          >
            <div className={styles.avatar}>
              <img src={member.image} alt='' />
            </div>
            <div title={member.title}>
              <div className={styles.name}>
                <span
                  className={member.is_admin ? styles.admin : ''}
                  title={member.is_admin ? 'Workspace Admin' : ''}
                >
                  {member.name}
                </span>
              </div>
              {member.title && <div className={styles.title}>{member.title}</div>}
            </div>
            <div>
              <div className={styles.reactions} title={'Top 3 reactions by ' + member.name}>
                <span role='img' aria-label=''>{member.reactions}</span>
              </div>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

MemberList.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object).isRequired,
  limit: PropTypes.number,
  member: PropTypes.string,
  onShow: PropTypes.func.isRequired,
  onSelect: PropTypes.func.isRequired,
}