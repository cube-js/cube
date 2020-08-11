import React from 'react';
import PropTypes from 'prop-types';
import styles from './MemberList.module.css';

export default function MemberList(props) {
  const { data } = props;

  return (
    <div className={styles.root}>
      <h2>Most Active Members</h2>
      <ul className={styles.list}>
        {data.map(member => (
          <li key={member.id} className={styles.item}>
            <div className={styles.avatar}>
              <img src={member.image} alt='' />
            </div>
            <div title={member.title}>
              <div className={styles.name + (member.is_admin ? ' ' + styles.admin : '')}
                   title={member.is_admin ? 'Workspace Admin' : ''}>
                {member.name}
              </div>
              {member.title && <div className={styles.title}>{member.title}</div>}
            </div>
            <div>
              <div className={styles.reactions} title={'Top 3 reactions by ' + member.real_name}>
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
  data: PropTypes.arrayOf(PropTypes.object).isRequired
}