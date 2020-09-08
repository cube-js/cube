import React from 'react';
import PropTypes from 'prop-types';
import styles from './styles.module.css';
import Header from '../Header';
import Banner from '../Banner';
import UploadBlock from '../UploadBlock';

function UploadView(props) {
  const { onUpload } = props;

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header />
        <UploadBlock onUpload={onUpload} />
      </div>
      <div className={styles.sidebar}>
        <Banner />
      </div>
    </div>
  );
}

export default UploadView;

UploadView.propTypes = {
  onUpload: PropTypes.func.isRequired,
};