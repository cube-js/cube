import React, { useState } from 'react';
import PropTypes from 'prop-types';
import Dropzone from 'react-dropzone';
import styles from './styles.module.css';
import { uploadSlackArchive } from '../api';

export default function UploadBlock(props) {
  const { onUpload } = props;

  const [ isInProgress, setIsInProgress ] = useState(false);

  return (
    <div className={styles.root}>
      <div className={styles.description}>
        <p>
          <a
            href='https://slack.com/intl/en-ru/help/articles/201658943-Export-your-workspace-data#use-standard-export'
            target='_blank'
            rel='noopener noreferrer'
          >
            Export public data
          </a>
          &nbsp;from&nbsp;your Slack workspace.
        </p>
        <p>Get a ZIP file and upload (or drop) it here.</p>
      </div>
      <Dropzone
        onDrop={files => {
          setIsInProgress(true);
          uploadSlackArchive(files[0]).then(onUpload);
        }}
        accept='application/zip'
        multiple={false}
        disabled={isInProgress}
      >
        {({ getRootProps, getInputProps }) => (
          <div
            className={styles.drop + ' ' + (isInProgress ? styles.progress : '')}
            {...getRootProps()}
          >
            {isInProgress ? (
              <div className={styles.message}>Uploading...</div>
            ) : (
              <button className={styles.button}>Upload ZIP archive</button>
            )}
            <input {...getInputProps()} />
          </div>
        )}
      </Dropzone>
    </div>
  )
}

UploadBlock.propTypes = {
  onUpload: PropTypes.func.isRequired,
}