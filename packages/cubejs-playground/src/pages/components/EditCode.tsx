import { useState, CSSProperties } from "react";

import { Controlled as CodeMirror } from 'react-codemirror2';
import yaml from 'js-yaml';
import { Button, Input, message, Modal } from 'antd'

import { playgroundFetch } from '../../shared/helpers';

import "codemirror/lib/codemirror.css";
import "codemirror/mode/yaml/yaml";

import './EditCode.less'
import { ExclamationCircleOutlined } from "@ant-design/icons";

type File = {
  absPath?: string,
  content: string,
  fileName: string,
  type: 'save' | 'create'
}

type EditCodeProps = {
  file: File,
  language?: string,
  style?: CSSProperties;
  onChange?: (reset?: boolean) => void
}

export const deleteFile = async (fileName) => {
  const res = await playgroundFetch('/playground/file/delete', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      fileName
    }),
  });
  const result = await res.json();
  return result
}

export const createFile = async ({ fileName, source }) => {
  const res = await playgroundFetch('/playground/file', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      fileName,
      source
    }),
  });
  const result = await res.json();
  return result
}

export const EditCode = (props: EditCodeProps) => {
  const { file, language, style, onChange } = props
  const { fileName, content } = file

  const [ymlContent, setYMLContent] = useState(content);
  const [errorContent, setErrorContent] = useState('')

  const handleSave = async () => {
    const result = await createFile({ fileName, source: ymlContent })

    if (result) {
      message.success('modified successfully')
    }

    onChange && onChange()
  }

  const handleReset = () => {
    setYMLContent(content)
    setErrorContent('')
  }

  const handleRename = () => {
    let newFileName = ''

    Modal.confirm({
      icon: null,
      content: <Input defaultValue={fileName} onInput={(e) => {
        const value = (e.target as HTMLInputElement).value || ''
        newFileName = value
      }} />,
      okText: 'confirm',
      cancelText: 'cancel',
      onOk: async () => {
        if (!newFileName || newFileName === fileName) return

        try {
          await deleteFile(fileName)

          const result = await createFile({
            fileName: newFileName,
            source: ymlContent
          })

          if (result) {
            message.success('renamed successfully')
          }

          onChange && onChange(true)
        } catch (error) {
          console.log(error)
        }
      }
    })
  }

  const handleRemove = async () => {
    Modal.confirm({
      title: 'Confirm',
      icon: <ExclamationCircleOutlined />,
      content: 'Are you sure to delete?',
      okText: 'confirm',
      cancelText: 'cancel',
      onOk: async () => {
        const result = await deleteFile(fileName)

        if (result) {
          message.success('deleted successfully')
        }

        onChange && onChange(true)
      }
    });
  }

  const handleYMLChange = (editor, data, value) => {
    setYMLContent(value);

    try {
      yaml.load(value);

      setErrorContent('')
    } catch (error: any) {
      console.error(error);

      setErrorContent(error.message)
    }
  };

  return (
    <div style={style}>
      <div className="error-content">{errorContent && errorContent}</div>

      <CodeMirror
        options={{
          mode: language || 'yaml',
          theme: 'default',
          lineNumbers: true,
        }}
        value={ymlContent}
        onBeforeChange={handleYMLChange}
        editorDidMount={(editor) => {
          editor.setSize(null, "600px"); // 设置编辑器高度
        }}
      />

      <div className="btns">
        <Button className="save" type="primary" onClick={handleSave}>Save</Button>
        <Button className="reset" onClick={handleReset}>Reset</Button>
        <Button className="delete" danger onClick={handleRemove}>delete</Button>
        <Button className="rename" onClick={handleRename}>Rename</Button>
      </div>
    </div>
  );
}
