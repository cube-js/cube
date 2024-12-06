import { useEffect, useRef } from 'react';
import MonacoEditor from 'react-monaco-editor';
import monaco from 'monaco-editor';

import { MONOSPACE_FONT_FAMILY } from './monaco/config';
import { setupMonaco } from './monaco';
import { useWindowSize } from './hooks/index';

import type { IRange, ISelection } from 'monaco-editor';

type EditorProps = {
  content: string;
};

export function VizardEditor({ content }: EditorProps) {
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor>();
  const windowSize = useWindowSize();

  const editor = editorRef.current;

  const options = {
    readOnly: true,
    automaticLayout: true,
    roundedSelection: false,
    scrollBeyondLastLine: true,
    fontSize: 14,
    fontFamily: MONOSPACE_FONT_FAMILY,
    minimap: {
      enabled: false,
    },
    tabSize: 2,
  };

  useEffect(() => {
    if (editor) {
      editor.setSelection({
        endColumn: 0,
        endLineNumber: 0,
        positionColumn: 0,
        positionLineNumber: 0,
        selectionStartColumn: 0,
        selectionStartLineNumber: 0,
        startColumn: 0,
        startLineNumber: 0,
      } as unknown as IRange & ISelection);
    }
  }, [editor]);

  useEffect(() => {
    editor?.layout();
  }, [editor, windowSize]);

  useEffect(() => {
    if (!editor) {
      return;
    }

    if (editorRef.current && content !== editor.getValue()) {
      editor.setValue(content);
    }
  }, [editor, content]);

  function editorWillMount() {
    setupMonaco();
  }

  return (
    <>
      <MonacoEditor
        theme="cube"
        options={options}
        editorWillMount={editorWillMount}
        editorDidMount={(editor) => {
          editorRef.current = editor;
        }}
      />
    </>
  );
}
