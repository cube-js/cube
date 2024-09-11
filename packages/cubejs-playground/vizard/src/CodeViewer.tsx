import {
  DownloadOutlined,
  FileOutlined,
  FolderOpenOutlined,
} from '@ant-design/icons';
import {
  Block,
  Button,
  Flow,
  Grid,
  Space,
  tasty,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import { Fragment, useCallback, useEffect, useMemo, useState } from 'react';

import { Tabs } from './components/Tabs';
import { FileSystemTree } from './types';

import { useEvent } from './hooks/index';

import { VizardEditor } from './Editor';
import { downloadFile } from './utils/download-file';

const ItemElement = tasty(Button, {
  type: 'neutral',
  size: 'small',
  styles: {
    placeContent: 'start',
  },
});

export interface VizardCodeProps {
  files: FileSystemTree;
  appName: string;
}

interface FileTreeProps {
  files: FileSystemTree;
  prefix?: string;
  selectedFile?: string;
  onPress: (path: string, name: string, contents: string) => void;
}

function FileTree(props: FileTreeProps) {
  const { files, selectedFile, prefix = '', onPress } = props;

  const names = Object.keys(files).sort() as string[];

  return (
    <>
      {names.map((name) => {
        const path = prefix ? `${prefix}/${name}` : name;
        const file = files[name];
        const contents = 'file' in file ? file.file.contents : '';
        const indent = `${(path.split('/').length - 1) * 24 + 11}px`;

        if ('directory' in file) {
          return (
            <Fragment key={path}>
              <ItemElement
                isDisabled
                fill="#white"
                icon={<FolderOpenOutlined />}
                styles={{ paddingLeft: indent }}
              >
                {name}
              </ItemElement>
              <FileTree
                selectedFile={selectedFile}
                files={file.directory}
                prefix={path}
                onPress={onPress}
              />
            </Fragment>
          );
        }

        return (
          <ItemElement
            key={path}
            type={selectedFile === path ? 'secondary' : 'neutral'}
            icon={<FileOutlined />}
            styles={{ paddingLeft: indent }}
            onPress={() => onPress(path, name as string, contents as string)}
          >
            {name}
          </ItemElement>
        );
      })}
    </>
  );
}

interface FileTab {
  path: string;
  name: string;
  contents: string;
}

function getFirstFile(
  files: FileSystemTree,
  matches: string[],
  path: string = ''
): FileTab | undefined {
  const names = Object.keys(files) as string[];

  for (const name of names) {
    const node = files[name];

    if ('directory' in node) {
      const result = getFirstFile(
        node.directory,
        matches,
        path ? `${path}/${name}` : name
      );

      if (result) {
        return result;
      }
    } else if (matches.includes(name)) {
      return {
        path: name,
        name,
        contents: node.file.contents as string,
      };
    }
  }
}

function getFlatFiles(
  files: FileSystemTree,
  prefix: string = ''
): Record<string, string> {
  const names = Object.keys(files) as string[];
  const result: Record<string, string> = {};

  for (const name of names) {
    const path = prefix ? `${prefix}/${name}` : name;
    const node = files[name];

    if ('directory' in node) {
      Object.assign(result, getFlatFiles(node.directory, path));
    } else {
      result[path] = node.file.contents as string;
    }
  }

  return result;
}

// The list of files to open by default. Only the first found file will be opened.
const DEFAULT_OPEN_FILES = ['App.tsx'];

export function CodeViewer(props: VizardCodeProps) {
  const { files, appName } = props;
  const [openFiles, setOpenFiles] = useState<FileTab[]>([]);
  const [activeTab, setActiveTab] = useState<string | undefined>(undefined);

  const flatFiles = useMemo(() => getFlatFiles(files), [files]);

  const openFile = useEvent((path: string, name: string, contents: string) => {
    const existFile = openFiles.find((f) => f.path === path);

    if (!existFile) {
      const newFiles = [...openFiles];

      newFiles.push({
        path,
        name,
        contents,
      });

      setOpenFiles(newFiles);
    }

    setActiveTab(path);
  });

  // Get the content of the active tab
  const activeTabContent = openFiles.find(
    (f) => f.path === activeTab
  )?.contents;

  // Update open files when the files are changed
  useEffect(() => {
    setOpenFiles((openFiles) => {
      const newFiles = openFiles
        .filter((file) => {
          return file.path in flatFiles;
        })
        .map((file) => {
          return {
            ...file,
            contents: flatFiles[file.path],
          };
        });

      if (!newFiles.length) {
        const firstFile = getFirstFile(files, DEFAULT_OPEN_FILES) as FileTab;

        if (firstFile) {
          newFiles.push(firstFile);
        }
      }

      return newFiles;
    });
  }, [flatFiles]);

  // If there are no active tabs, set the first one
  useEffect(() => {
    if (!activeTab && openFiles.length) {
      setActiveTab(openFiles[0].path);
    }
  }, [openFiles]);

  const onDownloadConfig = useCallback(() => {
    downloadFile('.env.local', flatFiles['.env.local']);
  }, [files]);

  return (
    <Grid
      columns="300px minmax(1px, 1fr)"
      placeContent="stretch"
      placeItems="stretch"
    >
      <Flow border="right">
        <Flow
          padding="1x"
          styles={{
            overflow: 'auto',
            styledScrollbar: true,
            height: 'calc(100vh - 65px - 44px)',
          }}
        >
          <Space flow="column" gap="1bw">
            {useMemo(
              () => (
                <FileTree
                  selectedFile={activeTab}
                  files={files}
                  onPress={openFile}
                />
              ),
              [files, activeTab]
            )}
          </Space>
        </Flow>
        <Grid
          columns="minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)"
          gap="1x"
          padding="1x"
          border="top"
        >
          <TooltipProvider title="Download the source code as ZIP archive">
            <Button
              to={`./download/${appName}.zip`}
              type="primary"
              size="small"
              icon={<DownloadOutlined />}
            >
              Source
            </Button>
          </TooltipProvider>
          <TooltipProvider title="Download .env.local file with credentials">
            <Button
              onPress={onDownloadConfig}
              type="primary"
              size="small"
              icon={<DownloadOutlined />}
            >
              Config
            </Button>
          </TooltipProvider>
          <Button
            to="!https://cube.dev/docs/product/workspace/vizard"
            size="small"
            icon={<FileOutlined />}
          >
            Docs
          </Button>
        </Grid>
      </Flow>
      <Grid rows="min-content 1fr">
        {openFiles.length ? (
          <Tabs
            type="card"
            label="Files"
            activeKey={activeTab}
            onChange={(tab: string) => setActiveTab(tab as string)}
            onDelete={(targetKey: string) => {
              const newFiles = openFiles.filter(
                (file) => file.path !== targetKey
              );

              setOpenFiles(newFiles);

              if (activeTab === targetKey) {
                setActiveTab(newFiles.length ? newFiles[0].path : undefined);
              }
            }}
          >
            {openFiles.map((file) => (
              <Tabs.Tab key={file.path} title={file.name} id={file.path} />
            ))}
          </Tabs>
        ) : (
          <Block padding="2x" preset="t2m">
            &lt;- Select a file to see its content
          </Block>
        )}
        {activeTab ? <VizardEditor content={activeTabContent || ''} /> : null}
      </Grid>
    </Grid>
  );
}
