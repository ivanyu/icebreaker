import { useCallback, useEffect, useMemo, Key, useState } from 'react';
import { useImmer } from 'use-immer';
import { Select, Splitter, Tabs, Table, Tree } from 'antd';
import type { TableColumnsType, TabsProps } from 'antd';
import { fetch } from '@tauri-apps/plugin-http';
import { listNamespaces, listTables, loadTable } from './rest_catalog_client/sdk.gen';
import { SnapshotReferences, StructField, TableMetadata } from './rest_catalog_client';
import './App.css';

const BaseURL = 'http://127.0.0.1:8181';

type TableCoordinates = {
  namespace: string;
  name: string;
}

type BaseTreeNode = {
  key: string;
  title: string;
};
type NamespaceTreeNode = BaseTreeNode & {
  type: 'namespace';
  keyElements: string[];
  isLeaf: false;
  children?: TreeNode[];
  selectable: false;
};
type TableTreeNode = BaseTreeNode & TableCoordinates & {
  type: 'table';
  isLeaf: true;
  selectable: true;
};
type TreeNode = NamespaceTreeNode | TableTreeNode;

function App() {
  const [treeNodes, setTreeNodes] = useImmer<TreeNode[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableCoordinates | undefined>(undefined);
  const [selectedTableMetadata, setSelectedTableMetadata] = useState<TableMetadata | undefined>(undefined);
  const [selectedBranch, setSelectedBranch] = useState<string | undefined>(undefined);

  useEffect(() => {
    // TODO handle errors
    listNamespacesNodes(undefined).then(setTreeNodes);
  }, []);

  const setChildren = useCallback((keyElements: string[], children: TreeNode[]) => {
    setTreeNodes((draft) => {
      var searchFront: TreeNode[] | undefined = draft;
      var currentNode: TreeNode | undefined = undefined;
      var levelKey: string | undefined = undefined;
      for (const keyEl of keyElements) {
        if (!searchFront) {
          console.error(`Node path ${JSON.stringify(keyElements)} not found`);
          return;
        }

        levelKey = levelKey ? levelKey + '\x1F' + keyEl : keyEl;

        currentNode = searchFront.find(n => n.key === levelKey);
        if (!currentNode) {
          console.error(`Node path ${JSON.stringify(keyElements)} not found`);
          return;
        }

        searchFront = currentNode.children;
      }
      currentNode!.children = children;
    });
  }, []);

  const onLoadData = useCallback(async ({ key, keyElements }: { key: string, keyElements: string[] }) => {
    const childrenNamespacesPromise: Promise<TreeNode[] | undefined> = listNamespacesNodes(key);
    const childrenTablesPromise: Promise<TreeNode[] | undefined> = listTablesNodes(key);

    var children: TreeNode[] = [];
    try {
      const childrenNamespaces = await childrenNamespacesPromise;
      if (childrenNamespaces) {
        children = children.concat(childrenNamespaces);
      }
    } catch (e) {
      // TODO show to user
      console.error(e);
    }

    try {
      const childrenTable = await childrenTablesPromise;
      if (childrenTable) {
        children = children.concat(childrenTable);
      }
    } catch (e) {
      // TODO show to user
      console.error(e);
    }

    setChildren(keyElements, children);
  }, []);

  const onSelect = useCallback((_selectedKeys: Key[], { selected, node }: { selected: boolean, node: TableTreeNode }) => {
    if (selected) {
      setSelectedTable({ namespace: node.namespace, name: node.name });
    }
  }, []);

  useEffect(() => {
    setSelectedTableMetadata(undefined);
    setSelectedBranch(undefined);
    if (selectedTable) {
      // TODO display error
      loadTableMetadata(selectedTable).then(setSelectedTableMetadata);
    }
  }, [selectedTable]);

  const leftPanel = (
    <Tree
      treeData={treeNodes}
      loadData={onLoadData}
      onSelect={onSelect}
      showLine
    />
  );

  const tableColumns: TableColumnsType<StructField> = useMemo(() => [
    { title: 'Id', 'dataIndex': 'id', key: 'id', align: 'center' },
    { title: 'Name', 'dataIndex': 'name', key: 'name', align: 'center', render: (n: string) => <pre>{n}</pre> },
    { title: 'Required', 'dataIndex': 'required', key: 'required', align: 'center', render: (r: boolean) => <pre>{`${r}`}</pre> },
    { title: 'Type', 'dataIndex': 'type', key: 'type', align: 'center', render: (t: string) => <pre>{t}</pre> },
  ], []);
  const tableData = currentSchemaFields(selectedTableMetadata);

  const schemaTab = <Table columns={tableColumns} dataSource={tableData} rowKey='id' bordered={true} size='small' pagination={false}></Table>;

  const snapshotsTab = (selectedTableMetadata && selectedTableMetadata.refs && Object.keys(selectedTableMetadata!.refs).length > 0)
    ? (<div><span>Branch: </span>
      <Select defaultValue={'main'} style={{width: '200px'}} onSelect={setSelectedBranch}>
        { Object.entries(selectedTableMetadata.refs)
        .filter(([_k, v]) => v.type === 'branch')
        .map(([k, _v]) => <Select.Option key={k} value={k}>{k}</Select.Option>) }
      </Select>
    </div>)
    : undefined;
  const tabs: TabsProps['items'] = [
    { 'key': 'schema', label: 'Schema', children: schemaTab },
    { 'key': 'snapshots', label: 'Snapshots', children: snapshotsTab },
  ];
  const rightPanel = <Tabs tabBarStyle={{ paddingLeft: '5px' }} defaultActiveKey='schema' items={tabs} ></Tabs>;
  return (
    <Splitter>
      <Splitter.Panel defaultSize="20%">
        {leftPanel}
      </Splitter.Panel>
      <Splitter.Panel>
        {rightPanel}
      </Splitter.Panel>
    </Splitter>
  );

}

function currentSchemaFields(metadata: TableMetadata | undefined): StructField[] | undefined {
  if (!metadata || !metadata.schemas || typeof metadata['current-schema-id'] === 'undefined') {
    return undefined;
  }

  const currentSchemaId = metadata['current-schema-id'];
  for (const schema of metadata.schemas) {
    if (schema['schema-id'] === currentSchemaId) {
      if (schema.type === 'struct') {
        return schema.fields;
      } else {
        console.log(`Current schema is not struct, but ${schema.type}`);
        return undefined;
      }
    }
  }
}

function xf(refs: SnapshotReferences) {
  for (const [k, v] of Object.entries(refs)) {
    console.log(k, v);
  }
}

async function listNamespacesNodes(parentKey: string | undefined): Promise<NamespaceTreeNode[]> {
  try {
    const resp = await listNamespaces({
      fetch: fetch,
      baseUrl: BaseURL,
      path: { prefix: '.' },
      query: { parent: parentKey }  // TODO support pagination
    });
    if (resp.error) {
      throw new Error(`Error listing namespaces: ${resp.response.status} ${resp.response.statusText} ${JSON.stringify(resp.error)}`);
    }

    if (resp.data!.namespaces) {
      return resp.data.namespaces.map(ns => {
        const nsName = ns[ns.length - 1];
        // const key = parentKey ? `${parentKey}.${nsName}` : nsName;
        const key = ns.join('\x1F');
        const keyElements = ns;
        return { key, keyElements, type: 'namespace', title: nsName, isLeaf: false, selectable: false };
      });
    } else {
      return [];
    }
  } catch (e) {
    throw new Error(`Error listing namespaces: ${e}`);
  }
}

async function listTablesNodes(namespace: string): Promise<TableTreeNode[]> {
  try {
    const resp = await listTables({
      fetch: fetch,
      baseUrl: BaseURL,
      path: {
        prefix: '.',
        namespace
      }, // TODO support pagination
    });
    if (resp.error) {
      throw new Error(`Error listing tables: ${resp.response.status} ${resp.response.statusText} ${JSON.stringify(resp.error)}`);
    }

    if (resp.data!) {
      return resp.data.identifiers!.map(id => {
        const key = id.namespace.join('\x1F') + '\x1F' + id.name;
        const keyElements = id.namespace.slice();
        keyElements.push(id.name);
        return { key, type: 'table', namespace, name: id.name, title: id.name, isLeaf: true, selectable: true };
      });
    } else {
      return [];
    }
  } catch (e) {
    throw new Error(`Error listing tables: ${e}`);
  }
}

async function loadTableMetadata(table: TableCoordinates): Promise<TableMetadata> {
  try {
    const resp = await loadTable({
      fetch: fetch,
      baseUrl: BaseURL,
      path: {
        prefix: '.',
        namespace: table.namespace,
        table: table.name,
      }, // TODO support pagination
    });
    if (resp.error) {
      throw new Error(`Error loading table: ${resp.response.status} ${resp.response.statusText} ${JSON.stringify(resp.error)}`);
    }
    return resp.data.metadata;
  } catch (e) {
    throw new Error(`Error loading table: ${e}`);
  }
}

export default App;
