package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.TableIdentifier;

class TableNode extends LeafNode {
    TableNode(final TableIdentifier tableId) {
        super(tableId);
    }
}
