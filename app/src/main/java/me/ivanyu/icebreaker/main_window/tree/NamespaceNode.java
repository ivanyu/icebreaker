package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.Namespace;

class NamespaceNode extends ExpandableNode {
    NamespaceNode(final Namespace namespace) {
        super(namespace);
    }
}
