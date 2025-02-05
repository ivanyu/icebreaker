package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.Namespace;

class RootNode extends ExpandableNode {
    RootNode() {
        super(Namespace.empty());
    }
}
