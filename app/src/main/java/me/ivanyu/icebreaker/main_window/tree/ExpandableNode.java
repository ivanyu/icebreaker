package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.Namespace;

import javax.swing.tree.DefaultMutableTreeNode;

abstract class ExpandableNode extends DefaultMutableTreeNode {
    private LoadingState loadingState = LoadingState.NOT_LOADED;
    LoadingPlaceholder loadingPlaceholder = null;
    final Namespace namespace;

    ExpandableNode(final Namespace namespace) {
        super(namespace.equals(Namespace.empty()) ? "Namespaces" : namespace.level(namespace.length() - 1));
        this.namespace = namespace;
    }

    LoadingState loadingState() {
        return loadingState;
    }

    void setLoadingStateLoading() {
        loadingState = LoadingState.LOADING;
    }

    void setLoadingStateLoaded() {
        loadingState = LoadingState.LOADED;
    }
}
