package me.ivanyu.icebreaker.main_window.tree;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.awt.*;
import java.io.IOException;

class TreeCellRenderer extends DefaultTreeCellRenderer {
    private static final ImageIcon NAMESPACE_ICON;
    private static final ImageIcon TABLE_ICON;
    private static final ImageIcon VIEW_ICON;

    static {
        try {
            NAMESPACE_ICON = new ImageIcon(
                ImageIO.read(TreeCellRenderer.class.getResource("/diagram-3.png"))
                    .getScaledInstance(32, 32, Image.SCALE_SMOOTH)
            );
            TABLE_ICON = new ImageIcon(
                ImageIO.read(TreeCellRenderer.class.getResource("/table.png"))
                    .getScaledInstance(32, 32, Image.SCALE_SMOOTH)
            );
            VIEW_ICON = new ImageIcon(
                ImageIO.read(TreeCellRenderer.class.getResource("/eye.png"))
                    .getScaledInstance(32, 32, Image.SCALE_SMOOTH)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Component getTreeCellRendererComponent(final JTree tree,
                                                  final Object value,
                                                  final boolean sel,
                                                  final boolean expanded,
                                                  final boolean leaf,
                                                  final int row,
                                                  final boolean hasFocus) {
        super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);


        if (value instanceof RootNode) {
            setIcon(getOpenIcon());
        } else if (value instanceof NamespaceNode) {
            setIcon(getOpenIcon());
        } else if (value instanceof TableNode) {
            setIcon(TABLE_ICON);
        } else if (value instanceof ViewNode) {
            setIcon(VIEW_ICON);
        } else {
            setIcon(getLeafIcon());
        }
        return this;
    }
}
