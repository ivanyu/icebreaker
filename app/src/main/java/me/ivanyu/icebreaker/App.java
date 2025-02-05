package me.ivanyu.icebreaker;

import com.formdev.flatlaf.FlatLightLaf;
import me.ivanyu.icebreaker.main_window.MainWindow;

public class App {
    public static void main(final String[] args) {
        final String url = args[0];
        final CatalogService catalogService = new CatalogService(url);

        FlatLightLaf.setup();

        final MainWindow mainWindow = new MainWindow(catalogService);
        mainWindow.run();
    }
}
