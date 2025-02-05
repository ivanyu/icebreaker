package me.ivanyu.icebreaker.main_window.details;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class TagsCell {
    private final Collection<String> tags;

    TagsCell(final Collection<String> tags) {
        this.tags = tags == null ? List.of() : new ArrayList<>(tags);
    }

    @Override
    public String toString() {
        return String.format("Tags: %s", String.join(", ", tags));
    }
}
