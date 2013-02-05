package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.msg.ListItem;

public class FakeFullListItem extends ListItem
{
    // for automatic data-binding:
    protected FakeFullListItem() { }

    public FakeFullListItem(ListItem i) {
        super(i);
    }
}
