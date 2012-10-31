package com.fasterxml.clustermate.service.cfg;

import java.util.*;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;


public class ClusterConfig
{
    @NotNull
    public ClusterDefType type;

    @Min(1)
    @Max(Integer.MAX_VALUE)
    public int clusterKeyspaceSize;

    // // Static config:

    // not mandatory, but if defined, must be valid
    @Valid
    // also: if defined, must have at least 1 entry
    @Size(min=1, max=Integer.MAX_VALUE)
    public List<NodeConfig> clusterNodes = new ArrayList<NodeConfig>();
}
