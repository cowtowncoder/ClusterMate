package com.fasterxml.clustermate.service.cleanup;

import java.io.*;

import com.fasterxml.clustermate.service.util.SizeUtil;

public class DiskUsageStats
{
    protected final int _maxDirs, _maxFiles;
    
    public int dbDirs = 0;
    
    public int dbFiles = 0;

    public long dbTotalBytes = 0L;

    public DiskUsageStats(int maxDirs, int maxFiles)
    {
        _maxDirs = maxDirs;
        _maxFiles = maxFiles;
    }
    
    public boolean addDir() {
        return ++dbDirs <= _maxDirs;
    }
    
    public boolean addFile(File f) {
        dbTotalBytes += f.length();
        return ++dbFiles <= _maxFiles;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(60)
            .append("Metadata DB stored in ").append(dbFiles)
            .append(" files, containing ").append(SizeUtil.sizeDesc(dbTotalBytes))
            .append(" of data");
        if (dbFiles >_maxFiles) {
            sb = sb.append(" (WARNING: calculation stopped at MAX_FILES)");
        } else if (dbDirs > _maxDirs) {
            sb = sb.append(" (WARNING: calculation stopped at MAX_DIRS)");
        }
        return sb.toString();
    }
}
