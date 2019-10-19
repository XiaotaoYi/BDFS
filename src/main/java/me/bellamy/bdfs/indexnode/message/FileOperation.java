package me.bellamy.bdfs.indexnode.message;

import java.io.Serializable;

public class FileOperation implements Serializable {
    private String operation;
    private String fileName;
    private String action;

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
