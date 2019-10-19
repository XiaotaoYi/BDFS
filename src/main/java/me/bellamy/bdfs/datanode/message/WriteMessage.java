package me.bellamy.bdfs.datanode.message;

public class WriteMessage {
    private String blockId;
    private String data;

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
