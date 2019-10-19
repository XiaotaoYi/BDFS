package me.bellamy.bdfs.datanode.message;

public class ReadMessage {
    private String blockId;

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }
}
