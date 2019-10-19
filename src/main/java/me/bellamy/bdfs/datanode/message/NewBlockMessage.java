package me.bellamy.bdfs.datanode.message;

public class NewBlockMessage {
    private Long blockId;

    public Long getBlockId() {
        return blockId;
    }

    public void setBlockId(Long blockId) {
        this.blockId = blockId;
    }
}
