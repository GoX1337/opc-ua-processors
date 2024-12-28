package org.gox.nifi.opcua.processors.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;

public class OpcUaNode {

    private String nodeId;
    private String name;
    private String dataType;
    private String value;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss")
    private LocalDateTime timestamp;

    public OpcUaNode(String nodeId, String name, String dataType, String value, LocalDateTime timestamp) {
        this.nodeId = nodeId;
        this.name = name;
        this.dataType = dataType;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OpcUaNode{" +
                "nodeId='" + nodeId + '\'' +
                ", name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
