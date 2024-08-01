package org.example;

public class OperationMessage {
    private int operand;
    private String operation;

    public OperationMessage(int operand, String operation) {
        this.operand = operand;
        this.operation = operation;
    }

    public int getOperand() {
        return operand;
    }

    public void setOperand(int operand) {
        this.operand = operand;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
}

