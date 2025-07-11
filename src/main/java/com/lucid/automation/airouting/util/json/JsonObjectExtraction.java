package com.lucid.automation.airouting.util.json;

/**
 * Helper class to encapsulate JSON object extraction results.
 */
public class JsonObjectExtraction {
    private final int start;
    private final int end;
    private final int nextPosition;
    private final String jsonObject;
    private final boolean valid;

    public JsonObjectExtraction(String input, int start, int end, int nextPosition) {
        this.start = start;
        this.end = end;
        this.nextPosition = nextPosition;
        
        if (end != -1) {
            this.jsonObject = input.substring(start, end + 1).trim();
            this.valid = JsonValidator.isValidJsonStructure(this.jsonObject);
        } else {
            this.jsonObject = "";
            this.valid = false;
        }
    }

    public boolean isValid() { return valid && end != -1; }
    public String getJsonObject() { return jsonObject; }
    public int getStart() { return start; }
    public int getEnd() { return end; }
    public int getNextPosition() { return nextPosition; }
    
    public String getTruncatedObject() {
        return jsonObject.length() > 100 ? jsonObject.substring(0, 100) + "..." : jsonObject;
    }
}
