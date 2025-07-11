package com.lucid.automation.airouting.util.json;

/**
 * Utility class for bracket matching operations.
 */
public class BracketMatcher {

    /**
     * Finds the matching closing bracket for a given opening bracket.
     *
     * @param text The text to search in
     * @param start The starting position of the opening bracket
     * @param openBracket The opening bracket character
     * @param closeBracket The closing bracket character
     * @return The position of the matching closing bracket, or -1 if not found
     */
    public static int findMatchingBracket(String text, int start, char openBracket, char closeBracket) {
        int depth = 0;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == openBracket) depth++;
            else if (c == closeBracket) {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }
}
