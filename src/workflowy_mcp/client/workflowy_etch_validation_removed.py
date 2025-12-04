    @staticmethod
    def _validate_note_field(note: str | None, skip_newline_check: bool = False) -> tuple[str | None, str | None]:
        """Validate and auto-escape note field for Workflowy compatibility.
        
        Handles:
        1. Angle brackets (auto-escape to HTML entities - Workflowy renderer bug workaround)
        
        REMOVED: Literal backslash-n validation (moved to MCP connector level)
        
        Args:
            note: Note content to validate/escape
            skip_newline_check: DEPRECATED - check removed, parameter kept for compatibility
            
        Returns:
            (processed_note, warning_message)
            - processed_note: Escaped/fixed note
            - warning_message: Info message if changes made
        """
        if note is None:
            return (None, None)
        
        # Check for override token (for documentation that needs literal sequences)
        OVERRIDE_TOKEN = "<<<LITERAL_BACKSLASH_N_INTENTIONAL>>>"
        if note.startswith(OVERRIDE_TOKEN):
            # Strip token and return as-is
            return (note, None)  # Caller strips token before API call
        
        # CHECK 1: Auto-escape angle brackets (Workflowy renderer bug workaround)
        # Web interface auto-escapes < to &lt; and > to &gt;
        # API doesn't - we must do it manually
        escaped_note = note
        angle_bracket_escaped = False
        
        if '<' in note or '>' in note:
            escaped_note = note.replace('<', '&lt;').replace('>', '&gt;')
            angle_bracket_escaped = True
        
        # Return processed note with optional warning
        if angle_bracket_escaped:
            warning_msg = """✅ AUTO-ESCAPED: Angle brackets converted to HTML entities

🐛 WORKFLOWY RENDERER BUG: The API doesn't auto-escape < and > like the web interface does.
   Angle brackets cause notes to display as completely blank.

⚙️ AUTO-FIX APPLIED:
   Your < characters were converted to &lt;
   Your > characters were converted to &gt;
   
   This matches how Workflowy's web interface handles angle brackets.
   Your note will display correctly.

📖 Bug documentation: SATCHEL VYRTHEX in Deployment Documentation Validation ARC
"""
            return (escaped_note, warning_msg)
        
        return (escaped_note, None)
