def execute_sql_file(cursor, file_path, replacements=None):
    """Execute SQL file with parameter replacement, handling multi-statement blocks"""
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Replace parameters if provided
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Simple state machine to handle BEGIN...END blocks
    statements = []
    current_statement = ""
    in_block = False

    lines = sql_content.splitlines()
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith('--'):
            continue
        
        current_statement += " " + stripped_line

        if stripped_line.upper().startswith('BEGIN'):
            in_block = True
        
        if stripped_line.upper().endswith('END;'):
            in_block = False
            statements.append(current_statement.strip())
            current_statement = ""
        elif not in_block and stripped_line.endswith(';'):
            statements.append(current_statement.strip())
            current_statement = ""

    # Execute the collected statements
    for statement in statements:
        if statement:
            try:
                print(f"Executing: {statement[:100]}...")
                cursor.execute(statement)
                print("✓ Success")
            except Exception as e:
                print(f"✗ Error: {e}")
                raise