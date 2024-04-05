use std::path::PathBuf;

/// Enum for system commands related to server state.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum SystemCommand {
    /// Create a new database.
    Create(String),
    /// Connect to an existing database.
    Connect(String),
    /// Resets the server state
    Reset,
    /// Shuts down the server.
    Shutdown,
    /// Closes the current database connection.
    CloseConnection,
    /// Sets the server to quiet mode.
    QuietMode,
    /// Lists all databases in the system.
    ShowDatabases,
    /// Test or diagnostic command.
    Test,
}

impl std::fmt::Display for SystemCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemCommand::Create(s) => write!(f, "Create({})", s),
            SystemCommand::Connect(s) => write!(f, "Connect({})", s),
            SystemCommand::Reset => write!(f, "Reset"),
            SystemCommand::Shutdown => write!(f, "Shutdown"),
            SystemCommand::CloseConnection => write!(f, "CloseConnection"),
            SystemCommand::QuietMode => write!(f, "QuietMode"),
            SystemCommand::ShowDatabases => write!(f, "ShowDatabases"),
            SystemCommand::Test => write!(f, "Test"),
        }
    }
}

/// Enum for database commands related to data manipulation and querying applied to database state.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum DBCommand {
    /// Execute a specific SQL statement.
    ExecuteSQL(String),
    /// Register a query for future use.
    RegisterQuery(String),
    /// Run a registered query to a specific timestamp.
    RunQueryFull(String),
    /// Run a registered query for the diffs of a timestamp range.
    RunQueryPartial(String),
    /// Convert a SQL query to a JSON plan.
    ConvertQuery(String),
    /// Show all tables in the current database.
    ShowTables,
    /// Show all registered queries in the current database.
    ShowQueries,
    /// Generates a CSV file from a specified source.
    Generate(String),
    /// Import a CSV file into a specified table.
    Import(String, PathBuf),
}

impl std::fmt::Display for DBCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBCommand::ExecuteSQL(s) => write!(f, "ExecuteSQL({})", s),
            DBCommand::RegisterQuery(s) => write!(f, "RegisterQuery({})", s),
            DBCommand::RunQueryFull(s) => write!(f, "RunQueryFull({})", s),
            DBCommand::RunQueryPartial(s) => write!(f, "RunQueryPartial({})", s),
            DBCommand::ConvertQuery(s) => write!(f, "ConvertQuery({})", s),
            DBCommand::ShowTables => write!(f, "ShowTables"),
            DBCommand::ShowQueries => write!(f, "ShowQueries"),
            DBCommand::Generate(s) => write!(f, "Generate({})", s),
            DBCommand::Import(s, p) => write!(f, "Import({}, {:?})", s, p),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum Command {
    System(SystemCommand),
    DB(DBCommand),
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::System(s) => write!(f, "{}", s),
            Command::DB(s) => write!(f, "{}", s),
        }
    }
}

/// Types of acceptable commands.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Ok,
    SystemMsg(String),
    SystemErr(String),
    QueryResult(crate::QueryResult),
    QueryExecutionError(String),
    Shutdown,
    QuietOk,
    QuietErr,
}

impl Response {
    pub fn is_ok(&self) -> bool {
        match self {
            Response::Ok => true,
            Response::SystemMsg(_) => true,
            Response::SystemErr(_) => false,
            Response::QueryResult(_) => true,
            Response::QueryExecutionError(_) => false,
            Response::Shutdown => true,
            Response::QuietOk => true,
            Response::QuietErr => false,
        }
    }
}

pub fn parse_command(mut cmd: String) -> Option<Command> {
    if cmd.ends_with('\n') {
        cmd.pop();
        if cmd.ends_with('\r') {
            cmd.pop();
        }
    }

    // Handle regular SQL commands not prefixed with '\'
    if !cmd.starts_with('\\') {
        return Some(Command::DB(DBCommand::ExecuteSQL(cmd)));
    }

    // Handle meta and regular commands prefixed with '\'
    match cmd.as_str() {
        "\\dt" => Some(Command::DB(DBCommand::ShowTables)),
        "\\dq" => Some(Command::DB(DBCommand::ShowQueries)),
        "\\l" => Some(Command::System(SystemCommand::ShowDatabases)),
        "\\reset" => Some(Command::System(SystemCommand::Reset)),
        "\\shutdown" => Some(Command::System(SystemCommand::Shutdown)),
        "\\quiet" => Some(Command::System(SystemCommand::QuietMode)),
        "\\t" => Some(Command::System(SystemCommand::Test)),
        _ => {
            if let Some(clean_cmd) = cmd.strip_prefix("\\r ") {
                Some(Command::System(SystemCommand::Create(
                    clean_cmd.to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\c ") {
                Some(Command::System(SystemCommand::Connect(
                    clean_cmd.to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\i ") {
                let mut split = clean_cmd.split(' ');
                let path = split.next().unwrap().to_string();
                let name = split.next().unwrap().to_string();
                Some(Command::DB(DBCommand::Import(name, PathBuf::from(path))))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\register") {
                Some(Command::DB(DBCommand::RegisterQuery(
                    clean_cmd.trim().to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\runFull") {
                Some(Command::DB(DBCommand::RunQueryFull(
                    clean_cmd.trim().to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\runPartial") {
                Some(Command::DB(DBCommand::RunQueryPartial(
                    clean_cmd.trim().to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\convert") {
                Some(Command::DB(DBCommand::ConvertQuery(
                    clean_cmd.trim().to_string(),
                )))
            } else if let Some(clean_cmd) = cmd.strip_prefix("\\generate") {
                Some(Command::DB(DBCommand::Generate(
                    clean_cmd.trim().to_string(),
                )))
            } else {
                info!("Invalid command received {}", cmd);
                None
            }
        }
    }
}

/*
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create() {
        let create: String = String::from("\\r name");
        assert_eq!(
            Command::Create("name".to_string()),
            parse_command(create).unwrap()
        );
    }

    #[test]
    fn test_connect() {
        let connect: String = String::from("\\c name");
        assert_eq!(
            Command::Connect("name".to_string()),
            parse_command(connect).unwrap()
        );
    }

    #[test]
    fn test_import() {
        let import: String = String::from("\\i path name");
        assert_eq!(
            Command::Import("path name".to_string()),
            parse_command(import).unwrap()
        );
    }

    #[test]
    fn test_reset() {
        let reset: String = String::from("\\reset\n");
        assert_eq!(Command::Reset, parse_command(reset).unwrap());
    }

    #[test]
    fn test_show_tables() {
        let show_tables: String = String::from("\\dt\n");
        assert_eq!(Command::ShowTables, parse_command(show_tables).unwrap());
    }
}

*/
